use crate::logging::log_line;
use crate::metrics::DriftStats;
use crate::types::WikiChange;
use bytes::Bytes;
use futures_util::StreamExt;
use reqwest::Client;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{Mutex, mpsc};

const CHANNEL_CAPACITY: usize = 2;
const DEADLINE: Duration = Duration::from_millis(2);
const REPORT_EVERY: u64 = 100;

#[derive(Clone)]
struct QueuedChange {
    payload: Bytes,
    enqueued_at: Instant,
    doc_key: String,
    sequence: u64,
}

struct ProcessSample {
    scheduling_drift: Duration,
    processing_time: Duration,
}

pub async fn run_async_pipeline() {
    let url = "https://stream.wikimedia.org/v2/stream/recentchange";

    // Separate channels for human and bot edits.
    let (tx_human, rx_human) = mpsc::channel::<QueuedChange>(CHANNEL_CAPACITY);
    let (tx_bot, rx_bot) = mpsc::channel::<QueuedChange>(CHANNEL_CAPACITY);

    // Receivers are shared so the producer can drop the oldest item when a lane is full.
    let rx_human = Arc::new(Mutex::new(rx_human));
    let rx_bot = Arc::new(Mutex::new(rx_bot));
    let latest_versions = Arc::new(Mutex::new(HashMap::<String, u64>::new()));
    let next_sequence = Arc::new(AtomicU64::new(1));

    let tx_human_clone = tx_human.clone();
    let tx_bot_clone = tx_bot.clone();
    let rx_human_worker = Arc::clone(&rx_human);
    let rx_bot_worker = Arc::clone(&rx_bot);
    let latest_versions_for_drop = Arc::clone(&latest_versions);
    let next_sequence_for_drop = Arc::clone(&next_sequence);

    // Worker task: always prioritize human edits.
    tokio::spawn(async move {
        let mut stats = DriftStats::default();
        let latest_versions = Arc::clone(&latest_versions);

        loop {
            if let Some(message) = recv_prioritized(&rx_human_worker, &rx_bot_worker).await {
                // Deadline starts when the packet leaves the ingestion channel.
                let deadline_start = Instant::now();
                let sample = process_change(message, deadline_start, &latest_versions).await;
                stats.record(sample.scheduling_drift, sample.processing_time, DEADLINE);

                if stats.processed() % REPORT_EVERY == 0 {
                    stats.report();
                }
            } else {
                break;
            }
        }

        stats.report();
    });

    // Producer: ingest stream.
    let client = Client::new();
    let response = client
        .get(url)
        .header("User-Agent", "WendyRTSProject/1.0 (learning project)")
        .send()
        .await
        .unwrap();

    let mut stream = response.bytes_stream();

    while let Some(item) = stream.next().await {
        let chunk = item.unwrap();
        if chunk.starts_with(b"data: ") {
            let json_bytes = chunk.slice(6..);

            // Parse the change zero-copy.
            if let Ok(change) = serde_json::from_slice::<WikiChange<'_>>(&json_bytes) {
                let lane = if change.bot.unwrap_or(false) {
                    "BOT"
                } else {
                    "HUMAN"
                };
                let summary = change_summary(&change);
                log_line(format!("📥 INCOMING {} {}", lane, summary));
                let doc_key = document_key(&change);
                let sequence = next_sequence_for_drop.fetch_add(1, Ordering::Relaxed);
                {
                    let mut latest = latest_versions_for_drop.lock().await;
                    latest.insert(doc_key.clone(), sequence);
                }

                let packet = QueuedChange {
                    payload: json_bytes.clone(),
                    enqueued_at: Instant::now(),
                    doc_key,
                    sequence,
                };

                let sent = if change.bot.unwrap_or(false) {
                    send_with_drop_oldest(&tx_bot_clone, &rx_bot, packet, lane, &summary).await
                } else {
                    send_with_drop_oldest(&tx_human_clone, &rx_human, packet, lane, &summary).await
                };

                if !sent {
                    break;
                }
            }
        }
    }
}

async fn recv_prioritized(
    human_rx: &Arc<Mutex<mpsc::Receiver<QueuedChange>>>,
    bot_rx: &Arc<Mutex<mpsc::Receiver<QueuedChange>>>,
) -> Option<QueuedChange> {
    let mut human_closed = false;
    let mut bot_closed = false;

    loop {
        let human_state = {
            let mut guard = human_rx.lock().await;
            guard.try_recv()
        };
        match human_state {
            Ok(bytes) => return Some(bytes),
            Err(mpsc::error::TryRecvError::Disconnected) => human_closed = true,
            Err(mpsc::error::TryRecvError::Empty) => {}
        }

        let bot_state = {
            let mut guard = bot_rx.lock().await;
            guard.try_recv()
        };
        match bot_state {
            Ok(bytes) => return Some(bytes),
            Err(mpsc::error::TryRecvError::Disconnected) => bot_closed = true,
            Err(mpsc::error::TryRecvError::Empty) => {}
        }

        if human_closed && bot_closed {
            return None;
        }

        tokio::time::sleep(Duration::from_micros(200)).await;
    }
}

async fn send_with_drop_oldest(
    tx: &mpsc::Sender<QueuedChange>,
    rx: &Arc<Mutex<mpsc::Receiver<QueuedChange>>>,
    payload: QueuedChange,
    lane: &str,
    summary: &str,
) -> bool {
    let mut pending = payload;
    loop {
        match tx.try_send(pending) {
            Ok(()) => {
                log_line(format!("➕ ENQUEUE {} {}", lane, summary));
                return true;
            }
            Err(mpsc::error::TrySendError::Closed(_)) => return false,
            Err(mpsc::error::TrySendError::Full(returned_payload)) => {
                pending = returned_payload;

                let dropped = {
                    let mut guard = rx.lock().await;
                    guard.try_recv().ok()
                };

                if let Some(oldest) = dropped {
                    let oldest_summary = summarize_bytes(&oldest.payload);
                    let ts = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or(Duration::ZERO);
                    println!(
                        "[{}.{:09}] OVERFLOW DROP {} dropped=[{}] incoming=[{}]",
                        ts.as_secs(),
                        ts.subsec_nanos(),
                        lane,
                        oldest_summary,
                        summary,
                    );
                } else {
                    log_line(format!("🔁 RETRY {} incoming=[{}]", lane, summary));
                    tokio::time::sleep(Duration::from_micros(100)).await;
                }
            }
        }
    }
}

// Process a single WikiChange.
async fn process_change(
    packet: QueuedChange,
    deadline_start: Instant,
    latest_versions: &Arc<Mutex<HashMap<String, u64>>>,
) -> ProcessSample {
    let scheduling_drift = deadline_start.duration_since(packet.enqueued_at);
    let mut overridden = false;

    if let Ok(change) = serde_json::from_slice::<WikiChange<'_>>(&packet.payload) {
        // Keep packet logs compact to reduce log-induced contention.
        let label = if change.bot.unwrap_or(false) {
            "BOT"
        } else {
            "HUMAN"
        };
        log_line(format!("📤 DEQUEUE {} {}", label, change_summary(&change)));
        log_line(format!(
            "⏱ {} Scheduling Drift: {:?}",
            label, scheduling_drift
        ));

        if change.bot.unwrap_or(false) {
            let is_stale = {
                let latest = latest_versions.lock().await;
                latest
                    .get(&packet.doc_key)
                    .is_some_and(|latest_seq| *latest_seq > packet.sequence)
            };

            if is_stale {
                overridden = true;
                log_line(format!(
                    "🚫 BOT OVERRIDDEN: doc_key={} seq={} newer_human_seq_exists",
                    packet.doc_key, packet.sequence
                ));
            }
        }
    }

    let processing_time = deadline_start.elapsed();
    if overridden {
        log_line("⏭ Final processing skipped because a newer human edit won.");
        if processing_time > DEADLINE {
            log_line(format!(
                "⛔ DEADLINE MISSED: {:?} (limit: {:?}) [overridden]",
                processing_time, DEADLINE
            ));
        } else {
            log_line(format!(
                "✅ Deadline met: {:?} [overridden]",
                processing_time
            ));
        }
        return ProcessSample {
            scheduling_drift,
            processing_time,
        };
    }

    if processing_time > DEADLINE {
        log_line(format!(
            "⛔ DEADLINE MISSED: {:?} (limit: {:?})",
            processing_time, DEADLINE
        ));
    } else {
        log_line(format!("✅ Deadline met: {:?}", processing_time));
    }

    ProcessSample {
        scheduling_drift,
        processing_time,
    }
}

fn document_key(change: &WikiChange<'_>) -> String {
    let namespace = change
        .namespace
        .map(|value| value.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let title = change.title.unwrap_or("unknown");

    format!("{}:{}", namespace, title)
}

fn change_summary(change: &WikiChange<'_>) -> String {
    format!(
        "event user={:?} bot={:?} server={:?}",
        change.user, change.bot, change.server_name
    )
}

fn summarize_bytes(bytes: &Bytes) -> String {
    if let Ok(change) = serde_json::from_slice::<WikiChange<'_>>(bytes) {
        change_summary(&change)
    } else if let Ok(text) = std::str::from_utf8(bytes) {
        text.chars().take(200).collect()
    } else {
        format!("raw {} bytes", bytes.len())
    }
}
