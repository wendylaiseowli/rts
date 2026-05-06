use crate::logging::log_line;
use crate::metrics::{DomainLeaderboard, DriftStats};
use crate::types::WikiChange;
use bytes::Bytes;
use futures_util::StreamExt;
use reqwest::Client;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{Mutex, mpsc};

const CHANNEL_CAPACITY: usize = 2;
const DEADLINE: Duration = Duration::from_millis(2);
const REPORT_EVERY: u64 = 100;

#[derive(Clone)]
struct QueuedChange {
    payload: Bytes,
}

struct ProcessSample {
    scheduling_drift_ns: i128,
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
    let leaderboard = Arc::new(Mutex::new(DomainLeaderboard::default()));

    let tx_human_clone = tx_human.clone();
    let tx_bot_clone = tx_bot.clone();
    let rx_human_worker = Arc::clone(&rx_human);
    let rx_bot_worker = Arc::clone(&rx_bot);
    let leaderboard_for_drop = Arc::clone(&leaderboard);

    // Worker task: always prioritize human edits.
    tokio::spawn(async move {
        let mut stats = DriftStats::default();
        let leaderboard = Arc::clone(&leaderboard);

        loop {
            let Some(message) = recv_prioritized(&rx_human_worker, &rx_bot_worker).await else {
                break;
            };

            let mut message = message;
            if is_bot_packet(&message) {
                if let Some(human_packet) = try_recv_human_now(&rx_human_worker).await {
                    log_line("🚫 BOT OVERRIDDEN by a newly arrived HUMAN packet.");
                    message = human_packet;
                }
            }

            // Deadline starts when the packet leaves the ingestion channel.
            let deadline_start = Instant::now();
            let sample = process_change(message, deadline_start).await;
            stats.record(sample.scheduling_drift_ns, sample.processing_time, DEADLINE);

            if stats.processed() % REPORT_EVERY == 0 {
                stats.report();
                let lb = leaderboard.lock().await;
                let _ = lb.write_html(None);
            }
        }

        stats.report();
        let lb = leaderboard.lock().await;
        let _ = lb.write_html(None);
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
                let domain = domain_key(&change);
                {
                    let mut lb = leaderboard_for_drop.lock().await;
                    lb.record(&domain);
                }

                let packet = QueuedChange {
                    payload: json_bytes.clone(),
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
) -> ProcessSample {
    let actual_processing_time = deadline_start.elapsed();
    let scheduling_drift_ns =
        actual_processing_time.as_nanos() as i128 - DEADLINE.as_nanos() as i128;

    if let Ok(change) = serde_json::from_slice::<WikiChange<'_>>(&packet.payload) {
        // Keep packet logs compact to reduce log-induced contention.
        let label = if change.bot.unwrap_or(false) {
            "BOT"
        } else {
            "HUMAN"
        };
        log_line(format!("?? DEQUEUE {} {}", label, change_summary(&change)));
        log_line(format!(
            "? {} Actual={:?} Expected={:?} Scheduling Drift(ns)={}",
            label, actual_processing_time, DEADLINE, scheduling_drift_ns
        ));
    }

    if actual_processing_time > DEADLINE {
        log_line(format!(
            "? DEADLINE MISSED: {:?} (limit: {:?})",
            actual_processing_time, DEADLINE
        ));
    } else {
        log_line(format!("? Deadline met: {:?}", actual_processing_time));
    }

    ProcessSample {
        scheduling_drift_ns,
        processing_time: actual_processing_time,
    }
}

async fn try_recv_human_now(
    human_rx: &Arc<Mutex<mpsc::Receiver<QueuedChange>>>,
) -> Option<QueuedChange> {
    let mut guard = human_rx.lock().await;
    guard.try_recv().ok()
}

fn domain_key(change: &WikiChange<'_>) -> String {
    change.server_name.unwrap_or("unknown").to_string()
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

fn is_bot_packet(packet: &QueuedChange) -> bool {
    serde_json::from_slice::<WikiChange<'_>>(&packet.payload)
        .ok()
        .and_then(|change| change.bot)
        .unwrap_or(false)
}



