use crate::logging::{log_error, log_line};
use crate::metrics::DriftStats;
use crate::types::WikiChange;
use bytes::Bytes;
use reqwest::blocking::Client;
use std::collections::HashMap;
use std::io::Read;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TryRecvError, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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

pub fn run_threaded_pipeline() {
    let url = "https://stream.wikimedia.org/v2/stream/recentchange";

    // Bounded channels for human and bot edits.
    let (tx_human, rx_human) = mpsc::sync_channel::<QueuedChange>(CHANNEL_CAPACITY);
    let (tx_bot, rx_bot) = mpsc::sync_channel::<QueuedChange>(CHANNEL_CAPACITY);

    let rx_human = Arc::new(Mutex::new(rx_human));
    let rx_bot = Arc::new(Mutex::new(rx_bot));
    let latest_versions = Arc::new(Mutex::new(HashMap::<String, u64>::new()));
    let next_sequence = Arc::new(AtomicU64::new(1));

    let tx_human_clone = tx_human.clone();
    let tx_bot_clone = tx_bot.clone();
    let rx_human_for_drop = Arc::clone(&rx_human);
    let rx_bot_for_drop = Arc::clone(&rx_bot);
    let latest_versions_for_drop = Arc::clone(&latest_versions);
    let next_sequence_for_drop = Arc::clone(&next_sequence);

    // Worker thread: always prioritize human edits.
    let worker_handle = {
        let rx_human = rx_human.clone();
        let rx_bot = rx_bot.clone();
        let latest_versions = Arc::clone(&latest_versions);
        thread::spawn(move || {
            let mut stats = DriftStats::default();

            loop {
                match recv_prioritized(&rx_human, &rx_bot) {
                    Some(message) => {
                        // Deadline starts when the packet leaves the ingestion channel.
                        let deadline_start = Instant::now();
                        let sample =
                            process_change_thread(message, deadline_start, &latest_versions);
                        stats.record(sample.scheduling_drift, sample.processing_time, DEADLINE);

                        if stats.processed() % REPORT_EVERY == 0 {
                            stats.report();
                        }
                    }
                    None => break,
                }
            }

            stats.report();
        })
    };

    // Producer thread.
    let producer_handle = thread::spawn(move || {
        let client = Client::new();
        let response = match client
            .get(url)
            .header("User-Agent", "WendyRTSProject/1.0")
            .send()
        {
            Ok(r) => r,
            Err(e) => {
                log_error(format!("Failed to connect: {:?}", e));
                return;
            }
        };

        let mut buffer = Vec::new();
        let mut reader = response;

        loop {
            buffer.clear();
            match reader.by_ref().take(4096).read_to_end(&mut buffer) {
                Ok(0) => break,
                Ok(_) => {
                    for line in buffer.split(|&b| b == b'\n') {
                        if line.starts_with(b"data: ") {
                            let json_bytes = Bytes::copy_from_slice(&line[6..]);

                            if let Ok(change) =
                                serde_json::from_slice::<WikiChange<'_>>(&json_bytes)
                            {
                                let lane = if change.bot.unwrap_or(false) {
                                    "BOT"
                                } else {
                                    "HUMAN"
                                };
                                let summary = change_summary(&change);
                                log_line(format!("📥 INCOMING {} {}", lane, summary));
                                let doc_key = document_key(&change);
                                let sequence =
                                    next_sequence_for_drop.fetch_add(1, Ordering::Relaxed);

                                {
                                    let mut latest = latest_versions_for_drop.lock().unwrap();
                                    latest.insert(doc_key.clone(), sequence);
                                }

                                let packet = QueuedChange {
                                    payload: json_bytes.clone(),
                                    enqueued_at: Instant::now(),
                                    doc_key,
                                    sequence,
                                };

                                let sent = if change.bot.unwrap_or(false) {
                                    send_with_drop_oldest(
                                        &tx_bot_clone,
                                        &rx_bot_for_drop,
                                        packet,
                                        lane,
                                        &summary,
                                    )
                                } else {
                                    send_with_drop_oldest(
                                        &tx_human_clone,
                                        &rx_human_for_drop,
                                        packet,
                                        lane,
                                        &summary,
                                    )
                                };

                                if !sent {
                                    eprintln!("{} channel closed; stopping producer thread", lane);
                                    return;
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    log_error(format!("Failed to read from stream: {:?}", e));
                    break;
                }
            }
        }
    });

    let _ = producer_handle.join();
    let _ = worker_handle.join();
}

fn recv_prioritized(
    human_rx: &Arc<Mutex<Receiver<QueuedChange>>>,
    bot_rx: &Arc<Mutex<Receiver<QueuedChange>>>,
) -> Option<QueuedChange> {
    let mut human_closed = false;
    let mut bot_closed = false;

    loop {
        let human_state = {
            if let Ok(guard) = human_rx.lock() {
                guard.try_recv()
            } else {
                Err(TryRecvError::Empty)
            }
        };

        match human_state {
            Ok(bytes) => return Some(bytes),
            Err(TryRecvError::Disconnected) => human_closed = true,
            Err(TryRecvError::Empty) => {}
        }

        let bot_state = {
            if let Ok(guard) = bot_rx.lock() {
                guard.try_recv()
            } else {
                Err(TryRecvError::Empty)
            }
        };

        match bot_state {
            Ok(bytes) => return Some(bytes),
            Err(TryRecvError::Disconnected) => bot_closed = true,
            Err(TryRecvError::Empty) => {}
        }

        if human_closed && bot_closed {
            return None;
        }

        thread::sleep(Duration::from_micros(200));
    }
}

fn send_with_drop_oldest(
    tx: &SyncSender<QueuedChange>,
    rx: &Arc<Mutex<Receiver<QueuedChange>>>,
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
            Err(TrySendError::Disconnected(_)) => return false,
            Err(TrySendError::Full(returned_payload)) => {
                pending = returned_payload;

                let dropped = {
                    if let Ok(guard) = rx.lock() {
                        guard.try_recv().ok()
                    } else {
                        None
                    }
                };

                if let Some(oldest) = dropped {
                    let oldest_summary = summarize_bytes(&oldest.payload);
                    log_overflow_event(lane, &oldest_summary, summary);
                } else {
                    log_line(format!("🔁 RETRY {} incoming=[{}]", lane, summary));
                    thread::sleep(Duration::from_micros(100));
                }
            }
        }
    }
}

fn process_change_thread(
    message: QueuedChange,
    deadline_start: Instant,
    latest_versions: &Arc<Mutex<HashMap<String, u64>>>,
) -> ProcessSample {
    let scheduling_drift = deadline_start.duration_since(message.enqueued_at);
    let mut overridden = false;

    if let Ok(change) = serde_json::from_slice::<WikiChange<'_>>(&message.payload) {
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
                let latest = latest_versions.lock().unwrap();
                latest
                    .get(&message.doc_key)
                    .is_some_and(|latest_seq| *latest_seq > message.sequence)
            };

            if is_stale {
                overridden = true;
                log_line(format!(
                    "🚫 BOT OVERRIDDEN: doc_key={} seq={} newer_human_seq_exists",
                    message.doc_key, message.sequence
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
    let event_id = change
        .meta
        .as_ref()
        .and_then(|meta| meta.id)
        .unwrap_or("unknown");

    format!(
        "event id={} user={:?} bot={:?} server={:?}",
        event_id, change.user, change.bot, change.server_name
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

fn log_overflow_event(lane: &str, dropped_summary: &str, incoming_summary: &str) {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO);
    log_line(format!(
        "[{}.{:09}] OVERFLOW DROP {} dropped=[{}] incoming=[{}]",
        ts.as_secs(),
        ts.subsec_nanos(),
        lane,
        dropped_summary,
        incoming_summary
    ));
}
