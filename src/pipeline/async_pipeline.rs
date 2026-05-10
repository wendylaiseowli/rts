use crate::logging::{log_error, log_line};
use crate::parser::{hot_path_view, is_bot, parse_wiki_change, server_name};
use crate::metrics::{DomainLeaderboard, DriftStats};
use crate::types::WikiChange;
use bytes::Bytes;
use futures_util::StreamExt;
use reqwest::Client;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, mpsc};

const CHANNEL_CAPACITY: usize = 2;
const DEADLINE: Duration = Duration::from_millis(2);
const JITTER_THRESHOLD: Duration = Duration::from_millis(1);
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
    let degraded_mode = Arc::new(AtomicBool::new(false));

    let tx_human_clone = tx_human.clone();
    let tx_bot_clone = tx_bot.clone();
    let rx_human_worker = Arc::clone(&rx_human);
    let rx_bot_worker = Arc::clone(&rx_bot);
    let leaderboard_for_drop = Arc::clone(&leaderboard);
    let degraded_mode_for_drop = Arc::clone(&degraded_mode);

    // Worker task: always prioritize human edits.
    tokio::spawn(async move {
        let mut stats = DriftStats::default();
        let leaderboard = Arc::clone(&leaderboard);
        let degraded_mode = Arc::clone(&degraded_mode);
        let mut previous_processing_time: Option<Duration> = None;

        loop {
            let Some(message) = recv_prioritized(&rx_human_worker, &rx_bot_worker).await else {
                break;
            };

            let mut message = message;
            if degraded_mode.load(Ordering::Relaxed) && is_bot_packet(&message) {
                log_line("DEGRADED MODE: dropping BOT packet to keep the system stable.");
                continue;
            }

            log_dequeue_event(&message);

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
            if let Some(previous) = previous_processing_time {
                let jitter = sample.processing_time.abs_diff(previous);
                if stats.observe_jitter(jitter, JITTER_THRESHOLD)
                    && !degraded_mode.swap(true, Ordering::SeqCst)
                {
                    log_line(format!(
                        "DEGRADED MODE ENTERED: processing jitter {:?} exceeded threshold {:?}.",
                        jitter, JITTER_THRESHOLD
                    ));
                }
            }
            previous_processing_time = Some(sample.processing_time);
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
    loop {
        let client = Client::new();
        let response = match client
            .get(url)
            .header("User-Agent", "WendyRTSProject/1.0 (learning project)")
            .send()
            .await
        {
            Ok(response) => response,
            Err(e) => {
                log_error(format!("Failed to connect: {:?}", e));
                log_line(format!("Network Reset: failed to connect: {:?}", e));
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };

        let mut stream = response.bytes_stream();

        loop {
            match tokio::time::timeout(Duration::from_secs(10), stream.next()).await {
                Ok(Some(Ok(chunk))) => {
                    if chunk.starts_with(b"data: ") {
                        let json_bytes = chunk.slice(6..);

                        // Parse the change zero-copy.
                        if let Ok(change) = parse_wiki_change(&json_bytes) {
                            let view = hot_path_view(&change);
                            let lane = match view.lane {
                                crate::parser::PacketLane::Bot => "BOT",
                                crate::parser::PacketLane::Human => "HUMAN",
                            };
                            let summary = change_summary(&change);
                            log_line(format!("INCOMING {} {}", lane, summary));
                            if degraded_mode_for_drop.load(Ordering::Relaxed) && is_bot(&change) {
                                log_line(
                                    "DEGRADED MODE: dropping incoming BOT packet before enqueue.",
                                );
                                continue;
                            }
                            let domain = server_name(&change).to_string();
                            {
                                let mut lb = leaderboard_for_drop.lock().await;
                                lb.record(&domain);
                            }

                            let packet = QueuedChange {
                                payload: json_bytes.clone(),
                            };

                            let sent = if is_bot(&change) {
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
                Ok(Some(Err(e))) => {
                    log_error(format!("Failed to read from stream: {:?}", e));
                    log_line(format!("Network Reset: stream error: {:?}", e));
                    break;
                }
                Ok(None) => {
                    log_line("Network Reset: stream ended, reconnecting.");
                    break;
                }
                Err(_) => {
                    log_line("Network Reset: no data for 10 seconds, reconnecting.");
                    break;
                }
            }
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
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
                    log_line(format!(
                        "OVERFLOW DROP {} dropped=[{}] incoming=[{}]",
                        lane,
                        oldest_summary,
                        summary,
                    ));
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
    if let Ok(change) = parse_wiki_change(&packet.payload) {
        // Keep packet logs compact to reduce log-induced contention.
        let label = if is_bot(&change) {
            "BOT"
        } else {
            "HUMAN"
        };
        let context = change_context(&change);
        let actual_processing_time = deadline_start.elapsed();
        let scheduling_drift_ns =
            actual_processing_time.as_nanos() as i128 - DEADLINE.as_nanos() as i128;
        log_line(format!(
            "? {} {} Actual={:?} Expected={:?} Scheduling Drift(ns)={}",
            label, context, actual_processing_time, DEADLINE, scheduling_drift_ns
        ));
        if actual_processing_time > DEADLINE {
            log_line(format!(
                "⛔ DEADLINE MISSED {} Actual={:?} Expected={:?} Scheduling Drift(ns)={}",
                context, actual_processing_time, DEADLINE, scheduling_drift_ns
            ));
        } else {
            log_line(format!(
                "✅ Deadline met {} Actual={:?} Expected={:?} Scheduling Drift(ns)={}",
                context, actual_processing_time, DEADLINE, scheduling_drift_ns
            ));
        }
        return ProcessSample {
            scheduling_drift_ns,
            processing_time: actual_processing_time,
        };
    }

    let actual_processing_time = deadline_start.elapsed();
    let scheduling_drift_ns =
        actual_processing_time.as_nanos() as i128 - DEADLINE.as_nanos() as i128;

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

fn change_summary(change: &WikiChange<'_>) -> String {
    let view = hot_path_view(change);
    format!(
        "event id={} user={:?} bot={:?} server={:?} title={:?}",
        view.event_id.unwrap_or("unknown"), view.user, change.bot, view.server_name, view.title
    )
}

fn change_context(change: &WikiChange<'_>) -> String {
    format!(
        "user={:?} bot={:?} server={:?}",
        change.user,
        change.bot,
        change.server_name
    )
}

fn summarize_bytes(bytes: &Bytes) -> String {
    if let Ok(change) = parse_wiki_change(bytes) {
        change_summary(&change)
    } else if let Ok(text) = std::str::from_utf8(bytes) {
        text.chars().take(200).collect()
    } else {
        format!("raw {} bytes", bytes.len())
    }
}

fn is_bot_packet(packet: &QueuedChange) -> bool {
    parse_wiki_change(&packet.payload)
        .ok()
        .map(|change| is_bot(&change))
        .unwrap_or(false)
}

fn log_dequeue_event(packet: &QueuedChange) {
    if let Ok(change) = parse_wiki_change(&packet.payload) {
        let label = if is_bot(&change) { "BOT" } else { "HUMAN" };
                log_line(format!("DEQUEUE {} {}", label, change_summary(&change)));
    }
}






