use crate::logging::{log_error, log_line};
use crate::parser::{hot_path_view, is_bot, parse_wiki_change, packet_lane, server_name};
use crate::metrics::{DomainLeaderboard, DriftStats, DriftTimeline, EditLane, LaneDriftStats};
use crate::types::WikiChange;
use bytes::Bytes;
use reqwest::blocking::Client;
use std::io::Read;
use std::sync::mpsc::{self, Receiver, SyncSender, TryRecvError, TrySendError};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const CHANNEL_CAPACITY: usize = 2;
const DEADLINE: Duration = Duration::from_millis(2);
const JITTER_THRESHOLD: Duration = Duration::from_millis(1);
const REPORT_EVERY: u64 = 100;

#[derive(Clone)]
struct QueuedChange {
    payload: Bytes,
}

struct ProcessSample {
    lane: EditLane,
    scheduling_drift_ns: i128,
    processing_time: Duration,
}

pub fn run_threaded_pipeline() {
    let url = "https://stream.wikimedia.org/v2/stream/recentchange";

    // Bounded channels for human and bot edits.
    let (tx_human, rx_human) = mpsc::sync_channel::<QueuedChange>(CHANNEL_CAPACITY);
    let (tx_bot, rx_bot) = mpsc::sync_channel::<QueuedChange>(CHANNEL_CAPACITY);

    let rx_human = Arc::new(Mutex::new(rx_human));
    let rx_bot = Arc::new(Mutex::new(rx_bot));
    let leaderboard = Arc::new(Mutex::new(DomainLeaderboard::default()));
    let drift_history = Arc::new(Mutex::new(DriftTimeline::default()));
    let lane_drift = Arc::new(Mutex::new(LaneDriftStats::default()));
    let degraded_mode = Arc::new(AtomicBool::new(false));

    let tx_human_clone = tx_human.clone();
    let tx_bot_clone = tx_bot.clone();
    let rx_human_for_drop = Arc::clone(&rx_human);
    let rx_bot_for_drop = Arc::clone(&rx_bot);
    let leaderboard_for_drop = Arc::clone(&leaderboard);
    let drift_history_for_worker = Arc::clone(&drift_history);
    let lane_drift_for_worker = Arc::clone(&lane_drift);
    let degraded_mode_for_drop = Arc::clone(&degraded_mode);

    // Worker thread: always prioritize human edits.
    let worker_handle = {
        let rx_human = rx_human.clone();
        let rx_bot = rx_bot.clone();
        let leaderboard = Arc::clone(&leaderboard);
        let drift_history = Arc::clone(&drift_history_for_worker);
        let lane_drift = Arc::clone(&lane_drift_for_worker);
        let degraded_mode = Arc::clone(&degraded_mode);
        thread::spawn(move || {
            let mut stats = DriftStats::default();
            let mut previous_processing_time: Option<Duration> = None;

            loop {
                let Some(message) = recv_prioritized(&rx_human, &rx_bot) else {
                    break;
                };

                let mut message = message;
                if degraded_mode.load(Ordering::Relaxed) && is_bot_packet(&message) {
                    log_line("DEGRADED MODE: dropping BOT packet to keep the system stable.");
                    continue;
                }

                log_dequeue_event(&message);

                if is_bot_packet(&message) {
                    if let Some(human_packet) = try_recv_human_now(&rx_human) {
                        log_line("🚫 BOT OVERRIDDEN by a newly arrived HUMAN packet.");
                        message = human_packet;
                    }
                }

                // Deadline starts when the packet leaves the ingestion channel.
                let deadline_start = Instant::now();
                let sample = process_change_thread(message, deadline_start);
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
                if let Ok(mut history) = drift_history.lock() {
                    history.record(sample.lane, sample.scheduling_drift_ns);
                }
                if let Ok(mut report) = lane_drift.lock() {
                    report.record(
                        sample.lane,
                        sample.scheduling_drift_ns,
                        sample.processing_time,
                        DEADLINE,
                    );
                }

                if stats.processed() % REPORT_EVERY == 0 {
                    stats.report();
                    if let Ok(lb) = leaderboard.lock() {
                        let _ = lb.write_html(None);
                    }
                    if let Ok(history) = drift_history.lock() {
                        let _ = history.write_svg(None);
                    }
                    if let Ok(report) = lane_drift.lock() {
                        let _ = report.write_html(None);
                    }
                }
            }

            stats.report();
            if let Ok(lb) = leaderboard.lock() {
                let _ = lb.write_html(None);
            }
            if let Ok(history) = drift_history.lock() {
                let _ = history.write_svg(None);
            }
            if let Ok(report) = lane_drift.lock() {
                let _ = report.write_html(None);
            }
        })
    };
    // Producer thread.
    let producer_handle = thread::spawn(move || {
        loop {
            let watchdog_timeout = Duration::from_secs(10);
            let mut last_data_received = Instant::now();

            let client = match Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
            {
                Ok(client) => client,
                Err(e) => {
                    log_error(format!("Failed to build client: {:?}", e));
                    return;
                }
            };

            log_line("?? Connecting to Wikipedia SSE stream...");
            let response = match client
                .get(url)
                .header("User-Agent", "WendyRTSProject/1.0")
                .send()
            {
                Ok(r) => r,
                Err(e) => {
                    log_error(format!("Failed to connect: {:?}", e));
                    log_line("Network Reset: reconnecting after connection failure.");
                    thread::sleep(Duration::from_secs(1));
                    continue;
                }
            };

            let mut buffer = Vec::new();
            let mut reader = response;

            loop {
                // Active watchdog: check if 10 seconds have passed since last data
                if last_data_received.elapsed() >= watchdog_timeout {
                    log_line("?? Network Reset: no data for 10 seconds, triggering reconnect.");
                    break;
                }

                buffer.clear();
                match reader.by_ref().take(4096).read_to_end(&mut buffer) {
                    Ok(0) => {
                        log_line("?? Network Reset: stream ended, reconnecting.");
                        break;
                    }
                    Ok(_) => {
                        last_data_received = Instant::now();
                        for line in buffer.split(|&b| b == b'\n') {
                            if line.starts_with(b"data: ") {
                                let json_bytes = Bytes::copy_from_slice(&line[6..]);

                                if let Ok(change) = parse_wiki_change(&json_bytes) {
                                    let view = hot_path_view(&change);
                                    let lane = match view.lane {
                                        crate::parser::PacketLane::Bot => "BOT",
                                        crate::parser::PacketLane::Human => "HUMAN",
                                    };
                                    let summary = change_summary(&change);
                                    log_line(format!("?? INCOMING {} {}", lane, summary));
                                    if degraded_mode_for_drop.load(Ordering::Relaxed)
                                        && is_bot(&change)
                                    {
                                        log_line(
                                            "DEGRADED MODE: dropping incoming BOT packet before enqueue.",
                                        );
                                        continue;
                                    }
                                    let domain = server_name(&change).to_string();
                                    if let Ok(mut lb) = leaderboard_for_drop.lock() {
                                        lb.record(&domain);
                                    }

                                    let packet = QueuedChange {
                                        payload: json_bytes.clone(),
                                    };

                                    let sent = if is_bot(&change) {
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
                        if last_data_received.elapsed() >= watchdog_timeout {
                            log_line("?? Network Reset: no data for 10 seconds, reconnecting.");
                        } else {
                            log_line("?? Network Reset: reconnecting after read error.");
                        }
                        break;
                    }
                }
            }

            thread::sleep(Duration::from_secs(1));
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
) -> ProcessSample {
    if let Ok(change) = parse_wiki_change(&message.payload) {
        let label = if is_bot(&change) {
            "BOT"
        } else {
            "HUMAN"
        };
        let lane = match packet_lane(&change) {
            crate::parser::PacketLane::Bot => EditLane::Bot,
            crate::parser::PacketLane::Human => EditLane::Human,
        };
        let context = change_context(&change);
        let actual_processing_time = deadline_start.elapsed();
        let scheduling_drift_ns =
            actual_processing_time.as_nanos() as i128 - DEADLINE.as_nanos() as i128;
        log_line(format!(
            "⏱ {} {} Actual={:?} Expected={:?} Scheduling Drift(ns)={}",
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
            lane,
            scheduling_drift_ns,
            processing_time: actual_processing_time,
        };
    }

    let actual_processing_time = deadline_start.elapsed();
    let scheduling_drift_ns =
        actual_processing_time.as_nanos() as i128 - DEADLINE.as_nanos() as i128;

    ProcessSample {
        lane: EditLane::Human,
        scheduling_drift_ns,
        processing_time: actual_processing_time,
    }
}

fn try_recv_human_now(
    human_rx: &Arc<Mutex<Receiver<QueuedChange>>>,
) -> Option<QueuedChange> {
    if let Ok(guard) = human_rx.lock() {
        guard.try_recv().ok()
    } else {
        None
    }
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
        log_line(format!("📤 DEQUEUE {} {}", label, change_summary(&change)));
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



