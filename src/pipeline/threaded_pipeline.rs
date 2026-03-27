use crate::types::WikiChange;
use bytes::Bytes;
use reqwest::blocking::Client;
use std::io::Read;
use std::sync::mpsc::{self, Receiver, SyncSender, TryRecvError, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const CHANNEL_CAPACITY: usize = 2;
const DEADLINE: Duration = Duration::from_millis(2);

#[derive(Clone)]
struct QueuedChange {
    payload: Bytes,
    enqueued_at: SystemTime,
}

pub fn run_threaded_pipeline() {
    let url = "https://stream.wikimedia.org/v2/stream/recentchange";

    // Bounded channels for human and bot edits
    let (tx_human, rx_human) = mpsc::sync_channel::<QueuedChange>(CHANNEL_CAPACITY);
    let (tx_bot, rx_bot) = mpsc::sync_channel::<QueuedChange>(CHANNEL_CAPACITY);

    let rx_human = Arc::new(Mutex::new(rx_human));
    let rx_bot = Arc::new(Mutex::new(rx_bot));

    let tx_human_clone = tx_human.clone();
    let tx_bot_clone = tx_bot.clone();
    let rx_human_for_drop = Arc::clone(&rx_human);
    let rx_bot_for_drop = Arc::clone(&rx_bot);

    // -------------------------
    // Worker thread (Priority: Human first)
    // -------------------------
    let worker_handle = {
        let rx_human = rx_human.clone();
        let rx_bot = rx_bot.clone();
        thread::spawn(move || {
            loop {
                match recv_prioritized(&rx_human, &rx_bot) {
                    Some(message) => {
                        let dequeue_time = SystemTime::now();
                        process_change_thread(message, dequeue_time);
                    }
                    None => break,
                }
            }
        })
    };

    // -------------------------
    // Producer thread
    // -------------------------
    let producer_handle = thread::spawn(move || {
        let client = Client::new();
        let response = match client
            .get(url)
            .header("User-Agent", "WendyRTSProject/1.0")
            .send()
        {
            Ok(r) => r,
            Err(e) => {
                eprintln!("Failed to connect: {:?}", e);
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
                                println!("INCOMING {} {}", lane, summary);

                                let packet = QueuedChange {
                                    payload: json_bytes.clone(),
                                    enqueued_at: SystemTime::now(),
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
                    eprintln!("Failed to read from stream: {:?}", e);
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
                println!("ENQUEUE {} {}", lane, summary);
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
                    println!("RETRY {} incoming=[{}]", lane, summary);
                    thread::sleep(Duration::from_micros(100));
                }
            }
        }
    }
}

fn process_change_thread(message: QueuedChange, dequeue_time: SystemTime) {
    if let Ok(change) = serde_json::from_slice::<WikiChange<'_>>(&message.payload) {
        let label = if change.bot.unwrap_or(false) {
            "BOT"
        } else {
            "HUMAN"
        };
        println!("DEQUEUE {} {}", label, change_summary(&change));

        let queue_drift = match dequeue_time.duration_since(message.enqueued_at) {
            Ok(delta) => delta,
            Err(_) => Duration::ZERO,
        };
        println!("{} Scheduling Drift: {:?}", label, queue_drift);
    }

    if let Ok(elapsed) = dequeue_time.elapsed() {
        if elapsed > DEADLINE {
            println!("🚨 DEADLINE MISSED: {:?} (limit: {:?})", elapsed, DEADLINE);
        } else {
            println!("✅ Deadline met: {:?}", elapsed);
        }
    }
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
    println!(
        "[{}.{:09}] ⚠️ Overflow Event: DROP {} dropped=[{}] incoming=[{}]",
        ts.as_secs(),
        ts.subsec_nanos(),
        lane,
        dropped_summary,
        incoming_summary
    );
}
