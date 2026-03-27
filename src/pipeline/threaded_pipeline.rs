use reqwest::blocking::Client;
use std::io::Read;
use std::sync::mpsc::{self, Receiver, SyncSender, TryRecvError, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use bytes::Bytes;
use crate::types::WikiChange;

const CHANNEL_CAPACITY: usize = 2;
const DEADLINE: Duration = Duration::from_millis(2);

pub fn run_threaded_pipeline() {
    let url = "https://stream.wikimedia.org/v2/stream/recentchange";

    // Bounded channels for human and bot edits
    let (tx_human, rx_human) = mpsc::sync_channel::<Bytes>(CHANNEL_CAPACITY);
    let (tx_bot, rx_bot) = mpsc::sync_channel::<Bytes>(CHANNEL_CAPACITY);

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
        thread::spawn(move || loop {
            match recv_prioritized(&rx_human, &rx_bot) {
                Some(bytes) => {
                    let dequeue_time = SystemTime::now();
                    process_change_thread(bytes, dequeue_time);
                }
                None => break,
            }
        })
    };

    // -------------------------
    // Producer thread
    // -------------------------
    let producer_handle = thread::spawn(move || {
        let client = Client::new();
        let response = match client.get(url)
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

                            if let Ok(change) = serde_json::from_slice::<WikiChange<'_>>(&json_bytes) {
                                let lane = if change.bot.unwrap_or(false) { "BOT" } else { "HUMAN" };
                                let summary = change_summary(&change);
                                println!("INCOMING {} {}", lane, summary);

                                let sent = if change.bot.unwrap_or(false) {
                                    send_with_drop_oldest(
                                        &tx_bot_clone,
                                        &rx_bot_for_drop,
                                        json_bytes,
                                        lane,
                                        &summary,
                                    )
                                } else {
                                    send_with_drop_oldest(
                                        &tx_human_clone,
                                        &rx_human_for_drop,
                                        json_bytes,
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
    human_rx: &Arc<Mutex<Receiver<Bytes>>>,
    bot_rx: &Arc<Mutex<Receiver<Bytes>>>,
) -> Option<Bytes> {
    let mut human_closed = false;
    let mut bot_closed = false;
    loop {
        let human_state = {
            if let Ok(mut guard) = human_rx.lock() {
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
            if let Ok(mut guard) = bot_rx.lock() {
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
    tx: &SyncSender<Bytes>,
    rx: &Arc<Mutex<Receiver<Bytes>>>,
    payload: Bytes,
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
                    if let Ok(mut guard) = rx.lock() {
                        guard.try_recv().ok()
                    } else {
                        None
                    }
                };

                if let Some(oldest) = dropped {
                    let oldest_summary = summarize_bytes(&oldest);
                    log_overflow_event(lane, &oldest_summary, summary);
                } else {
                    println!("RETRY {} incoming=[{}]", lane, summary);
                    thread::sleep(Duration::from_micros(100));
                }
            }
        }
    }
}

fn process_change_thread(json_bytes: Bytes, dequeue_time: SystemTime) {
    if let Ok(change) = serde_json::from_slice::<WikiChange<'_>>(&json_bytes) {
        let label = if change.bot.unwrap_or(false) { "BOT" } else { "HUMAN" };
        println!("DEQUEUE {} {}", label, change_summary(&change));

        if let Some(timestamp) = change.timestamp_sec {
            let expected = UNIX_EPOCH + Duration::from_secs(timestamp as u64);
            if let Ok(drift) = dequeue_time.duration_since(expected) {
                println!("{} Scheduling Drift: {:?}", label, drift);
            }
        }
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
