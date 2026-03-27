use crate::types::WikiChange;
use bytes::Bytes;
use futures_util::StreamExt;
use reqwest::Client;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{Mutex, mpsc};

const CHANNEL_CAPACITY: usize = 2;
const DEADLINE: Duration = Duration::from_millis(2);

#[derive(Clone)]
struct QueuedChange {
    payload: Bytes,
    enqueued_at: SystemTime,
}

pub async fn run_async_pipeline() {
    let url = "https://stream.wikimedia.org/v2/stream/recentchange";

    // Separate channels for human and bot edits
    let (tx_human, rx_human) = mpsc::channel::<QueuedChange>(CHANNEL_CAPACITY);
    let (tx_bot, rx_bot) = mpsc::channel::<QueuedChange>(CHANNEL_CAPACITY);

    // Receivers are shared so producer can drop oldest item when a lane is full.
    let rx_human = Arc::new(Mutex::new(rx_human));
    let rx_bot = Arc::new(Mutex::new(rx_bot));

    let tx_human_clone = tx_human.clone();
    let tx_bot_clone = tx_bot.clone();
    let rx_human_worker = Arc::clone(&rx_human);
    let rx_bot_worker = Arc::clone(&rx_bot);

    // Worker task: always prioritize human edits
    tokio::spawn(async move {
        loop {
            if let Some(message) = recv_prioritized(&rx_human_worker, &rx_bot_worker).await {
                // Deadline starts when packet leaves the ingestion channel.
                let dequeue_time = SystemTime::now();
                process_change(message, dequeue_time).await;
            } else {
                break;
            }
        }
    });

    // Producer: ingest stream
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

            // Parse the change zero-copy
            if let Ok(change) = serde_json::from_slice::<WikiChange<'_>>(&json_bytes) {
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
    loop {
        let human_state = {
            let mut guard = human_rx.lock().await;
            guard.try_recv()
        };
        match human_state {
            Ok(bytes) => return Some(bytes),
            Err(mpsc::error::TryRecvError::Disconnected) => {
                let bot_state = {
                    let mut guard = bot_rx.lock().await;
                    guard.try_recv()
                };
                return bot_state.ok();
            }
            Err(mpsc::error::TryRecvError::Empty) => {}
        }

        let bot_state = {
            let mut guard = bot_rx.lock().await;
            guard.try_recv()
        };
        match bot_state {
            Ok(bytes) => return Some(bytes),
            Err(mpsc::error::TryRecvError::Disconnected) => {
                let human_state = {
                    let mut guard = human_rx.lock().await;
                    guard.try_recv()
                };
                return human_state.ok();
            }
            Err(mpsc::error::TryRecvError::Empty) => {}
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
                println!("ENQUEUE {} {}", lane, summary);
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
                        "[{}.{:09}] ⚠️ Overflow Event: DROP {} dropped=[{}] incoming=[{}]",
                        ts.as_secs(),
                        ts.subsec_nanos(),
                        lane,
                        oldest_summary,
                        summary,
                    );
                } else {
                    println!("RETRY {} incoming=[{}]", lane, summary);
                    tokio::time::sleep(Duration::from_micros(100)).await;
                }
            }
        }
    }
}

// Process a single WikiChange
async fn process_change(packet: QueuedChange, dequeue_time: SystemTime) {
    let start = dequeue_time;
    let queue_drift = match dequeue_time.duration_since(packet.enqueued_at) {
        Ok(delta) => delta,
        Err(_) => Duration::ZERO,
    };

    if let Ok(change) = serde_json::from_slice::<WikiChange<'_>>(&packet.payload) {
        // Keep packet logs compact to reduce log-induced contention.
        let label = if change.bot.unwrap_or(false) {
            "BOT"
        } else {
            "HUMAN"
        };
        println!("DEQUEUE {} {}", label, change_summary(&change));

        // Scheduling drift is the time spent waiting in the queue.
        println!("{} Scheduling Drift: {:?}", label, queue_drift);
    }

    if let Ok(elapsed) = start.elapsed() {
        if elapsed > DEADLINE {
            println!("🚨 DEADLINE MISSED: {:?} (limit: {:?})", elapsed, DEADLINE);
        } else {
            println!("✅ Deadline met: {:?}", elapsed);
        }
    }
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
