use reqwest::Client;
use futures_util::StreamExt;
use tokio::sync::mpsc::{self, error::TrySendError};
use std::collections::VecDeque;
use std::time::{SystemTime, UNIX_EPOCH, Duration};
use crate::types::WikiChange;
use bytes::Bytes;

pub async fn run_async_pipeline() {
    let url = "https://stream.wikimedia.org/v2/stream/recentchange";

    // Separate channels for human and bot edits
    let (tx_human, mut rx_human) = mpsc::channel::<Bytes>(100);
    let (tx_bot, mut rx_bot) = mpsc::channel::<Bytes>(100);

    // Side queues for backpressure management
    let human_queue: VecDeque<Bytes> = VecDeque::with_capacity(100);
    let bot_queue: VecDeque<Bytes> = VecDeque::with_capacity(100);

    let human_queue = tokio::sync::Mutex::new(human_queue);
    let bot_queue = tokio::sync::Mutex::new(bot_queue);

    let tx_human_clone = tx_human.clone();
    let tx_bot_clone = tx_bot.clone();

    // Worker task: always prioritize human edits
    for _ in 0..4 { // 4 worker tasks
    let mut rx_human = rx_human.clone();
    let mut rx_bot = rx_bot.clone();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;
                Some(json_bytes) = rx_human.recv() => process_change(json_bytes).await,
                Some(json_bytes) = rx_bot.recv() => process_change(json_bytes).await,
                else => break,
            }
        }
    });
}

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
            if let Ok(change) = serde_json::from_slice::<WikiChange>(&json_bytes) {
                let target_tx;
                let target_queue;

                if change.bot.unwrap_or(false) {
                    target_tx = &tx_bot_clone;
                    target_queue = &bot_queue;
                } else {
                    target_tx = &tx_human_clone;
                    target_queue = &human_queue;
                }

                let mut guard = target_queue.lock().await;

                // Backpressure: drop oldest if full
                if guard.len() == 100 {
                    guard.pop_front();
                    println!("[{:?}] ⚠️ Overflow Event: Dropped oldest packet", SystemTime::now());
                }

                guard.push_back(json_bytes.clone());

                // Send to channel (non-blocking)
                if let Err(TrySendError::Closed(_)) = target_tx.try_send(json_bytes) {
                    break;
                }
            }
        }
    }
}

// Process a single WikiChange
async fn process_change(json_bytes: Bytes) {
    let start = SystemTime::now();

    if let Ok(change) = serde_json::from_slice::<WikiChange>(&json_bytes) {
        // Log the change
        println!("{:?}", change);

        // Calculate scheduling drift if timestamp exists
        let label = if change.bot.unwrap_or(false) { "🤖 BOT" } else { "👤 HUMAN" };

        if let Some(timestamp) = change.timestamp_sec {
            let expected = UNIX_EPOCH + Duration::from_secs(timestamp as u64);
            if let Ok(drift) = start.duration_since(expected) {
                // Labeling makes the demonstration clear in logs
                println!("{} Scheduling Drift: {:?}", label, drift);
            }
        }
    }

    if let Ok(elapsed) = start.elapsed() {
        println!("Processing time: {:?}", elapsed);
    }
}


//new
// async fn process_change(json_bytes: Bytes) {
//     let start_processing = SystemTime::now(); // The "Moment it leaves the channel"
//     let deadline = Duration::from_millis(2);

//     if let Ok(change) = serde_json::from_slice::<WikiChange>(&json_bytes) {
//         let label = if change.bot.unwrap_or(false) { "BOT" } else { "HUMAN" };

//         // 1. Calculate Scheduling Drift (Time spent in channel)
//         if let Some(timestamp) = change.timestamp_sec {
//             let event_time = UNIX_EPOCH + Duration::from_secs(timestamp as u64);
//             if let Ok(drift) = start_processing.duration_since(event_time) {
//                 println!("{} Scheduling Drift: {:?}", label, drift);
//             }
//         }

//         // 2. Perform the actual work (Printing/Analysis)
//         println!("{:?}", change);

//         // 3. Check Micro-Deadline
//         let duration = start_processing.elapsed().unwrap_or(Duration::from_secs(0));
//         if duration > deadline {
//             println!("🚨 DEADLINE MISSED: {:?} (Limit: 2ms)", duration);
//         } else {
//             println!("✅ Deadline Met: {:?}", duration);
//         }
//     }
// }