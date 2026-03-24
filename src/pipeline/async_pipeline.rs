use reqwest::Client;
use futures_util::StreamExt;
use tokio::sync::mpsc::{self, error::TrySendError};
use std::collections::VecDeque;
use std::time::{SystemTime};
use crate::types::WikiChange;
use bytes::Bytes;

pub async fn run_async_pipeline() {
    let url = "https://stream.wikimedia.org/v2/stream/recentchange";

    // Bounded channel for worker, capacity 100
    let (tx, mut rx) = mpsc::channel::<Bytes>(100);

    // Side queue to manage drop-oldest
    let queue: VecDeque<Bytes> = VecDeque::with_capacity(100);
    let queue = tokio::sync::Mutex::new(queue);
    let tx_clone = tx.clone();

    // Worker task
    tokio::spawn(async move {
        while let Some(json_bytes) = rx.recv().await {
            let start = SystemTime::now();
            if let Ok(change) = serde_json::from_slice::<WikiChange>(&json_bytes) {
                println!("{:?}", change);
            }
            if let Ok(elapsed) = start.elapsed() {
                println!("Processing time: {:?}", elapsed);
            }
        }
    });

    // Producer (stream ingestion)
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

            let mut guard = queue.lock().await;

            // Drop oldest if full
            if guard.len() == 100 {
                guard.pop_front();
                println!(
                    "[{:?}] ⚠️ Overflow Event: Dropped oldest packet",
                    SystemTime::now()
                );
            }

            guard.push_back(json_bytes.clone());

            // Try sending to the channel without blocking
            if let Err(TrySendError::Closed(_)) = tx_clone.try_send(json_bytes) {
                // Receiver closed; exit producer loop
                break;
            }
        }
    }
}