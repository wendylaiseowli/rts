// Async/Await version
use reqwest::Client;
use futures_util::StreamExt;
use tokio::sync::Mutex;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{SystemTime, Duration};
use bytes::Bytes;
use crate::types::WikiChange;

pub async fn run_async_pipeline() {
    let url = "https://stream.wikimedia.org/v2/stream/recentchange";

    let queue: Arc<Mutex<VecDeque<Bytes>>> = Arc::new(Mutex::new(VecDeque::with_capacity(100)));
    let queue_worker = queue.clone();

    // Worker task
    tokio::spawn(async move {
        loop {
            let mut guard = queue_worker.lock().await;
            if let Some(json_bytes) = guard.pop_front() {
                drop(guard);

                let start = SystemTime::now();
                if let Ok(change) = serde_json::from_slice::<WikiChange>(&json_bytes) {  // from_slice borrows from Bytes
                    if let Some(true) = change.bot {
                        continue;
                    }
                    println!("{:?}", change);
                }

                if let Ok(elapsed) = start.elapsed() {
                    println!("Processing time: {:?}", elapsed);
                }
            } else {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
    });

    // Producer (Stream ingestion)
    let client = Client::new();
    let response = client
        .get(url)
        .header("User-Agent", "WendyRTSProject/1.0 (learning project)")
        .send()
        .await
        .unwrap();

    let mut stream = response.bytes_stream();

    while let Some(item) = stream.next().await {
        let chunk = item.unwrap(); // chunk: Bytes
        if chunk.starts_with(b"data: ") {           // compare with byte slice
            let json_bytes = chunk.slice(6..);      // zero-copy slice
            let mut guard = queue.lock().await;

            if guard.len() == 100 {
                guard.pop_front();
                println!(
                    "[{:?}] ⚠️ Overflow Event: Dropped oldest packet",
                    SystemTime::now()
                );
            }
            guard.push_back(json_bytes);  // store Bytes directly
        }
    }
}