// use reqwest::Client;
// use futures_util::StreamExt;
// use serde::Deserialize;
// use tokio::sync::Mutex;
// use std::collections::VecDeque;
// use std::sync::Arc;
// use std::time::{SystemTime, Duration};
// use bytes::Bytes;

#[derive(Debug, Deserialize)]
struct WikiChange<'a> {
    user: Option<&'a str>,
    bot: Option<bool>,
    server_name: Option<&'a str>,
}

// Wrap queued item with arrival timestamp
struct QueuedChange {
    arrival: SystemTime, // when the item was received
    bytes: Bytes,
}


// pub async fn run_async_pipeline() {
//     let url = "https://stream.wikimedia.org/v2/stream/recentchange";

//     let queue: Arc<Mutex<VecDeque<Bytes>>> = Arc::new(Mutex::new(VecDeque::with_capacity(100)));
//     let queue_worker = queue.clone();

//     // Worker task
//     tokio::spawn(async move {
//         loop {
//             let mut guard = queue_worker.lock().await;
//             if let Some(json_bytes) = guard.pop_front() {
//                 drop(guard);

//                 let start = SystemTime::now();
//                 if let Ok(change) = serde_json::from_slice::<WikiChange>(&json_bytes) {  // from_slice borrows from Bytes
//                     if let Some(true) = change.bot {
//                         continue;
//                     }
//                     println!("{:?}", change);
//                 }

//                 if let Ok(elapsed) = start.elapsed() {
//                     println!("Processing time: {:?}", elapsed);
//                 }
//             } else {
//                 tokio::time::sleep(Duration::from_millis(1)).await;
//             }
//         }
//     });

//     // Producer (Stream ingestion)
//     let client = Client::new();
//     let response = client
//         .get(url)
//         .header("User-Agent", "WendyRTSProject/1.0 (learning project)")
//         .send()
//         .await
//         .unwrap();

//     let mut stream = response.bytes_stream();

//     while let Some(item) = stream.next().await {
//         let chunk = item.unwrap(); // chunk: Bytes
//         if chunk.starts_with(b"data: ") {           // compare with byte slice
//             let json_bytes = chunk.slice(6..);      // zero-copy slice
//             let mut guard = queue.lock().await;

//             if guard.len() == 100 {
//                 guard.pop_front();
//                 println!(
//                     "[{:?}] ⚠️ Overflow Event: Dropped oldest packet",
//                     SystemTime::now()
//                 );
//             }
//             guard.push_back(json_bytes);  // store Bytes directly
//         }
//     }
// }


use reqwest::blocking::Client;
use serde::Deserialize;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime};
use std::io::{BufReader, BufRead};
use bytes::Bytes;

pub fn run_threaded_pipeline() {
    let url = "https://stream.wikimedia.org/v2/stream/recentchange";

    // -------------------------
    // Two separate queues for priority
    // -------------------------
    let human_queue: Arc<Mutex<VecDeque<Bytes>>> = Arc::new(Mutex::new(VecDeque::with_capacity(100)));
    let bot_queue: Arc<Mutex<VecDeque<Bytes>>> = Arc::new(Mutex::new(VecDeque::with_capacity(100)));

    let human_worker = human_queue.clone();
    let bot_worker = bot_queue.clone();

    // -------------------------
    // Worker thread (priority)
    // -------------------------
    let worker_handle = thread::spawn(move || loop {
        let maybe_json = {
            // Always try human queue first
            let mut guard = human_worker.lock().unwrap();
            if let Some(json) = guard.pop_front() {
                Some(json)
            } else {
                // If human queue empty, try bot queue
                let mut bot_guard = bot_worker.lock().unwrap();
                bot_guard.pop_front()
            }
        };

        if let Some(json_bytes) = maybe_json {
            let start = SystemTime::now();

            if let Ok(change) = serde_json::from_slice::<WikiChange>(&json_bytes) {
                if change.bot.unwrap_or(false) {
                    println!("Bot Edit: {:?}", change);
                } else {
                    println!("Human Edit: {:?}", change);
                }
            }

            if let Ok(elapsed) = start.elapsed() {
                println!("Processing time: {:?}", elapsed);
            }
        } else {
            thread::sleep(Duration::from_millis(1));
        }
    });

    // -------------------------
    // Producer thread
    // -------------------------
    let human_producer = human_queue.clone();
    let bot_producer = bot_queue.clone();
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

        let reader = BufReader::new(response);
        for line in reader.split(b'\n') {
            match line {
                Ok(mut line_bytes) => {
                    if line_bytes.starts_with(b"data: ") {
                        let json_bytes = Bytes::copy_from_slice(&line_bytes[6..]);

                        // Peek bot flag from raw JSON bytes
                        let is_bot = json_bytes.windows(7).any(|w| w == b"\"bot\":true");

                        if is_bot {
                            let mut guard = bot_producer.lock().unwrap();
                            if guard.len() == 100 { guard.pop_front(); }
                            guard.push_back(json_bytes);
                        } else {
                            let mut guard = human_producer.lock().unwrap();
                            if guard.len() == 100 { guard.pop_front(); }
                            guard.push_back(json_bytes);
                        }
                    }
                }
                Err(e) => eprintln!("Failed to read line: {:?}", e),
            }
        }
    });

    let _ = producer_handle.join();
    let _ = worker_handle.join();
}


#[tokio::main]
async fn main() {
    //Run async version
    // println!("Running Async/Await Pipeline...");
    // tokio::spawn(async { run_async_pipeline().await }).await.unwrap();

    // Or run threaded version
    println!("Running Threaded Pipeline...");
    run_threaded_pipeline();
}