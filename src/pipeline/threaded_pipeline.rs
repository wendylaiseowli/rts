// Threaded version
use reqwest::blocking::Client;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime};
use std::io::{BufReader, BufRead};
use bytes::Bytes;
use crate::types::WikiChange;

pub fn run_threaded_pipeline() {
    let url = "https://stream.wikimedia.org/v2/stream/recentchange";

    let queue: Arc<Mutex<VecDeque<Bytes>>> = Arc::new(Mutex::new(VecDeque::with_capacity(100)));
    let queue_worker = queue.clone();

    // -------------------------
    // Worker thread
    // -------------------------
    let worker_handle = thread::spawn(move || loop {
        let maybe_json = {
            let mut guard = queue_worker.lock().unwrap();
            guard.pop_front()
        };

        if let Some(json_bytes) = maybe_json {
            let start = SystemTime::now();

            // Parse directly from Bytes (zero-copy for deserializer)
            if let Ok(change) = serde_json::from_slice::<WikiChange>(&json_bytes) {
                if let Some(true) = change.bot { continue; }
                println!("{:?}", change);
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
    let queue_producer = queue.clone();
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
        for line in reader.split(b'\n') {  // stable: split by newline
            match line {
                Ok(mut line_bytes) => {
                    if line_bytes.starts_with(b"data: ") {
                        // Remove "data: " prefix by slicing
                        let mut line_bytes = Bytes::from(line_bytes);
                        let json_bytes = line_bytes.split_off(6); 

                        let mut guard = queue_producer.lock().unwrap();

                        if guard.len() == 100 {
                            guard.pop_front();
                            println!(
                                "[{:?}] ⚠️ Overflow Event: Dropped oldest packet",
                                SystemTime::now()
                            );
                        }

                        guard.push_back(json_bytes);
                    }
                }
                Err(e) => eprintln!("Failed to read line: {:?}", e),
            }
        }
    });

    let _ = producer_handle.join();
    let _ = worker_handle.join();
}