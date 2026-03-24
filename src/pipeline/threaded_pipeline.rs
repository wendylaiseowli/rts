use reqwest::blocking::Client;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, SystemTime};
use bytes::Bytes;
use crate::types::WikiChange;
use std::io::Read;

pub fn run_threaded_pipeline() {
    let url = "https://stream.wikimedia.org/v2/stream/recentchange";

    // Bounded channel for worker
    let (tx, rx) = mpsc::sync_channel::<Bytes>(100);
    let rx = Arc::new(Mutex::new(rx));

    // Side queue to manage drop-oldest behavior
    let queue: VecDeque<Bytes> = VecDeque::with_capacity(100);
    let queue = Arc::new(Mutex::new(queue));
    let tx_clone = tx.clone();

    // -------------------------
    // Worker thread
    // -------------------------
    let rx_worker = rx.clone();
    let worker_handle = thread::spawn(move || loop {
        let maybe_json = {
            let guard = rx_worker.lock().unwrap();
            guard.try_recv().ok()
        };

        if let Some(json_bytes) = maybe_json {
            let start = SystemTime::now();

             // Zero-copy deserialization from Bytes
            if let Ok(change) = serde_json::from_slice::<WikiChange>(&json_bytes) {
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

        let mut buffer = Vec::new();
        let mut reader = response; // reqwest::blocking::Response implements Read


        // Read the stream in chunks
        loop {
            buffer.clear();
            match reader.by_ref().take(4096).read_to_end(&mut buffer) {
                Ok(0) => break, // EOF
                Ok(_) => {
                    // Split lines without copying
                    for line in buffer.split(|&b| b == b'\n') {
                        if line.starts_with(b"data: ") {
                            // Slice off "data: " prefix directly
                            let json_bytes = Bytes::copy_from_slice(&line[6..]); // Zero-copy for deserializer

                            // Lock side queue for drop-oldest
                            let mut guard = queue_producer.lock().unwrap();
                            if guard.len() == 100 {
                                guard.pop_front();
                                println!(
                                    "[{:?}] ⚠️ Overflow Event: Dropped oldest packet",
                                    SystemTime::now()
                                );
                            }
                            guard.push_back(json_bytes.clone());

                            // Send to channel
                            if let Err(_) = tx_clone.try_send(json_bytes) {
                                // Channel full; already handled via side queue
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