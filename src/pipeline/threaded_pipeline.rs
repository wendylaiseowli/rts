use reqwest::blocking::Client;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use bytes::Bytes;
use crate::types::WikiChange;
use std::io::Read;

pub fn run_threaded_pipeline() {
    let url = "https://stream.wikimedia.org/v2/stream/recentchange";

    // Bounded channels for human and bot edits
    let (tx_human, rx_human) = mpsc::sync_channel::<Bytes>(100);
    let (tx_bot, rx_bot) = mpsc::sync_channel::<Bytes>(100);

    let rx_human = Arc::new(Mutex::new(rx_human));
    let rx_bot = Arc::new(Mutex::new(rx_bot));

    // Side queues for backpressure
    let human_queue: VecDeque<Bytes> = VecDeque::with_capacity(100);
    let bot_queue: VecDeque<Bytes> = VecDeque::with_capacity(100);

    let human_queue = Arc::new(Mutex::new(human_queue));
    let bot_queue = Arc::new(Mutex::new(bot_queue));

    let tx_human_clone = tx_human.clone();
    let tx_bot_clone = tx_bot.clone();
    let human_queue_clone = human_queue.clone();
    let bot_queue_clone = bot_queue.clone();

    // -------------------------
    // Worker thread (Priority: Human first)
    // -------------------------
    let worker_handle = {
        let rx_human = rx_human.clone();
        let rx_bot = rx_bot.clone();
        thread::spawn(move || loop {
            let start = SystemTime::now();

            let mut got_packet = false;

            // 1️⃣ Try human edit first
            if let Ok(mut guard) = rx_human.lock() {
                if let Ok(json_bytes) = guard.try_recv() {
                    process_change_thread(json_bytes, "👤 HUMAN", start);
                    got_packet = true;
                }
            }

            // 2️⃣ Then try bot edit
            if !got_packet {
                if let Ok(mut guard) = rx_bot.lock() {
                    if let Ok(json_bytes) = guard.try_recv() {
                        process_change_thread(json_bytes, "🤖 BOT", start);
                    }
                }
            }

            if !got_packet {
                // Sleep briefly to avoid busy wait
                thread::sleep(Duration::from_millis(1));
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

                            if let Ok(change) = serde_json::from_slice::<WikiChange>(&json_bytes) {
                                let (target_tx, target_queue) = if change.bot.unwrap_or(false) {
                                    (&tx_bot_clone, &bot_queue_clone)
                                } else {
                                    (&tx_human_clone, &human_queue_clone)
                                };

                                // Backpressure: drop oldest if full
                                if let Ok(mut guard) = target_queue.lock() {
                                    if guard.len() == 100 {
                                        guard.pop_front();
                                        println!(
                                            "[{:?}] ⚠️ Overflow Event: Dropped oldest packet",
                                            SystemTime::now()
                                        );
                                    }
                                    guard.push_back(json_bytes.clone());
                                }

                                // Try to send to channel (non-blocking)
                                let _ = target_tx.try_send(json_bytes);
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

// Worker processing function
fn process_change_thread(json_bytes: Bytes, label: &str, start_processing: SystemTime) {
    let deadline = Duration::from_millis(2);

    if let Ok(change) = serde_json::from_slice::<WikiChange>(&json_bytes) {
        // Scheduling drift
        if let Some(timestamp) = change.timestamp_sec {
            let event_time = UNIX_EPOCH + Duration::from_secs(timestamp as u64);
            if let Ok(drift) = start_processing.duration_since(event_time) {
                println!("{} Scheduling Drift: {:?}", label, drift);
            }
        }

        println!("{:?}", change);

        // Micro-deadline check
        if let Ok(elapsed) = start_processing.elapsed() {
            if elapsed > deadline {
                println!("🚨 DEADLINE MISSED: {:?} (Limit: 2ms)", elapsed);
            } else {
                println!("✅ Deadline Met: {:?}", elapsed);
            }
        }
    }
}
// use reqwest::blocking::Client;
// use std::collections::VecDeque;
// use std::sync::{Arc, Mutex};
// use std::thread;
// use std::time::{Duration, SystemTime};
// use bytes::Bytes;
// use serde_json;
// use crate::types::WikiChange;
// use std::io::BufRead;
// use std::io::BufReader;

// // Packet with timestamp (for drift measurement)
// struct Packet {
//     data: Bytes,
//     arrival_time: SystemTime,
// }

// pub fn run_threaded_pipeline() {
//     let url = "https://stream.wikimedia.org/v2/stream/recentchange";

//     // Two priority queues
//     let high_queue: Arc<Mutex<VecDeque<Packet>>> =
//         Arc::new(Mutex::new(VecDeque::new())); // human
//     let low_queue: Arc<Mutex<VecDeque<Packet>>> =
//         Arc::new(Mutex::new(VecDeque::new())); // bot

//     let high_worker = high_queue.clone();
//     let low_worker = low_queue.clone();

//     // =========================
//     // Worker Thread (Priority Scheduling)
//     // =========================
//     let worker_handle = thread::spawn(move || loop {
//         let maybe_packet = {
//             let mut high = high_worker.lock().unwrap();

//             if let Some(pkt) = high.pop_front() {
//                 Some(pkt)
//             } else {
//                 drop(high);
//                 let mut low = low_worker.lock().unwrap();
//                 low.pop_front()
//             }
//         };

//         if let Some(packet) = maybe_packet {
//             let now = SystemTime::now();

//             if let Ok(drift) = now.duration_since(packet.arrival_time) {
//                 if let Ok(change) =
//                     serde_json::from_slice::<WikiChange>(&packet.data)
//                 {
//                     if let Some(true) = change.bot {
//                         println!("BOT drift: {:?}", drift);
//                     } else {
//                         println!("HUMAN drift: {:?}", drift);
//                     }

//                     println!("{:?}", change);
//                 }
//             }
//         } else {
//             thread::sleep(Duration::from_millis(40));
//         }
//     });

//     // =========================
//     // Producer Thread
//     // =========================
//     let high_producer = high_queue.clone();
//     let low_producer = low_queue.clone();

//     let producer_handle = thread::spawn(move || {
//         let client = Client::new();

//         let response = match client
//             .get(url)
//             .header("User-Agent", "WendyRTSProject/1.0")
//             .send()
//         {
//             Ok(r) => r,
//             Err(e) => {
//                 eprintln!("Connection failed: {:?}", e);
//                 return;
//             }
//         };

//         let reader = BufReader::new(response);

//         for line in reader.lines() {
//             if let Ok(line) = line {
//                 if line.starts_with("data: ") {
//                     let json_str = &line[6..];

//                     // Convert to Bytes (for zero-copy parsing later)
//                     let json_bytes = Bytes::copy_from_slice(json_str.as_bytes());

//                     // TEMP parse to check bot (small cost, acceptable)
//                     let is_bot = if let Ok(change) =
//                         serde_json::from_slice::<WikiChange>(&json_bytes)
//                     {
//                         change.bot.unwrap_or(false)
//                     } else {
//                         false
//                     };

//                     let mut high = high_producer.lock().unwrap();
//                     let mut low = low_producer.lock().unwrap();

//                     // =========================
//                     // GLOBAL CAPACITY CHECK (50)
//                     // =========================
//                     let total = high.len() + low.len();

//                     if total >= 4 {
//                         if !low.is_empty() {
//                             low.pop_front();
//                             println!(
//                                 "[{:?}] ⚠️ Overflow: Dropped BOT packet",
//                                 SystemTime::now()
//                             );
//                         } else {
//                             high.pop_front();
//                             println!(
//                                 "[{:?}] ⚠️ Overflow: Dropped HUMAN packet",
//                                 SystemTime::now()
//                             );
//                         }
//                     }

//                     // =========================
//                     // Insert with timestamp
//                     // =========================
//                     let packet = Packet {
//                         data: json_bytes,
//                         arrival_time: SystemTime::now(),
//                     };

//                     if is_bot {
//                         low.push_back(packet);
//                     } else {
//                         high.push_back(packet);
//                     }
//                 }
//             }
//         }
//     });

//     let _ = producer_handle.join();
//     let _ = worker_handle.join();
// }