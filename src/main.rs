mod logging;
mod parser;
mod metrics;
mod pipeline;
mod types;
use logging::init_log_file;
use pipeline::async_pipeline::run_async_pipeline;
use pipeline::threaded_pipeline::run_threaded_pipeline;

#[tokio::main]
async fn main() {
    init_log_file();
    let mode = "threaded"; // change to "threaded"

    if mode == "async" {
        println!("Running Async Pipeline...");
        run_async_pipeline().await;
    } else {
        println!("Running Threaded Pipeline...");
        run_threaded_pipeline();
    }
}
