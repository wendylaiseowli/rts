mod pipeline;
mod types;
use pipeline::async_pipeline::run_async_pipeline;
use pipeline::threaded_pipeline::run_threaded_pipeline;

#[tokio::main]
async fn main() {
    let mode = "threaded"; // change to "threaded"

    if mode == "async" {
        println!("Running Async Pipeline...");
        run_async_pipeline().await;
    } else {
        println!("Running Threaded Pipeline...");
        run_threaded_pipeline();
    }
}