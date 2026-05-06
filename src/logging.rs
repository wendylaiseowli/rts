use std::fs::OpenOptions;
use std::io::Write;

const LOG_PATH: &str = "pipeline.log";

pub fn log_line(message: impl AsRef<str>) {
    let message = message.as_ref();
    println!("{}", message);
    append_line(message);
}

pub fn log_error(message: impl AsRef<str>) {
    let message = message.as_ref();
    eprintln!("{}", message);
    append_line(message);
}

fn append_line(message: &str) {
    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(LOG_PATH) {
        let _ = writeln!(file, "{}", message);
    }
}
