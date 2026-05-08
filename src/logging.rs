use std::fs::OpenOptions;
use std::io::Write;
use std::sync::OnceLock;

const LOG_PATH: &str = "pipeline.log";
static LOG_INITIALIZED: OnceLock<()> = OnceLock::new();

pub fn init_log_file() {
    let _ = LOG_INITIALIZED.get_or_init(|| {
        let _ = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(LOG_PATH);
    });
}

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
    init_log_file();
    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(LOG_PATH) {
        let _ = writeln!(file, "{}", message);
    }
}
