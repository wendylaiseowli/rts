use std::fs::OpenOptions;
use std::io::Write;
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

const LOG_PATH: &str = "pipeline.log";
static LOG_INITIALIZED: OnceLock<()> = OnceLock::new();
static LOG_WRITE_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

const TIMESTAMP_W: usize = 20;
const EVENT_W: usize = 20;
const LANE_W: usize = 7;
const USER_W: usize = 24;
const BOT_W: usize = 5;
const SERVER_W: usize = 24;
const ACTUAL_W: usize = 10;
const EXPECTED_W: usize = 10;
const DRIFT_W: usize = 12;
const DETAILS_W: usize = 72;

const LOG_HEADER: &str = "timestamp            | event                | lane   | user                     | bot   | server                  | actual     | expected   | drift_ns     | details";

pub fn init_log_file() {
    let _ = LOG_INITIALIZED.get_or_init(|| {
        if let Ok(mut file) = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(LOG_PATH)
        {
            let _ = writeln!(file, "{}", LOG_HEADER);
        }
    });
}

pub fn log_line(message: impl AsRef<str>) {
    let message = message.as_ref();
    println!("{}", message);
    append_row(message);
}

pub fn log_error(message: impl AsRef<str>) {
    let message = message.as_ref();
    eprintln!("{}", message);
    append_row(message);
}

fn append_row(message: &str) {
    let lock = LOG_WRITE_LOCK.get_or_init(|| Mutex::new(()));
    let _guard = lock.lock().ok();
    init_log_file();
    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(LOG_PATH) {
        let parsed = parse_log_message(message);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        let _ = writeln!(
            file,
            "{}",
            format_row(&[
                (format!("{}.{:09}", timestamp.as_secs(), timestamp.subsec_nanos()), TIMESTAMP_W),
                (parsed.event, EVENT_W),
                (parsed.lane, LANE_W),
                (parsed.user, USER_W),
                (parsed.bot, BOT_W),
                (parsed.server, SERVER_W),
                (parsed.actual, ACTUAL_W),
                (parsed.expected, EXPECTED_W),
                (parsed.drift_ns, DRIFT_W),
                (parsed.details, DETAILS_W),
            ])
        );
    }
}

#[derive(Default)]
struct ParsedLogMessage {
    event: String,
    lane: String,
    user: String,
    bot: String,
    server: String,
    actual: String,
    expected: String,
    drift_ns: String,
    details: String,
}

fn parse_log_message(message: &str) -> ParsedLogMessage {
    let mut parsed = ParsedLogMessage {
        event: classify_event(message).to_string(),
        lane: extract_lane(message),
        user: extract_some_value(message, "user=", Some(" bot=")),
        bot: extract_some_value(message, "bot=", Some(" server=")),
        server: extract_some_value(message, "server=", None),
        actual: extract_after(message, "Actual=", Some(" Expected=")).unwrap_or_default(),
        expected: extract_after(message, "Expected=", Some(" Scheduling Drift(ns)=")).unwrap_or_default(),
        drift_ns: extract_after(message, "Scheduling Drift(ns)=", None).unwrap_or_default(),
        details: sanitize_field(message),
    };

    if parsed.event.is_empty() {
        parsed.event = "MESSAGE".to_string();
    }

    if parsed.event == "TIMING" || parsed.event == "DEADLINE_MET" || parsed.event == "DEADLINE_MISSED" {
        parsed.details.clear();
    }

    if parsed.event == "OVERFLOW_DROP" {
        parsed.details = sanitize_field(message);
    }

    if parsed.event == "DRIFT_REPORT" {
        parsed.details = extract_after(message, "count=", None).unwrap_or_default();
    }

    parsed
}

fn classify_event(message: &str) -> &'static str {
    if message.contains("DRIFT REPORT") {
        "DRIFT_REPORT"
    } else if message.contains("OVERFLOW DROP") {
        "OVERFLOW_DROP"
    } else if message.contains("DEGRADED MODE ENTERED") {
        "DEGRADED_MODE_ENTERED"
    } else if message.contains("DEGRADED MODE:") {
        "DEGRADED_MODE"
    } else if message.contains("BOT OVERRIDDEN") {
        "BOT_OVERRIDDEN"
    } else if message.contains("Deadline met") {
        "DEADLINE_MET"
    } else if message.contains("DEADLINE MISSED") {
        "DEADLINE_MISSED"
    } else if message.contains("Actual=") && message.contains("Scheduling Drift(ns)=") {
        "TIMING"
    } else if message.contains("INCOMING") {
        "INCOMING"
    } else if message.contains("ENQUEUE") {
        "ENQUEUE"
    } else if message.contains("DEQUEUE") {
        "DEQUEUE"
    } else if message.contains("RETRY") {
        "RETRY"
    } else if message.contains("Network Reset") {
        "NETWORK_RESET"
    } else if message.contains("Connecting") {
        "CONNECT"
    } else if message.contains("Failed") {
        "ERROR"
    } else {
        ""
    }
}

fn extract_lane(message: &str) -> String {
    for marker in ["INCOMING ", "ENQUEUE ", "DEQUEUE ", "RETRY ", "OVERFLOW DROP "] {
        if let Some(rest) = message.split_once(marker).map(|(_, rest)| rest) {
            if let Some(lane) = rest.split_whitespace().next() {
                if lane == "HUMAN" || lane == "BOT" {
                    return lane.to_string();
                }
            }
        }
    }

    if message.contains("HUMAN packet") {
        return "HUMAN".to_string();
    }
    if message.contains("BOT packet") {
        return "BOT".to_string();
    }

    String::new()
}

fn extract_field(message: &str, start: &str, end: Option<&str>) -> Option<String> {
    let value = message.split_once(start)?.1;
    let raw = if let Some(end) = end {
        value.split_once(end).map(|(head, _)| head).unwrap_or(value)
    } else {
        value
    };
    Some(raw.trim().to_string())
}

fn extract_after(message: &str, start: &str, end: Option<&str>) -> Option<String> {
    let value = message.split_once(start)?.1;
    let raw = if let Some(end) = end {
        value.split_once(end).map(|(head, _)| head).unwrap_or(value)
    } else {
        value
    };
    Some(raw.trim().trim_matches(|c| c == ',' || c == ')' || c == '.').to_string())
}

fn extract_some_value(message: &str, start: &str, end: Option<&str>) -> String {
    let Some(value) = extract_field(message, start, end) else {
        return String::new();
    };

    let value = value.trim();
    if value == "None" {
        return String::new();
    }

    if let Some(inner) = value.strip_prefix("Some(\"") {
        if let Some(end_idx) = inner.find("\")") {
            return inner[..end_idx].to_string();
        }
    }

    if let Some(inner) = value.strip_prefix("Some(").and_then(|v| v.strip_suffix(')')) {
        return inner.trim_matches('"').to_string();
    }

    value.to_string()
}

fn sanitize_field(value: &str) -> String {
    value
        .replace('\t', " ")
        .replace('\r', " ")
        .replace('\n', " ")
}

fn format_row(columns: &[(String, usize)]) -> String {
    columns
        .iter()
        .map(|(value, width)| pad_or_truncate(value, *width))
        .collect::<Vec<_>>()
        .join(" | ")
}

fn pad_or_truncate(value: &str, width: usize) -> String {
    let trimmed: String = value.chars().take(width).collect();
    format!("{trimmed:<width$}")
}
