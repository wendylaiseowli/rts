use crate::logging::log_line;
use std::collections::HashMap;
use std::fs;
use std::time::Duration;

const DEFAULT_LEADERBOARD_HTML_PATH: &str = "leaderboard_dashboard.html";

#[derive(Debug, Default, Clone)]
pub struct DriftStats {
    processed: u64,
    total_scheduling_drift_ns: i128,
    max_scheduling_drift_ns: i128,
    min_scheduling_drift_ns: i128,
    total_processing_time: Duration,
    max_processing_time: Duration,
    deadline_misses: u64,
    max_jitter: Duration,
    jitter_breaches: u64,
}

#[derive(Debug, Default, Clone)]
pub struct DomainLeaderboard {
    counts: HashMap<String, u64>,
}

impl DomainLeaderboard {
    pub fn record(&mut self, domain: &str) {
        *self.counts.entry(domain.to_string()).or_insert(0) += 1;
    }

    pub fn top_3(&self) -> Vec<(String, u64)> {
        let mut items: Vec<(String, u64)> = self
            .counts
            .iter()
            .map(|(domain, count)| (domain.clone(), *count))
            .collect();

        items.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        items.truncate(3);
        items
    }

    pub fn write_html(&self, path: Option<&str>) -> std::io::Result<()> {
        let path = path.unwrap_or(DEFAULT_LEADERBOARD_HTML_PATH);
        let top_3 = self.top_3();
        let rows = top_3
            .iter()
            .enumerate()
            .map(|(idx, (domain, count))| {
                format!(
                    "<tr><td>{}</td><td>{}</td><td>{}</td></tr>",
                    idx + 1,
                    escape_html(domain),
                    count
                )
            })
            .collect::<Vec<_>>()
            .join("");

        let html = format!(
            r##"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta http-equiv="refresh" content="2" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Wikipedia Domain Leaderboard</title>
  <style>
    :root {{
      color-scheme: dark;
      --bg: #0b1020;
      --panel: #111827;
      --line: #243041;
      --text: #e5e7eb;
      --muted: #94a3b8;
      --accent: #22c55e;
    }}
    body {{
      margin: 0;
      font-family: Arial, sans-serif;
      background: radial-gradient(circle at top, #172033 0, var(--bg) 50%);
      color: var(--text);
      padding: 32px;
    }}
    .card {{
      max-width: 800px;
      margin: 0 auto;
      background: rgba(17, 24, 39, 0.92);
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 24px;
      box-shadow: 0 24px 80px rgba(0, 0, 0, 0.35);
    }}
    h1 {{
      margin: 0 0 8px 0;
      font-size: 28px;
    }}
    p {{
      margin: 0 0 20px 0;
      color: var(--muted);
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      overflow: hidden;
    }}
    th, td {{
      text-align: left;
      padding: 14px 12px;
      border-bottom: 1px solid var(--line);
    }}
    th {{
      color: var(--muted);
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      font-size: 12px;
    }}
    tr:first-child td {{
      color: #d1fae5;
    }}
    .badge {{
      display: inline-block;
      margin-top: 16px;
      padding: 8px 12px;
      border-radius: 999px;
      background: rgba(34, 197, 94, 0.12);
      color: var(--accent);
      font-size: 12px;
      font-weight: 700;
      letter-spacing: 0.04em;
      text-transform: uppercase;
    }}
  </style>
</head>
<body>
  <div class="card">
    <h1>Wikipedia Domain Leaderboard</h1>
    <p>Live top 3 edited domains from the SSE stream. Auto-refreshes every 2 seconds.</p>
    <table>
      <thead>
        <tr><th>Rank</th><th>Domain</th><th>Edits</th></tr>
      </thead>
      <tbody>
        {rows}
      </tbody>
    </table>
    <div class="badge">Live shared leaderboard</div>
  </div>
</body>
</html>"##
        );

        fs::write(path, html)
    }
}

fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

impl DriftStats {
    pub fn record(
        &mut self,
        scheduling_drift_ns: i128,
        processing_time: Duration,
        deadline: Duration,
    ) {
        self.processed += 1;
        self.total_scheduling_drift_ns += scheduling_drift_ns;
        self.total_processing_time += processing_time;
        self.max_scheduling_drift_ns = self.max_scheduling_drift_ns.max(scheduling_drift_ns);
        if self.processed == 1 || scheduling_drift_ns < self.min_scheduling_drift_ns {
            self.min_scheduling_drift_ns = scheduling_drift_ns;
        }
        self.max_processing_time = self.max_processing_time.max(processing_time);
        if processing_time > deadline {
            self.deadline_misses += 1;
        }
    }

    pub fn processed(&self) -> u64 {
        self.processed
    }

    pub fn observe_jitter(&mut self, jitter: Duration, threshold: Duration) -> bool {
        self.max_jitter = self.max_jitter.max(jitter);
        if jitter > threshold {
            self.jitter_breaches += 1;
            return true;
        }
        false
    }

    pub fn report(&self) {
        if self.processed == 0 {
            return;
        }

        let count = self.processed as f64;
        let avg_scheduling_drift_ns = self.total_scheduling_drift_ns as f64 / count;
        let avg_processing_time =
            Duration::from_secs_f64(self.total_processing_time.as_secs_f64() / count);

        log_line(format!(
            "DRIFT REPORT count={} avg_expected=2ms avg_actual_processing_time={:?} avg_scheduling_drift_ns={:.0} max_scheduling_drift_ns={} min_scheduling_drift_ns={} max_processing_time={:?} deadline_misses={} max_jitter={:?} jitter_breaches={}",
            self.processed,
            avg_processing_time,
            avg_scheduling_drift_ns,
            self.max_scheduling_drift_ns,
            self.min_scheduling_drift_ns,
            self.max_processing_time,
            self.deadline_misses,
            self.max_jitter,
            self.jitter_breaches
        ));
    }
}

