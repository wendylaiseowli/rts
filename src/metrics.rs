use crate::logging::log_line;
use plotters::prelude::*;
use plotters::series::DashedLineSeries;
use plotters::series::LineSeries;
use std::collections::HashMap;
use std::fs;
use std::time::Duration;

const DEFAULT_LEADERBOARD_HTML_PATH: &str = "leaderboard_dashboard.html";
const DEFAULT_DRIFT_SVG_PATH: &str = "scheduling_drift_over_time.svg";
const DEFAULT_LANE_DRIFT_HTML_PATH: &str = "lane_drift_summary.html";

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EditLane {
    Human,
    Bot,
}

#[derive(Debug, Clone)]
struct DriftPoint {
    index: u64,
    lane: EditLane,
    drift_ns: i128,
}

#[derive(Debug, Default, Clone)]
pub struct DriftTimeline {
    samples: Vec<DriftPoint>,
    next_index: u64,
}

#[derive(Debug, Default, Clone)]
pub struct LaneDriftStats {
    human: LaneStats,
    bot: LaneStats,
}

#[derive(Debug, Default, Clone)]
struct LaneStats {
    count: u64,
    total_drift_ns: i128,
    max_drift_ns: i128,
    min_drift_ns: i128,
    deadline_misses: u64,
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

impl DriftTimeline {
    pub fn record(&mut self, lane: EditLane, drift_ns: i128) {
        self.samples.push(DriftPoint {
            index: self.next_index,
            lane,
            drift_ns,
        });
        self.next_index = self.next_index.saturating_add(1);
    }

    pub fn write_svg(&self, path: Option<&str>) -> std::io::Result<()> {
        let path = path.unwrap_or(DEFAULT_DRIFT_SVG_PATH);
        let backend = SVGBackend::new(path, (1280, 720));
        let root = backend.into_drawing_area();
        root.fill(&RGBColor(11, 16, 32))
            .map_err(|e| std::io::Error::other(format!("{e:?}")))?;

        let mut points: Vec<&DriftPoint> = self.samples.iter().collect();
        points.sort_by_key(|point| point.index);

        let title_style = ("Arial", 30).into_font().color(&WHITE);
        let subtitle_style = ("Arial", 14).into_font().color(&RGBColor(148, 163, 184));
        root.draw(&Text::new(
            "Scheduling Drift Over Time",
            (48, 40),
            title_style,
        ))
        .map_err(|e| std::io::Error::other(format!("{e:?}")))?;
        root.draw(&Text::new(
            "Positive values mean the packet exceeded the 2 ms budget; negative values finished early.",
            (48, 66),
            subtitle_style,
        ))
        .map_err(|e| std::io::Error::other(format!("{e:?}")))?;

        if points.is_empty() {
            root.present()
                .map_err(|e| std::io::Error::other(format!("{e:?}")))?;
            return Ok(());
        }

        let mut human_series = Vec::<(u64, f64)>::new();
        let mut bot_series = Vec::<(u64, f64)>::new();
        let mut min_y = f64::INFINITY;
        let mut max_y = f64::NEG_INFINITY;

        for point in &points {
            let drift_ms = point.drift_ns as f64 / 1_000_000.0;
            min_y = min_y.min(drift_ms);
            max_y = max_y.max(drift_ms);
            match point.lane {
                EditLane::Human => human_series.push((point.index, drift_ms)),
                EditLane::Bot => bot_series.push((point.index, drift_ms)),
            }
        }

        min_y = min_y.min(0.0);
        max_y = max_y.max(0.0);
        if (max_y - min_y).abs() < 0.001 {
            min_y -= 1.0;
            max_y += 1.0;
        } else {
            let pad = ((max_y - min_y) * 0.10).max(0.25);
            min_y -= pad;
            max_y += pad;
        }

        let x_max = points.last().map(|p| p.index).unwrap_or(0);
        let mut chart = ChartBuilder::on(&root)
            .margin(24)
            .x_label_area_size(50)
            .y_label_area_size(70)
            .build_cartesian_2d(0u64..x_max.saturating_add(1), min_y..max_y)
            .map_err(|e| std::io::Error::other(format!("{e:?}")))?;

        chart
            .configure_mesh()
            .x_desc("Packet index over time")
            .y_desc("Scheduling drift (ms) = actual processing time - 2 ms")
            .axis_style(WHITE.mix(0.65))
            .label_style(("Arial", 12).into_font().color(&RGBColor(148, 163, 184)))
            .light_line_style(RGBColor(36, 48, 65))
            .bold_line_style(RGBColor(36, 48, 65))
            .draw()
            .map_err(|e| std::io::Error::other(format!("{e:?}")))?;

        chart
            .draw_series(LineSeries::new(
                human_series.iter().copied(),
                RGBColor(34, 197, 94),
            ))
            .map_err(|e| std::io::Error::other(format!("{e:?}")))?
            .label(format!("Human ({})", human_series.len()))
            .legend(|(x, y)| {
                PathElement::new(vec![(x, y), (x + 20, y)], RGBColor(34, 197, 94))
            });

        chart
            .draw_series(DashedLineSeries::new(
                bot_series.iter().copied(),
                10,
                8,
                RGBColor(96, 165, 250).into(),
            ))
            .map_err(|e| std::io::Error::other(format!("{e:?}")))?
            .label(format!("Bot ({})", bot_series.len()))
            .legend(|(x, y)| {
                PathElement::new(
                    vec![(x, y), (x + 20, y)],
                    ShapeStyle::from(&RGBColor(96, 165, 250)).stroke_width(2),
                )
            });

        chart
            .draw_series(std::iter::once(PathElement::new(
                vec![(0, 0.0), (x_max.saturating_add(1), 0.0)],
                RGBColor(245, 158, 11).stroke_width(2),
            )))
            .map_err(|e| std::io::Error::other(format!("{e:?}")))?;

        chart
            .configure_series_labels()
            .background_style(RGBColor(17, 24, 39).mix(0.9))
            .border_style(RGBColor(36, 48, 65))
            .label_font(("Arial", 12).into_font().color(&WHITE))
            .draw()
            .map_err(|e| std::io::Error::other(format!("{e:?}")))?;

        root.present()
            .map_err(|e| std::io::Error::other(format!("{e:?}")))?;
        Ok(())
    }
}

impl LaneDriftStats {
    pub fn record(
        &mut self,
        lane: EditLane,
        scheduling_drift_ns: i128,
        processing_time: Duration,
        deadline: Duration,
    ) {
        let stats = match lane {
            EditLane::Human => &mut self.human,
            EditLane::Bot => &mut self.bot,
        };

        stats.count += 1;
        stats.total_drift_ns += scheduling_drift_ns;
        stats.max_drift_ns = stats.max_drift_ns.max(scheduling_drift_ns);
        if stats.count == 1 || scheduling_drift_ns < stats.min_drift_ns {
            stats.min_drift_ns = scheduling_drift_ns;
        }
        if processing_time > deadline {
            stats.deadline_misses += 1;
        }
    }

    pub fn write_html(&self, path: Option<&str>) -> std::io::Result<()> {
        let path = path.unwrap_or(DEFAULT_LANE_DRIFT_HTML_PATH);

        let rows = [
            ("Human", &self.human, "#22c55e"),
            ("Bot", &self.bot, "#60a5fa"),
        ]
        .into_iter()
        .map(|(label, stats, accent)| {
            let avg_drift_ns = if stats.count == 0 {
                0.0
            } else {
                stats.total_drift_ns as f64 / stats.count as f64
            };
            format!(
                "<tr><td>{}</td><td>{}</td><td>{:.0}</td><td>{}</td><td>{}</td><td style=\"color:{}\">{}</td></tr>",
                label,
                stats.count,
                avg_drift_ns,
                stats.max_drift_ns,
                stats.min_drift_ns,
                accent,
                stats.deadline_misses
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
  <title>Lane Drift Summary</title>
  <style>
    :root {{
      color-scheme: dark;
      --bg: #0b1020;
      --panel: #111827;
      --line: #243041;
      --text: #e5e7eb;
      --muted: #94a3b8;
    }}
    body {{
      margin: 0;
      font-family: Arial, sans-serif;
      background: radial-gradient(circle at top, #172033 0, var(--bg) 50%);
      color: var(--text);
      padding: 32px;
    }}
    .card {{
      max-width: 900px;
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
  </style>
</head>
<body>
  <div class="card">
    <h1>Human vs Bot Scheduling Drift</h1>
    <p>Human edits should show lower drift because the worker prioritizes them over bot edits.</p>
    <table>
      <thead>
        <tr>
          <th>Lane</th>
          <th>Packets</th>
          <th>Avg Drift (ns)</th>
          <th>Max Drift (ns)</th>
          <th>Min Drift (ns)</th>
          <th>Deadline Misses</th>
        </tr>
      </thead>
      <tbody>
        {rows}
      </tbody>
    </table>
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

