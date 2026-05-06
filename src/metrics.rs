use crate::logging::log_line;
use std::time::Duration;

#[derive(Debug, Default, Clone)]
pub struct DriftStats {
    processed: u64,
    total_scheduling_drift: Duration,
    max_scheduling_drift: Duration,
    total_processing_time: Duration,
    max_processing_time: Duration,
    deadline_misses: u64,
}

impl DriftStats {
    pub fn record(
        &mut self,
        scheduling_drift: Duration,
        processing_time: Duration,
        deadline: Duration,
    ) {
        self.processed += 1;
        self.total_scheduling_drift += scheduling_drift;
        self.total_processing_time += processing_time;
        self.max_scheduling_drift = self.max_scheduling_drift.max(scheduling_drift);
        self.max_processing_time = self.max_processing_time.max(processing_time);
        if processing_time > deadline {
            self.deadline_misses += 1;
        }
    }

    pub fn processed(&self) -> u64 {
        self.processed
    }

    pub fn report(&self) {
        if self.processed == 0 {
            return;
        }

        let count = self.processed as f64;
        let avg_scheduling_drift =
            Duration::from_secs_f64(self.total_scheduling_drift.as_secs_f64() / count);
        let avg_processing_time =
            Duration::from_secs_f64(self.total_processing_time.as_secs_f64() / count);

        log_line(format!(
            "DRIFT REPORT count={} avg_scheduling_drift={:?} max_scheduling_drift={:?} avg_processing_time={:?} max_processing_time={:?} deadline_misses={}",
            self.processed,
            avg_scheduling_drift,
            self.max_scheduling_drift,
            avg_processing_time,
            self.max_processing_time,
            self.deadline_misses
        ));
    }
}
