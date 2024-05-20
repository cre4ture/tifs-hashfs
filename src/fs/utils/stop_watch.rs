use std::{fmt, ops::{Deref, DerefMut}, time::{Duration, SystemTime}};


pub struct StopWatch {
    start: SystemTime,
    last_sync: SystemTime,
    name: &'static str,
    laps: Vec<(&'static str, Duration)>,
}

impl StopWatch {
    pub fn start(name: &'static str) -> Self {
        let now = SystemTime::now();
        Self {
            start: now,
            last_sync: now,
            name,
            laps: Vec::new(),
        }
    }

    pub fn since_start(&self) -> Duration {
        self.start.elapsed().unwrap()
    }

    pub fn sync(&mut self, name: &'static str) {
        let now = SystemTime::now();
        let lap_time = now.duration_since(self.last_sync).unwrap();
        self.last_sync = now;
        self.laps.push((name, lap_time));
    }
}

impl fmt::Display for StopWatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let total = self.since_start();
        write!(f, "{}(ms){}", self.name, total.as_millis())?;
        for (l_name, l_time) in &self.laps {
            write!(f, "/{}{}", l_name, l_time.as_millis())?;
        }
        Ok(())
    }
}

pub struct AutoStopWatch {
    watch: StopWatch,
}

impl AutoStopWatch {
    pub fn start(name: &'static str) -> Self {
        Self {
            watch: StopWatch::start(name),
        }
    }
}

impl Drop for AutoStopWatch {
    fn drop(&mut self) {
        eprintln!("AutoStopWatch:{}", self.watch);
    }
}

impl Deref for AutoStopWatch {
    type Target = StopWatch;

    fn deref(&self) -> &Self::Target {
        &self.watch
    }
}

impl DerefMut for AutoStopWatch {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.watch
    }
}
