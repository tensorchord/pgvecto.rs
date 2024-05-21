mod config;
mod message;
mod protocol;

use crate::protocol::pipe_log;
use log::{set_boxed_logger, set_max_level, LevelFilter, Metadata, Record};

#[derive(Debug, Clone)]
pub struct VectorLogger {
    level: LevelFilter,
}

impl log::Log for VectorLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= self.level
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            pipe_log(&record.args().to_string());
        }
    }

    fn flush(&self) {}
}

impl VectorLogger {
    pub fn init(&self) {
        set_boxed_logger(Box::new(self.clone())).unwrap();
        set_max_level(self.level);
    }

    pub fn build() -> Self {
        VectorLogger {
            level: LevelFilter::Info,
        }
    }

    pub fn filter_level(&mut self, level: LevelFilter) {
        self.level = level;
    }
}
