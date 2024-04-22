use crate::protocol::pipe_msg;
use log::{set_boxed_logger, set_max_level, Level, LevelFilter, Metadata, Record};

pub struct VectorLogger;

impl log::Log for VectorLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= Level::Info
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            pipe_msg(&record.args().to_string());
        }
    }

    fn flush(&self) {}
}

impl VectorLogger {
    pub fn init() {
        let log_level = LevelFilter::Info;
        set_boxed_logger(Box::new(VectorLogger)).unwrap();
        set_max_level(log_level);
    }
}
