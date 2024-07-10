mod postgres;

use log::{LevelFilter, Metadata, Record};

#[derive(Debug)]
pub struct Logger {
    level: LevelFilter,
}

impl log::Log for Logger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= self.level
    }

    fn log(&self, record: &Record) {
        postgres::send_message_to_server_log(record);
    }

    fn flush(&self) {}
}

impl Logger {
    pub fn new(level: LevelFilter) -> Self {
        Logger { level }
    }

    pub fn init(self) -> Result<(), log::SetLoggerError> {
        log::set_max_level(self.level);
        log::set_boxed_logger(Box::new(self))?;
        Ok(())
    }
}
