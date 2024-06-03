use super::message::Message;
use pgrx::pg_sys::Log_destination;

const LOG_DESTINATION_STDERR: u32 = 1;
const LOG_DESTINATION_CSVLOG: u32 = 8;
const LOG_DESTINATION_JSONLOG: u32 = 16;

pub fn pipe_log(msg: &str) {
    let message = Message::new(msg);
    unsafe {
        if Log_destination as u32 & LOG_DESTINATION_CSVLOG != 0 {
            message.csv_chunk()
        }
        if Log_destination as u32 & LOG_DESTINATION_STDERR != 0 {
            message.stderr_chunk()
        }
        if Log_destination as u32 & LOG_DESTINATION_JSONLOG != 0 {
            message.json_chunk()
        }
    }
}
