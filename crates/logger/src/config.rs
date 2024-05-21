use pgrx::pg_sys::GetConfigOption;
use std::ffi::CStr;

pub const PIPE_PROTO_DEST_STDERR: u8 = 0x10;
pub const PIPE_PROTO_DEST_CSVLOG: u8 = 0x20;
pub const PIPE_PROTO_DEST_JSONLOG: u8 = 0x40;
pub const LAST_CHUNK: u8 = 0x01;
pub const LOG_ERROR: &str = "stderr";
pub const LOG_CSV: &str = "csvlog";
pub const LOG_JSON: &str = "jsonlog";

fn c_char_to_string(c_str: *const i8) -> String {
    unsafe {
        if c_str.is_null() {
            return String::new();
        }

        let c_str = CStr::from_ptr(c_str);
        c_str.to_string_lossy().into_owned()
    }
}

pub fn need(log_type: &str) -> bool {
    let log_destination = unsafe { GetConfigOption(c"log_destination".as_ptr(), false, false) };
    let log_destination = c_char_to_string(log_destination);
    log_destination.contains(log_type)
}
