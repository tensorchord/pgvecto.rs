use pgrx::pg_sys::GetConfigOption;
use std::ffi::CStr;

pub const PIPE_PROTO_DEST_STDERR: u8 = 0x10;
pub const PIPE_PROTO_DEST_CSVLOG: u8 = 0x20;
pub const PIPE_PROTO_DEST_JSONLOG: u8 = 0x40;

fn c_char_to_string(c_str: *const i8) -> String {
    unsafe {
        if c_str.is_null() {
            return String::new();
        }

        let c_str = CStr::from_ptr(c_str);
        c_str.to_string_lossy().into_owned()
    }
}

fn need(log: &str) -> bool {
    let log_destination = unsafe { GetConfigOption(c"log_destination".as_ptr(), false, false) };
    let log_destination = c_char_to_string(log_destination);
    let mut flag = 0;
    return log_destination.contains(log);
}

pub fn need_stderr() -> bool {
    return { need("stderr") };
}

pub fn need_json() -> bool {
    return { need("jsonlog") };
}

pub fn need_csv() -> bool {
    return { need("csvlog") };
}
