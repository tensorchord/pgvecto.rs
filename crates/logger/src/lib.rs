use std::ffi::CStr;
use pgrx::pg_sys::{GetConfigOption, LOG_DESTINATION_STDERR, LOG_DESTINATION_JSONLOG, LOG_DESTINATION_CSVLOG};

fn c_char_to_string(c_str: *const i8) -> String {
    unsafe {
        if c_str.is_null() {
            return String::new();
        }

        let c_str = CStr::from_ptr(c_str);
        c_str.to_string_lossy().into_owned()
    }
}

struct LogMap {
    flag: u32,
    name: &'static str,
}

pub fn get_log_type() -> [LogMap; 3] {
    return [
        LogMap { name: "stderr", flag: LOG_DESTINATION_STDERR },
        LogMap { name: "jsonlog", flag: LOG_DESTINATION_JSONLOG },
        LogMap { name: "csvlog", flag: LOG_DESTINATION_CSVLOG }
    ];
}

pub unsafe fn get_log_destination_flag() -> u32 {
    let log_destination = unsafe { GetConfigOption(c"log_destination".as_ptr(), false, false) };
    let log_destination = c_char_to_string(log_destination);
    let mut flag = 0;
    for log_item in get_log_type() {
        if log_destination.contains(log_item.name) {
            flag |= log_item.flag
        }
    }
    flag
}