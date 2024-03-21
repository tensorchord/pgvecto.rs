mod am;
mod am_options;
mod am_scan;
mod catalog;
mod compatibility;
mod functions;
mod hooks;
mod utils;
mod views;

pub unsafe fn init() {
    unsafe {
        self::hooks::init();
        self::am::init();
    }
}
