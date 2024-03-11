mod am;
mod am_build;
mod am_scan;
mod am_setup;
mod am_update;
mod functions;
mod hook_compat;
mod hook_maintain;
mod hooks;
mod utils;
mod views;

pub unsafe fn init() {
    unsafe {
        self::hooks::init();
        self::am::init();
    }
}
