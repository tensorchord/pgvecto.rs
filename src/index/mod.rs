mod am;
mod am_build;
mod am_scan;
mod am_setup;
mod am_update;
mod compat;
mod functions;
mod hook_executor;
mod hook_transaction;
mod hooks;
mod utils;
mod views;

pub unsafe fn init() {
    unsafe {
        self::hooks::init();
        self::am::init();
    }
}
