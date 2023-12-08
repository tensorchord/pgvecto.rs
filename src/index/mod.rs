#![allow(unsafe_op_in_unsafe_fn)]

mod am;
mod am_build;
mod am_scan;
mod am_setup;
mod am_update;
mod client;
mod hook_executor;
mod hook_transaction;
mod hooks;
mod utils;
mod views;

pub unsafe fn init() {
    self::hooks::init();
    self::am::init();
}
