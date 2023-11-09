mod casts;
pub mod datatype;
mod functions;
pub mod gucs;
mod hook_custom_scan;
mod hook_executor;
mod hook_transaction;
mod hooks;
mod index;
mod index_build;
mod index_scan;
mod index_setup;
mod index_update;
mod operators;

pub unsafe fn init() {
    self::gucs::init();
    self::hooks::init();
    self::index::init();
}
