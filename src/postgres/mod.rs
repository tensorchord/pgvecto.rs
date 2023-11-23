mod casts;
pub mod datatype;
pub mod gucs;
mod hook_executor;
mod hook_transaction;
mod hooks;
mod index;
mod index_build;
mod index_scan;
mod index_setup;
mod index_update;
mod operators;
mod stat;

pub unsafe fn init() {
    self::gucs::init();
    self::hooks::init();
    self::index::init();
}
