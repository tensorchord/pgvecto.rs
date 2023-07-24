mod datatype;
mod gucs;
mod hooks;
mod index;

pub use gucs::K;
pub use gucs::OPENAI_API_KEY_GUC;
pub use gucs::PORT;

pub unsafe fn init() {
    self::gucs::init();
    self::hooks::init();
    self::index::init();
}
