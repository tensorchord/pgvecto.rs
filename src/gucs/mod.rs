pub mod executing;
pub mod internal;
pub mod planning;

pub unsafe fn init() {
    unsafe {
        self::planning::init();
        self::internal::init();
        self::executing::init();
    }
}
