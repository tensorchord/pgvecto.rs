pub mod embedding;
pub mod executing;
pub mod internal;
pub mod planning;

pub unsafe fn init() {
    unsafe {
        planning::init();
        internal::init();
        executing::init();
        embedding::init();
        #[cfg(feature = "pg14")]
        pgrx::pg_sys::EmitWarningsOnPlaceholders(c"vectors".as_ptr());
        #[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
        pgrx::pg_sys::MarkGUCPrefixReserved(c"vectors".as_ptr());
    }
}
