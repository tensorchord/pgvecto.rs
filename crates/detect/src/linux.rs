use std::sync::atomic::{AtomicBool, Ordering};

static ATOMIC_MEMFD: AtomicBool = AtomicBool::new(false);

pub fn test_memfd() -> bool {
    use rustix::fs::MemfdFlags;
    use std::io::ErrorKind;
    match rustix::fs::memfd_create(".memfd.VECTORS.SUPPORT", MemfdFlags::empty()) {
        Ok(_) => true,
        Err(e) if e.kind() == ErrorKind::Unsupported => false,
        Err(_) => false,
    }
}

pub fn ctor_memfd() {
    ATOMIC_MEMFD.store(test_memfd(), Ordering::Relaxed);
}

pub fn detect_memfd() -> bool {
    ATOMIC_MEMFD.load(Ordering::Relaxed)
}
