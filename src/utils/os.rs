use rustix::fd::{AsFd, OwnedFd};
use rustix::mm::{MapFlags, ProtFlags};
use std::sync::atomic::AtomicU32;

#[cfg(target_os = "linux")]
pub unsafe fn futex_wait(futex: &AtomicU32, value: u32) {
    const FUTEX_TIMEOUT: libc::timespec = libc::timespec {
        tv_sec: 15,
        tv_nsec: 0,
    };
    libc::syscall(
        libc::SYS_futex,
        futex.as_ptr(),
        libc::FUTEX_WAIT,
        value,
        &FUTEX_TIMEOUT,
    );
}

#[cfg(target_os = "linux")]
pub unsafe fn futex_wake(futex: &AtomicU32) {
    libc::syscall(libc::SYS_futex, futex.as_ptr(), libc::FUTEX_WAKE, i32::MAX);
}

#[cfg(target_os = "linux")]
pub fn memfd_create() -> std::io::Result<OwnedFd> {
    use rustix::fs::MemfdFlags;
    Ok(rustix::fs::memfd_create("transport", MemfdFlags::empty())?)
}

#[cfg(target_os = "linux")]
pub unsafe fn mmap_populate(len: usize, fd: impl AsFd) -> std::io::Result<*mut libc::c_void> {
    use std::ptr::null_mut;
    Ok(rustix::mm::mmap(
        null_mut(),
        len,
        ProtFlags::READ | ProtFlags::WRITE,
        MapFlags::SHARED | MapFlags::POPULATE,
        fd,
        0,
    )?)
}

#[cfg(target_os = "macos")]
pub unsafe fn futex_wait(futex: &AtomicU32, value: u32) {
    const ULOCK_TIMEOUT: u32 = 15_000_000;
    ulock_sys::__ulock_wait(
        ulock_sys::darwin19::UL_COMPARE_AND_WAIT_SHARED,
        futex.as_ptr().cast(),
        value as _,
        ULOCK_TIMEOUT,
    );
}

#[cfg(target_os = "macos")]
pub unsafe fn futex_wake(futex: &AtomicU32) {
    ulock_sys::__ulock_wake(
        ulock_sys::darwin19::UL_COMPARE_AND_WAIT_SHARED,
        futex.as_ptr().cast(),
        0,
    );
}

#[cfg(target_os = "macos")]
pub fn memfd_create() -> std::io::Result<OwnedFd> {
    use rustix::fs::Mode;
    use rustix::shm::ShmOFlags;
    Ok(rustix::shm::shm_open(
        &format!("./.s.VECTORS.{}", std::process::id()),
        ShmOFlags::RDWR | ShmOFlags::CREATE | ShmOFlags::EXCL,
        Mode::RUSR | Mode::WUSR,
    )?)
}

#[cfg(target_os = "macos")]
pub unsafe fn mmap_populate(len: usize, fd: impl AsFd) -> std::io::Result<*mut libc::c_void> {
    use std::ptr::null_mut;
    Ok(rustix::mm::mmap(
        null_mut(),
        len,
        ProtFlags::READ | ProtFlags::WRITE,
        MapFlags::SHARED,
        fd,
        0,
    )?)
}
