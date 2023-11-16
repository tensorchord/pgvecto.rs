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
    Ok(rustix::fs::memfd_create(
        &format!(".memfd.VECTORS.{:x}", std::process::id()),
        MemfdFlags::empty(),
    )?)
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
    // SHM_NAME_MAX = 30
    // 9 + 8 + 8 = 25 < SHM_NAME_MAX
    // reference: https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/shm_open.2.html
    // reference: https://github.com/apple-oss-distributions/xnu/blob/main/bsd/kern/posix_sem.c#L89-L90
    let name = format!(
        "/.shm.V.{:x}.{:x}",
        std::process::id(),
        rand::random::<u32>()
    );
    let fd = rustix::shm::shm_open(
        &name,
        ShmOFlags::RDWR | ShmOFlags::CREATE | ShmOFlags::EXCL,
        Mode::RUSR | Mode::WUSR,
    )?;
    rustix::shm::shm_unlink(&name)?;
    Ok(fd)
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
