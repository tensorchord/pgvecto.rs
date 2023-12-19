use rustix::fd::{AsFd, OwnedFd};
use rustix::mm::{MapFlags, ProtFlags};
use std::io::ErrorKind;
use std::sync::atomic::AtomicU32;
use std::sync::LazyLock;

#[cfg(target_os = "linux")]
static SUPPORT_MEMFD: LazyLock<bool> = LazyLock::new(|| {
    use rustix::fs::MemfdFlags;
    match rustix::fs::memfd_create(".memfd.VECTORS.SUPPORT", MemfdFlags::empty()) {
        Ok(_) => true,
        Err(e) if e.kind() == ErrorKind::Unsupported => false,
        Err(_) => false,
    }
});

#[cfg(target_os = "linux")]
pub unsafe fn futex_wait(futex: &AtomicU32, value: u32) {
    const FUTEX_TIMEOUT: libc::timespec = libc::timespec {
        tv_sec: 15,
        tv_nsec: 0,
    };
    unsafe {
        libc::syscall(
            libc::SYS_futex,
            futex.as_ptr(),
            libc::FUTEX_WAIT,
            value,
            &FUTEX_TIMEOUT,
        );
    }
}

#[cfg(target_os = "linux")]
pub unsafe fn futex_wake(futex: &AtomicU32) {
    unsafe {
        libc::syscall(libc::SYS_futex, futex.as_ptr(), libc::FUTEX_WAKE, i32::MAX);
    }
}

#[cfg(target_os = "linux")]
pub fn memfd_create() -> std::io::Result<OwnedFd> {
    if *SUPPORT_MEMFD {
        use rustix::fs::MemfdFlags;
        Ok(rustix::fs::memfd_create(
            format!(".memfd.VECTORS.{:x}", std::process::id()),
            MemfdFlags::empty(),
        )?)
    } else {
        use rustix::fs::Mode;
        use rustix::fs::OFlags;
        // POSIX fcntl locking do not support shmem, so we use a regular file here.
        // reference: https://man7.org/linux/man-pages/man3/fcntl.3p.html
        let name = format!(
            ".shm.VECTORS.{:x}.{:x}",
            std::process::id(),
            rand::random::<u32>()
        );
        let fd = rustix::fs::open(
            &name,
            OFlags::RDWR | OFlags::CREATE | OFlags::EXCL,
            Mode::RUSR | Mode::WUSR,
        )?;
        rustix::fs::unlink(&name)?;
        Ok(fd)
    }
}

#[cfg(target_os = "linux")]
pub unsafe fn mmap_populate(len: usize, fd: impl AsFd) -> std::io::Result<*mut libc::c_void> {
    use std::ptr::null_mut;
    unsafe {
        Ok(rustix::mm::mmap(
            null_mut(),
            len,
            ProtFlags::READ | ProtFlags::WRITE,
            MapFlags::SHARED | MapFlags::POPULATE,
            fd,
            0,
        )?)
    }
}

#[cfg(target_os = "macos")]
pub unsafe fn futex_wait(futex: &AtomicU32, value: u32) {
    const ULOCK_TIMEOUT: u32 = 15_000_000;
    unsafe {
        ulock_sys::__ulock_wait(
            ulock_sys::darwin19::UL_COMPARE_AND_WAIT_SHARED,
            futex.as_ptr().cast(),
            value as _,
            ULOCK_TIMEOUT,
        );
    }
}

#[cfg(target_os = "macos")]
pub unsafe fn futex_wake(futex: &AtomicU32) {
    unsafe {
        ulock_sys::__ulock_wake(
            ulock_sys::darwin19::UL_COMPARE_AND_WAIT_SHARED,
            futex.as_ptr().cast(),
            0,
        );
    }
}

#[cfg(target_os = "macos")]
pub fn memfd_create() -> std::io::Result<OwnedFd> {
    use rustix::fs::Mode;
    use rustix::fs::OFlags;
    // POSIX fcntl locking do not support shmem, so we use a regular file here.
    // reference: https://man7.org/linux/man-pages/man3/fcntl.3p.html
    let name = format!(
        ".shm.VECTORS.{:x}.{:x}",
        std::process::id(),
        rand::random::<u32>()
    );
    let fd = rustix::fs::open(
        &name,
        OFlags::RDWR | OFlags::CREATE | OFlags::EXCL,
        Mode::RUSR | Mode::WUSR,
    )?;
    rustix::fs::unlink(&name)?;
    Ok(fd)
}

#[cfg(target_os = "macos")]
pub unsafe fn mmap_populate(len: usize, fd: impl AsFd) -> std::io::Result<*mut libc::c_void> {
    use std::ptr::null_mut;
    unsafe {
        Ok(rustix::mm::mmap(
            null_mut(),
            len,
            ProtFlags::READ | ProtFlags::WRITE,
            MapFlags::SHARED,
            fd,
            0,
        )?)
    }
}
