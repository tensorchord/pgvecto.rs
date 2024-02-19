use rustix::fd::{AsFd, OwnedFd};
use rustix::mm::{MapFlags, ProtFlags};

#[cfg(target_os = "linux")]
pub fn memfd_create() -> std::io::Result<OwnedFd> {
    if detect::linux::detect_memfd() {
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

#[cfg(target_os = "freebsd")]
pub fn memfd_create() -> std::io::Result<OwnedFd> {
    use rustix::fs::Mode;
    use rustix::fs::OFlags;
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

#[cfg(target_os = "freebsd")]
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
