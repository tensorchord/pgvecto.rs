#![feature(thread_local)]

use std::os::fd::OwnedFd;

#[cfg(target_os = "linux")]
pub fn memfd_create() -> std::io::Result<OwnedFd> {
    use std::cell::Cell;
    #[thread_local]
    static SUPPORT_MEMFD: Cell<bool> = Cell::new(true);
    if SUPPORT_MEMFD.get() {
        use rustix::fs::MemfdFlags;
        let r = rustix::fs::memfd_create(
            format!(".memfd.MEMFD.{:x}", std::process::id()),
            MemfdFlags::empty(),
        );
        match r {
            Ok(fd) => {
                return Ok(fd);
            }
            Err(e) if e.kind() == std::io::ErrorKind::Unsupported => {
                SUPPORT_MEMFD.set(false);
            }
            Err(e) => {
                return Err(e.into());
            }
        }
    }
    use rustix::fs::Mode;
    use rustix::fs::OFlags;
    // POSIX fcntl locking do not support shmem, so we use a regular file here.
    // reference: https://man7.org/linux/man-pages/man3/fcntl.3p.html
    // However, Linux shmem supports fcntl locking.
    let name = format!(
        ".shm.MEMFD.{:x}.{:x}",
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
pub fn memfd_create() -> std::io::Result<OwnedFd> {
    use rustix::fs::Mode;
    use rustix::fs::OFlags;
    // POSIX fcntl locking do not support shmem, so we use a regular file here.
    // reference: https://man7.org/linux/man-pages/man3/fcntl.3p.html
    let name = format!(
        ".shm.MEMFD.{:x}.{:x}",
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
pub fn memfd_create() -> std::io::Result<OwnedFd> {
    use rustix::fs::Mode;
    use rustix::fs::OFlags;
    // POSIX fcntl locking do not support shmem, so we use a regular file here.
    // reference: https://man7.org/linux/man-pages/man3/fcntl.3p.html
    let name = format!(
        ".shm.MEMFD.{:x}.{:x}",
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
