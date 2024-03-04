use super::ConnectionError;
use rustix::fd::{AsFd, OwnedFd};
use rustix::fs::FlockOperation;
use send_fd::SendFd;
use std::cell::UnsafeCell;
use std::io::ErrorKind;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::OnceLock;
use std::time::Duration;

const BUFFER_SIZE: usize = 1024 * 1024;
const SPIN_LIMIT: usize = 8;
const TIMEOUT: Duration = Duration::from_secs(15);

static CHANNEL: OnceLock<SendFd> = OnceLock::new();

pub fn init() {
    CHANNEL.set(SendFd::new().unwrap()).ok().unwrap();
}

pub fn accept() -> Socket {
    let memfd = CHANNEL.get().unwrap().recv().unwrap();
    rustix::fs::fcntl_lock(&memfd, FlockOperation::NonBlockingLockShared).unwrap();
    let memmap = unsafe {
        memmap2::MmapOptions::new()
            .len(BUFFER_SIZE)
            .populate()
            .map_mut(&memfd)
            .unwrap()
    };
    Socket {
        is_server: true,
        addr: memmap.as_ptr().cast(),
        memfd,
        _memmap: memmap,
    }
}

pub fn connect() -> Socket {
    let memfd = memfd::memfd_create().unwrap();
    rustix::fs::ftruncate(&memfd, BUFFER_SIZE as u64).unwrap();
    rustix::fs::fcntl_lock(&memfd, FlockOperation::NonBlockingLockShared).unwrap();
    CHANNEL.get().unwrap().send(memfd.as_fd()).unwrap();
    let memmap = unsafe {
        memmap2::MmapOptions::new()
            .len(BUFFER_SIZE)
            .populate()
            .map_mut(&memfd)
            .unwrap()
    };
    Socket {
        is_server: false,
        addr: memmap.as_ptr().cast(),
        memfd,
        _memmap: memmap,
    }
}

pub struct Socket {
    is_server: bool,
    addr: *const Channel,
    memfd: OwnedFd,
    _memmap: memmap2::MmapMut,
}

unsafe impl Send for Socket {}
unsafe impl Sync for Socket {}

impl Drop for Socket {
    fn drop(&mut self) {
        rustix::fs::fcntl_lock(&self.memfd, FlockOperation::Unlock).unwrap();
    }
}

impl Socket {
    pub fn test(&self) -> bool {
        match rustix::fs::fcntl_lock(&self.memfd, FlockOperation::NonBlockingLockExclusive) {
            Ok(()) => false,
            Err(e) if e.kind() == ErrorKind::WouldBlock => true,
            Err(e) => panic!("{:?}", e),
        }
    }
    pub fn send(&mut self, packet: &[u8]) -> Result<(), ConnectionError> {
        if packet.len() > BUFFER_SIZE - 8 {
            return Err(ConnectionError::PacketTooLarge);
        }
        unsafe {
            if self.is_server {
                (*self.addr).server_send(packet);
            } else {
                (*self.addr).client_send(packet);
            }
        }
        Ok(())
    }
    pub fn recv(&mut self) -> Result<Vec<u8>, ConnectionError> {
        let packet = unsafe {
            if self.is_server {
                (*self.addr).server_recv(|| self.test())?
            } else {
                (*self.addr).client_recv(|| self.test())?
            }
        };
        Ok(packet)
    }
}

#[repr(C, align(128))]
struct Channel {
    bytes: UnsafeCell<[u8; BUFFER_SIZE - 8]>,
    len: UnsafeCell<u32>,
    /// 0: locked by client, nobody is waiting
    /// 1: locked by server, nobody is waiting
    /// 2: locked by client, server is waiting
    /// 3: locked by server, client is waiting
    futex: AtomicU32,
}

const _: () = assert!(std::mem::size_of::<Channel>() == BUFFER_SIZE);

impl Channel {
    unsafe fn client_recv(&self, test: impl Fn() -> bool) -> Result<Vec<u8>, ConnectionError> {
        const S: u32 = 0;
        const T: u32 = 1;
        const X: u32 = 2;
        const Y: u32 = 3;
        let mut backoff = 0usize;
        loop {
            match self.futex.load(Ordering::Acquire) {
                S | X => break,
                T if backoff <= SPIN_LIMIT => {
                    for _ in 0..1usize << backoff {
                        std::hint::spin_loop();
                    }
                    backoff += 1;
                }
                T => {
                    if self
                        .futex
                        .compare_exchange(T, Y, Ordering::Relaxed, Ordering::Acquire)
                        .is_err()
                    {
                        break;
                    }
                    interprocess_atomic_wait::wait(&self.futex, Y, TIMEOUT);
                }
                Y => {
                    if !test() {
                        return Err(ConnectionError::ClosedConnection);
                    }
                    interprocess_atomic_wait::wait(&self.futex, Y, TIMEOUT);
                }
                _ => unsafe { std::hint::unreachable_unchecked() },
            }
        }
        unsafe {
            let len = *self.len.get();
            let res = (*self.bytes.get())[0..len as usize].to_vec();
            Ok(res)
        }
    }
    unsafe fn client_send(&self, data: &[u8]) {
        const S: u32 = 0;
        const T: u32 = 1;
        const X: u32 = 2;
        debug_assert!(matches!(self.futex.load(Ordering::Relaxed), S | X));
        unsafe {
            *self.len.get() = data.len() as u32;
            (*self.bytes.get())[0..data.len()].copy_from_slice(data);
        }
        if X == self.futex.swap(T, Ordering::Release) {
            interprocess_atomic_wait::wake(&self.futex);
        }
    }
    unsafe fn server_recv(&self, test: impl Fn() -> bool) -> Result<Vec<u8>, ConnectionError> {
        const S: u32 = 1;
        const T: u32 = 0;
        const X: u32 = 3;
        const Y: u32 = 2;
        let mut backoff = 0usize;
        loop {
            match self.futex.load(Ordering::Acquire) {
                S | X => break,
                T if backoff <= SPIN_LIMIT => {
                    for _ in 0..1usize << backoff {
                        std::hint::spin_loop();
                    }
                    backoff += 1;
                }
                T => {
                    if self
                        .futex
                        .compare_exchange(T, Y, Ordering::Relaxed, Ordering::Acquire)
                        .is_err()
                    {
                        break;
                    }
                    interprocess_atomic_wait::wait(&self.futex, Y, TIMEOUT);
                }
                Y => {
                    if !test() {
                        return Err(ConnectionError::ClosedConnection);
                    }
                    interprocess_atomic_wait::wait(&self.futex, Y, TIMEOUT);
                }
                _ => unsafe { std::hint::unreachable_unchecked() },
            }
        }
        unsafe {
            let len = *self.len.get();
            let res = (*self.bytes.get())[0..len as usize].to_vec();
            Ok(res)
        }
    }
    unsafe fn server_send(&self, data: &[u8]) {
        const S: u32 = 1;
        const T: u32 = 0;
        const X: u32 = 3;
        debug_assert!(matches!(self.futex.load(Ordering::Relaxed), S | X));
        unsafe {
            *self.len.get() = data.len() as u32;
            (*self.bytes.get())[0..data.len()].copy_from_slice(data);
        }
        if X == self.futex.swap(T, Ordering::Release) {
            interprocess_atomic_wait::wake(&self.futex);
        }
    }
}
