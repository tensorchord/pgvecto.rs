use rustix::fd::BorrowedFd;
use rustix::fd::{AsFd, OwnedFd};
use rustix::net::{RecvAncillaryBuffer, RecvAncillaryMessage, RecvFlags};
use rustix::net::{SendAncillaryBuffer, SendAncillaryMessage, SendFlags};
use std::io::{IoSlice, IoSliceMut};
use std::os::unix::net::UnixStream;

#[repr(C)]
pub struct FileSocket {
    tx: OwnedFd,
    rx: OwnedFd,
}

impl FileSocket {
    pub fn new() -> std::io::Result<Self> {
        let (tx, rx) = UnixStream::pair()?;
        Ok(Self {
            tx: tx.into(),
            rx: rx.into(),
        })
    }
    pub fn recv(&self) -> std::io::Result<OwnedFd> {
        let rx = self.rx.as_fd();
        recv_fd(rx)
    }
    pub fn send(&self, fd: BorrowedFd<'_>) -> std::io::Result<()> {
        let tx = self.tx.as_fd();
        send_fd(tx, fd)?;
        Ok(())
    }
}

fn send_fd(tx: BorrowedFd<'_>, fd: BorrowedFd<'_>) -> std::io::Result<()> {
    let fds = [fd];
    let mut buffer = AncillaryBuffer([0u8; rustix::cmsg_space!(ScmRights(1))]);
    let mut control = SendAncillaryBuffer::new(&mut buffer.0);
    let pushed = control.push(SendAncillaryMessage::ScmRights(&fds));
    assert!(pushed);
    let ios = IoSlice::new(&[b'$']);
    rustix::net::sendmsg(tx, &[ios], &mut control, SendFlags::empty())?;
    Ok(())
}

fn recv_fd(rx: BorrowedFd<'_>) -> std::io::Result<OwnedFd> {
    loop {
        let mut buffer = AncillaryBuffer([0u8; rustix::cmsg_space!(ScmRights(1))]);
        let mut control = RecvAncillaryBuffer::new(&mut buffer.0);
        let mut buffer_ios = [b'.'];
        let ios = IoSliceMut::new(&mut buffer_ios);
        let returned = rustix::net::recvmsg(rx, &mut [ios], &mut control, RecvFlags::CMSG_CLOEXEC)?;
        if returned.flags.bits() & libc::MSG_CTRUNC as u32 != 0 {
            log::warn!("Ancillary is truncated.");
        }
        // it's impossible for a graceful shutdown since we opened the other end
        assert_eq!(returned.bytes, 1);
        assert_eq!(buffer_ios[0], b'$');
        let mut fds = vec![];
        for message in control.drain() {
            match message {
                RecvAncillaryMessage::ScmRights(iter) => {
                    fds.extend(iter);
                }
                _ => {
                    // impossible to receive other than one file descriptor since we do not send
                    unreachable!()
                }
            }
        }
        // it's impossible for more than one file descriptor since the buffer can only contain one
        assert!(fds.len() <= 1);
        if let Some(fd) = fds.pop() {
            return Ok(fd);
        }
        log::warn!("Ancillary is expected.");
    }
}

#[repr(C, align(32))]
struct AncillaryBuffer([u8; rustix::cmsg_space!(ScmRights(1))]);
