pub mod client;
mod packet;
pub mod server;
pub mod transport;

use self::server::RpcHandler;
use service::prelude::*;
use thiserror::Error;

#[derive(Debug, Clone, Error)]
pub enum IpcError {
    #[error("IPC connection is closed unexpected.")]
    Closed,
}

impl FriendlyErrorLike for IpcError {
    fn convert(self) -> FriendlyError {
        FriendlyError::Ipc
    }
}

pub fn listen_unix() -> impl Iterator<Item = RpcHandler> {
    std::iter::from_fn(move || {
        let socket = self::transport::ServerSocket::Unix(self::transport::unix::accept());
        Some(self::server::RpcHandler::new(socket))
    })
}

pub fn listen_mmap() -> impl Iterator<Item = RpcHandler> {
    std::iter::from_fn(move || {
        let socket = self::transport::ServerSocket::Mmap(self::transport::mmap::accept());
        Some(self::server::RpcHandler::new(socket))
    })
}

pub fn connect_unix() -> self::transport::ClientSocket {
    self::transport::ClientSocket::Unix {
        ok: true,
        socket: self::transport::unix::connect(),
    }
}

pub fn connect_mmap() -> self::transport::ClientSocket {
    self::transport::ClientSocket::Mmap {
        ok: true,
        socket: self::transport::mmap::connect(),
    }
}

pub fn init() {
    self::transport::mmap::init();
    self::transport::unix::init();
}
