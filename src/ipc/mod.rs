pub mod client;
mod packet;
pub mod server;
pub mod transport;

use self::client::Rpc;
use self::server::RpcHandler;
use thiserror::Error;

#[derive(Debug, Clone, Error)]
#[rustfmt::skip]
pub enum IpcError {
    #[error("\
pgvecto.rs: IPC connection is closed unexpected.
ADVICE: The error is raisen by background worker errors. \
Please check the full Postgresql log to get more information.\
")]
    Closed,
}

pub fn listen_unix() -> impl Iterator<Item = RpcHandler> {
    std::iter::from_fn(move || {
        let socket = self::transport::Socket::Unix(self::transport::unix::accept());
        Some(self::server::RpcHandler::new(socket))
    })
}

pub fn listen_mmap() -> impl Iterator<Item = RpcHandler> {
    std::iter::from_fn(move || {
        let socket = self::transport::Socket::Mmap(self::transport::mmap::accept());
        Some(self::server::RpcHandler::new(socket))
    })
}

pub fn connect_unix() -> Rpc {
    let socket = self::transport::Socket::Unix(self::transport::unix::connect());
    self::client::Rpc::new(socket)
}

pub fn connect_mmap() -> Rpc {
    let socket = self::transport::Socket::Mmap(self::transport::mmap::connect());
    self::client::Rpc::new(socket)
}
