pub mod client;
mod packet;
pub mod server;
pub mod transport;

use self::client::Client;
use self::server::RpcHandler;
use service::prelude::*;
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

impl FriendlyErrorLike for IpcError {
    fn friendly(self) -> ! {
        panic!("pgvecto.rs: {}", self);
    }
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

pub fn connect_unix() -> Client {
    let socket = self::transport::Socket::Unix(self::transport::unix::connect());
    Client::new(socket)
}

pub fn connect_mmap() -> Client {
    let socket = self::transport::Socket::Mmap(self::transport::mmap::connect());
    Client::new(socket)
}

pub fn init() {
    self::transport::mmap::init();
    self::transport::unix::init();
}
