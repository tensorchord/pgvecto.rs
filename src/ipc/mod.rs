pub mod client;
mod packet;
pub mod server;
mod transport;

use self::client::Rpc;
use self::server::RpcHandler;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Error, Serialize, Deserialize)]
pub enum ServerIpcError {
    #[error("The connection is closed.")]
    Closed,
    #[error("Server encounters an error.")]
    Server,
}

#[derive(Debug, Error, Serialize, Deserialize)]
pub enum ClientIpcError {
    #[error("The connection is closed.")]
    Closed,
    #[error("Server encounters an error.")]
    Server,
}

pub fn listen() -> impl Iterator<Item = RpcHandler> {
    let mut listener = self::transport::Listener::new();
    std::iter::from_fn(move || {
        let socket = listener.accept();
        Some(self::server::RpcHandler::new(socket))
    })
}

pub fn connect() -> Rpc {
    let socket = self::transport::Socket::new();
    self::client::Rpc::new(socket)
}
