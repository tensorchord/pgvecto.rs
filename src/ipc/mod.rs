pub mod client;
mod packet;
pub mod server;
mod transport;

use self::client::Rpc;
use self::server::RpcHandler;
use thiserror::Error;

#[derive(Debug, Clone, Error)]
pub enum IpcError {
    #[error("The connection is closed.")]
    Closed,
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
