mod channel;
pub mod client;
#[cfg(feature = "ipc-use-mmap")]
mod mmap;
mod packet;
pub mod server;
#[cfg(not(feature = "ipc-use-mmap"))]
mod socket;

pub use self::channel::*;
use self::client::Rpc;
use self::server::RpcHandler;

#[cfg(not(feature = "ipc-use-mmap"))]
pub fn listen() -> impl Iterator<Item = RpcHandler> {
    let mut listener = self::socket::Listener::new();
    std::iter::from_fn(move || {
        let socket = Box::new(listener.accept());
        Some(self::server::RpcHandler::new(socket))
    })
}

#[cfg(not(feature = "ipc-use-mmap"))]
pub fn connect() -> Rpc {
    let socket = Box::new(self::socket::Socket::new());
    self::client::Rpc::new(socket)
}

#[cfg(feature = "ipc-use-mmap")]
pub fn listen() -> impl Iterator<Item = RpcHandler> {
    let mut listener = self::mmap::Listener::new();
    std::iter::from_fn(move || {
        let channel = Box::new(listener.accept());
        Some(self::server::RpcHandler::new(channel))
    })
}

#[cfg(feature = "ipc-use-mmap")]
pub fn connect() -> Rpc {
    let channel = Box::new(self::mmap::MmapSynchronizer::conn());
    self::client::Rpc::new(channel)
}
