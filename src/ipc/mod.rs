pub mod client;
mod packet;
pub mod server;
pub mod transport;

use self::server::RpcHandler;
use crate::prelude::*;
use serde::{Deserialize, Serialize};
use service::prelude::ServiceError;
use thiserror::Error;

#[derive(Debug, Clone, Error)]
pub enum ConnectionError {
    #[error("\
IPC connection is closed unexpected.
ADVICE: The error is raisen by background worker errors. \
Please check the full PostgreSQL log to get more information. Please read `https://docs.pgvecto.rs/admin/configuration.html`.\
")]
    Unexpected,
    #[error(transparent)]
    Service(#[from] ServiceError),
    #[error(transparent)]
    Grace(#[from] GraceError),
}

impl FriendlyError for ConnectionError {}

#[derive(Debug, Clone, Error, Serialize, Deserialize)]
#[error("Client performs a graceful shutdown.")]
pub struct GraceError;

impl FriendlyError for GraceError {}

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
    self::transport::ClientSocket::Unix(self::transport::unix::connect())
}

pub fn connect_mmap() -> self::transport::ClientSocket {
    self::transport::ClientSocket::Mmap(self::transport::mmap::connect())
}

pub fn init() {
    self::transport::mmap::init();
    self::transport::unix::init();
}
