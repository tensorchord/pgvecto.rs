pub mod transport;

use self::transport::ClientSocket;
use self::transport::ServerSocket;
use crate::gucs::internal::{Transport, TRANSPORT};
use crate::ipc::transport::Packet;
use crate::prelude::*;
use crate::utils::cells::PgRefCell;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub enum ConnectionError {
    ClosedConnection,
    BadSerialization,
    BadDeserialization,
    PacketTooLarge,
}

pub fn listen_unix() -> impl Iterator<Item = ServerRpcHandler> {
    std::iter::from_fn(move || {
        let socket = self::transport::ServerSocket::Unix(self::transport::unix::accept());
        Some(self::ServerRpcHandler::new(socket))
    })
}

pub fn listen_mmap() -> impl Iterator<Item = ServerRpcHandler> {
    std::iter::from_fn(move || {
        let socket = self::transport::ServerSocket::Mmap(self::transport::mmap::accept());
        Some(self::ServerRpcHandler::new(socket))
    })
}

pub fn connect_unix() -> ClientSocket {
    self::transport::ClientSocket::Unix(self::transport::unix::connect())
}

pub fn connect_mmap() -> ClientSocket {
    self::transport::ClientSocket::Mmap(self::transport::mmap::connect())
}

pub fn init() {
    self::transport::mmap::init();
    self::transport::unix::init();
}

impl Drop for ClientRpc {
    fn drop(&mut self) {
        let socket = self.socket.take();
        if let Some(socket) = socket {
            if !std::thread::panicking() {
                let mut x = CLIENTS.borrow_mut();
                x.push(socket);
            }
        }
    }
}

pub struct ClientRpc {
    pub socket: Option<ClientSocket>,
}

impl ClientRpc {
    fn new(socket: ClientSocket) -> Self {
        Self {
            socket: Some(socket),
        }
    }
    fn _ok<U: Packet>(&mut self, packet: U) -> Result<(), ConnectionError> {
        self.socket.as_mut().unwrap().ok(packet)
    }
    fn _recv<U: Packet>(&mut self) -> Result<U, ConnectionError> {
        self.socket.as_mut().unwrap().recv()
    }
}

static CLIENTS: PgRefCell<Vec<ClientSocket>> = unsafe { PgRefCell::new(Vec::new()) };

pub fn client() -> Option<ClientRpc> {
    if !crate::bgworker::is_started() {
        return None;
    }
    let mut x = CLIENTS.borrow_mut();
    if let Some(socket) = x.pop() {
        return Some(ClientRpc::new(socket));
    }
    let socket = match TRANSPORT.get() {
        Transport::unix => connect_unix(),
        Transport::mmap => connect_mmap(),
    };
    Some(ClientRpc::new(socket))
}

pub struct ServerRpcHandler {
    socket: ServerSocket,
}

impl ServerRpcHandler {
    pub(super) fn new(socket: ServerSocket) -> Self {
        Self { socket }
    }
}

macro_rules! define_packets {
    (unary $name:ident($($p_name:ident: $p_ty:ty),*) -> $r:ty;) => {
        paste::paste! {
            #[derive(Debug, Serialize, Deserialize)]
            pub struct [<Packet $name:camel>] {
                pub result: Result<$r, [< $name:camel Error >]>,
            }
        }
    };
    (stream $name:ident($($p_name:ident: $p_ty:ty),*) -> $r:ty;) => {
        paste::paste! {
            #[derive(Debug, Serialize, Deserialize)]
            pub struct [<Packet $name:camel 0>] {
                pub result: Result<(), [< $name:camel Error >]>,
            }

            #[derive(Debug, Serialize, Deserialize)]
            pub enum [<Packet $name:camel>] {
                Next {},
                Leave {},
            }

            #[derive(Debug, Serialize, Deserialize)]
            pub struct [<Packet $name:camel 1>] {
                pub p: Option<$r>,
            }

            #[derive(Debug, Serialize, Deserialize)]
            pub struct [<Packet $name:camel 2>] {}
        }
    };
}

macro_rules! define_client_stuffs {
    (unary $name:ident($($p_name:ident:$p_ty:ty),*) -> $r:ty;) => {
        paste::paste! {
            impl ClientRpc {
                pub fn $name(&mut self, $($p_name:$p_ty),*) -> Result<$r, [< $name:camel Error >]> {
                    let packet = PacketRpc::[< $name:camel >] { $($p_name),* };
                    check_connection(self._ok(packet));
                    let [<Packet $name:camel>] { result } = check_connection(self._recv());
                    result
                }
            }
        }
    };
    (stream $name:ident($($p_name:ident:$p_ty:ty),*) -> $r:ty;) => {
        paste::paste! {
            impl ClientRpc {
                pub fn $name(mut self, $($p_name:$p_ty),*) -> Result<[<Client $name:camel>], (Self, [< $name:camel Error >])> {
                    let packet = PacketRpc::[<$name:camel>] { $($p_name),* };
                    check_connection(self._ok(packet));
                    let [<Packet $name:camel 0>] { result } = check_connection(self._recv());
                    if let Err(e) = result {
                        Err((self, e))
                    } else {
                        Ok([<Client $name:camel>] {
                            socket: self.socket.take()
                        })
                    }
                }
            }

            pub struct [<Client $name:camel>] {
                socket: Option<ClientSocket>,
            }

            impl [<Client $name:camel>] {
                fn _ok<U: Packet>(&mut self, packet: U) -> Result<(), ConnectionError> {
                    self.socket.as_mut().unwrap().ok(packet)
                }
                fn _recv<U: Packet>(&mut self) -> Result<U, ConnectionError> {
                    self.socket.as_mut().unwrap().recv()
                }
            }

            impl [<Client $name:camel>] {
                pub fn next(&mut self) -> Option<$r> {
                    let packet = [<Packet $name:camel>]::Next {};
                    check_connection(self._ok(packet));
                    let [<Packet $name:camel 1>] { p } = check_connection(self._recv());
                    p
                }
                pub fn leave(mut self) -> ClientRpc {
                    let packet = [<Packet $name:camel>]::Leave {};
                    check_connection(self._ok(packet));
                    let [<Packet $name:camel 2>] {} = check_connection(self._recv());
                    ClientRpc { socket: self.socket.take() }
                }
            }
        }
    };
}

macro_rules! define_server_stuffs {
    (unary $name:ident($($p_name:ident:$p_ty:ty),*) -> $r:ty;) => {
        paste::paste! {
            pub struct [<Server $name:camel>] {
                socket: ServerSocket,
            }

            impl [<Server $name:camel>] {
                pub fn leave(mut self, result: Result<$r, [<$name:camel Error>]>) -> Result<ServerRpcHandler, ConnectionError> {
                    let packet = [<Packet $name:camel>] { result };
                    self.socket.ok(packet)?;
                    Ok(ServerRpcHandler {
                        socket: self.socket,
                    })
                }
            }
        }
    };
    (stream $name:ident($($p_name:ident:$p_ty:ty),*) -> $r:ty;) => {
        paste::paste! {
            pub struct [<Server $name:camel>] {
                socket: ServerSocket,
            }

            impl [<Server $name:camel>] {
                pub fn error_ok(mut self) -> Result<[<Server $name:camel Handler>], ConnectionError> {
                    self.socket.ok([<Packet $name:camel 0>] { result: Ok(()) })?;
                    Ok([<Server $name:camel Handler>] {
                        socket: self.socket,
                    })
                }
                pub fn error_err(mut self, err: [<$name:camel Error>]) -> Result<ServerRpcHandler, ConnectionError> {
                    self.socket.ok([<Packet $name:camel 0>] { result: Err(err) })?;
                    Ok(ServerRpcHandler {
                        socket: self.socket,
                    })
                }
            }

            pub struct [<Server $name:camel Handler>] {
                socket: ServerSocket,
            }

            impl [<Server $name:camel Handler>] {
                pub fn handle(mut self) -> Result<[<Server $name:camel Handle>], ConnectionError> {
                    Ok(match self.socket.recv::<[<Packet $name:camel>]>()? {
                        [<Packet $name:camel>]::Next {} => [<Server $name:camel Handle>]::Next {
                            x: [<Server $name:camel Next>] {
                                socket: self.socket,
                            },
                        },
                        [<Packet $name:camel>]::Leave {} => {
                            self.socket.ok([<Packet $name:camel 2>] {})?;
                            [<Server $name:camel Handle>]::Leave {
                                x: ServerRpcHandler {
                                    socket: self.socket,
                                },
                            }
                        }
                    })
                }
            }

            pub enum [<Server $name:camel Handle>] {
                Next { x: [<Server $name:camel Next>] },
                Leave { x: ServerRpcHandler },
            }

            pub struct [<Server $name:camel Next>] {
                socket: ServerSocket,
            }

            impl [<Server $name:camel Next>] {
                pub fn leave(mut self, p: Option<$r>) -> Result<[<Server $name:camel Handler>], ConnectionError> {
                    let packet = [<Packet $name:camel 1>] { p };
                    self.socket.ok(packet)?;
                    Ok([<Server $name:camel Handler>] {
                        socket: self.socket,
                    })
                }
            }
        }
    };
}

macro_rules! defines {
    (
        $($kind:ident $name:ident($($p_name:ident:$p_ty:ty),*) -> $r:ty;)*
    ) => {
        $(define_packets!($kind $name($($p_name:$p_ty),*) -> $r;);)*
        $(define_client_stuffs!($kind $name($($p_name:$p_ty),*) -> $r;);)*
        $(define_server_stuffs!($kind $name($($p_name:$p_ty),*) -> $r;);)*

        paste::paste! {
            #[derive(Debug, Serialize, Deserialize)]
            pub enum PacketRpc {
                $([<$name:camel>]{$($p_name:$p_ty),*},)*
            }

            impl ServerRpcHandler {
                pub fn handle(mut self) -> Result<ServerRpcHandle, ConnectionError> {
                    Ok(match self.socket.recv::<PacketRpc>()? {
                        $(PacketRpc::[<$name:camel>] { $($p_name),* } => ServerRpcHandle::[<$name:camel>] {
                            $($p_name),*,
                            x: [<Server $name:camel>] {
                                socket: self.socket,
                            },
                        },)*
                    })
                }
            }

            pub enum ServerRpcHandle {
                $([<$name:camel>] {
                    $($p_name:$p_ty),*,
                    x: [< Server $name:camel >],
                }),*
            }
        }
    };
}

defines! {
    unary create(handle: Handle, options: IndexOptions) -> ();
    unary drop(handle: Handle) -> ();
    unary flush(handle: Handle) -> ();
    unary insert(handle: Handle, vector: OwnedVector, pointer: Pointer) -> ();
    unary delete(handle: Handle, pointer: Pointer) -> ();
    stream basic(handle: Handle, vector: OwnedVector, opts: SearchOptions) -> Pointer;
    stream vbase(handle: Handle, vector: OwnedVector, opts: SearchOptions) -> Pointer;
    stream list(handle: Handle) -> Pointer;
    unary stat(handle: Handle) -> IndexStat;
}
