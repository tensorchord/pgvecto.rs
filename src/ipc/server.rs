use crate::index::IndexOptions;
use crate::ipc::packet::*;
use crate::ipc::transport::Socket;
use crate::ipc::IpcError;
use crate::prelude::*;

pub struct RpcHandler {
    socket: Socket,
}

impl RpcHandler {
    pub(super) fn new(socket: Socket) -> Self {
        Self { socket }
    }
    pub fn handle(mut self) -> Result<RpcHandle, IpcError> {
        Ok(match self.socket.recv::<RpcPacket>()? {
            RpcPacket::Create { id, options } => RpcHandle::Create {
                id,
                options,
                x: Create {
                    socket: self.socket,
                },
            },
            RpcPacket::Insert { id, insert } => RpcHandle::Insert {
                id,
                insert,
                x: Insert {
                    socket: self.socket,
                },
            },
            RpcPacket::Delete { id } => RpcHandle::Delete {
                id,
                x: Delete {
                    socket: self.socket,
                },
            },
            RpcPacket::Search {
                id,
                search,
                prefilter,
            } => RpcHandle::Search {
                id,
                search,
                prefilter,
                x: Search {
                    socket: self.socket,
                },
            },
            RpcPacket::Flush { id } => RpcHandle::Flush {
                id,
                x: Flush {
                    socket: self.socket,
                },
            },
            RpcPacket::Destory { id } => RpcHandle::Destory {
                id,
                x: Destory {
                    socket: self.socket,
                },
            },
            RpcPacket::Leave {} => RpcHandle::Leave {},
        })
    }
}

pub enum RpcHandle {
    Create {
        id: Id,
        options: IndexOptions,
        x: Create,
    },
    Search {
        id: Id,
        search: (Vec<Scalar>, usize),
        prefilter: bool,
        x: Search,
    },
    Insert {
        id: Id,
        insert: (Vec<Scalar>, Pointer),
        x: Insert,
    },
    Delete {
        id: Id,
        x: Delete,
    },
    Flush {
        id: Id,
        x: Flush,
    },
    Destory {
        id: Id,
        x: Destory,
    },
    Leave {},
}

pub struct Create {
    socket: Socket,
}

impl Create {
    pub fn leave(mut self) -> Result<RpcHandler, IpcError> {
        let packet = CreatePacket::Leave {};
        self.socket.send(packet)?;
        Ok(RpcHandler {
            socket: self.socket,
        })
    }
}

pub struct Insert {
    socket: Socket,
}

impl Insert {
    pub fn leave(mut self, result: Result<(), FriendlyError>) -> Result<RpcHandler, IpcError> {
        let packet = InsertPacket::Leave { result };
        self.socket.send(packet)?;
        Ok(RpcHandler {
            socket: self.socket,
        })
    }
}

pub struct Delete {
    socket: Socket,
}

impl Delete {
    pub fn next(&mut self, p: Pointer) -> Result<bool, IpcError> {
        let packet = DeletePacket::Next { p };
        self.socket.send(packet)?;
        let DeleteNextPacket::Leave { delete } = self.socket.recv::<DeleteNextPacket>()?;
        Ok(delete)
    }
    pub fn leave(mut self) -> Result<RpcHandler, IpcError> {
        let packet = DeletePacket::Leave {};
        self.socket.send(packet)?;
        Ok(RpcHandler {
            socket: self.socket,
        })
    }
}

pub struct Search {
    socket: Socket,
}

impl Search {
    pub fn check(&mut self, p: Pointer) -> Result<bool, IpcError> {
        let packet = SearchPacket::Check { p };
        self.socket.send(packet)?;
        let SearchCheckPacket::Leave { result } = self.socket.recv::<SearchCheckPacket>()?;
        Ok(result)
    }
    pub fn leave(
        mut self,
        result: Result<Vec<Pointer>, FriendlyError>,
    ) -> Result<RpcHandler, IpcError> {
        let packet = SearchPacket::Leave { result };
        self.socket.send(packet)?;
        Ok(RpcHandler {
            socket: self.socket,
        })
    }
}

pub struct Flush {
    socket: Socket,
}

impl Flush {
    pub fn leave(mut self) -> Result<RpcHandler, IpcError> {
        let packet = FlushPacket::Leave {};
        self.socket.send(packet)?;
        Ok(RpcHandler {
            socket: self.socket,
        })
    }
}

pub struct Destory {
    socket: Socket,
}

impl Destory {
    pub fn leave(mut self) -> Result<RpcHandler, IpcError> {
        let packet = DestoryPacket::Leave {};
        self.socket.send(packet)?;
        Ok(RpcHandler {
            socket: self.socket,
        })
    }
}
