use crate::bgworker::index::IndexOptions;
use crate::ipc::packet::*;
use crate::ipc::transport::Socket;
use crate::ipc::ServerIpcError;
use crate::prelude::*;

pub struct RpcHandler {
    socket: Socket,
}

impl RpcHandler {
    pub(super) fn new(socket: Socket) -> Self {
        Self { socket }
    }
    pub fn handle(mut self) -> Result<RpcHandle, ServerIpcError> {
        Ok(match self.socket.server_recv::<RpcPacket>()? {
            RpcPacket::Build { id, options } => RpcHandle::Build {
                id,
                options,
                x: Build {
                    socket: self.socket,
                    reach: false,
                },
            },
            RpcPacket::Insert { id, insert } => RpcHandle::Insert {
                id,
                insert,
                x: Insert {
                    socket: self.socket,
                },
            },
            RpcPacket::Delete { id, delete } => RpcHandle::Delete {
                id,
                delete,
                x: Delete {
                    socket: self.socket,
                },
            },
            RpcPacket::Search { id, target, k } => RpcHandle::Search {
                id,
                target,
                k,
                x: Search {
                    socket: self.socket,
                },
            },
            RpcPacket::Load { id } => RpcHandle::Load {
                id,
                x: Load {
                    socket: self.socket,
                },
            },
            RpcPacket::Unload { id } => RpcHandle::Unload {
                id,
                x: Unload {
                    socket: self.socket,
                },
            },
            RpcPacket::Flush { id } => RpcHandle::Flush {
                id,
                x: Flush {
                    socket: self.socket,
                },
            },
            RpcPacket::Clean { id } => RpcHandle::Clean {
                id,
                x: Clean {
                    socket: self.socket,
                },
            },
            RpcPacket::Leave {} => RpcHandle::Leave {},
        })
    }
}

pub enum RpcHandle {
    Build {
        id: Id,
        options: IndexOptions,
        x: Build,
    },
    Search {
        id: Id,
        target: Box<[Scalar]>,
        k: usize,
        x: Search,
    },
    Insert {
        id: Id,
        insert: (Box<[Scalar]>, Pointer),
        x: Insert,
    },
    Delete {
        id: Id,
        delete: Pointer,
        x: Delete,
    },
    Load {
        id: Id,
        x: Load,
    },
    Unload {
        id: Id,
        x: Unload,
    },
    Flush {
        id: Id,
        x: Flush,
    },
    Clean {
        id: Id,
        x: Clean,
    },
    Leave {},
}

pub struct Build {
    socket: Socket,
    reach: bool,
}

impl Build {
    pub fn next(&mut self) -> Result<Option<(Box<[Scalar]>, Pointer)>, ServerIpcError> {
        if !self.reach {
            let packet = self.socket.server_recv::<NextPacket>()?;
            match packet {
                NextPacket::Leave { data: Some(data) } => Ok(Some(data)),
                NextPacket::Leave { data: None } => {
                    self.reach = true;
                    Ok(None)
                }
            }
        } else {
            Ok(None)
        }
    }
    pub fn leave(mut self) -> Result<RpcHandler, ServerIpcError> {
        let packet = BuildPacket::Leave {};
        self.socket.server_send(packet)?;
        Ok(RpcHandler {
            socket: self.socket,
        })
    }
}

pub struct Insert {
    socket: Socket,
}

impl Insert {
    pub fn leave(mut self) -> Result<RpcHandler, ServerIpcError> {
        let packet = InsertPacket::Leave {};
        self.socket.server_send(packet)?;
        Ok(RpcHandler {
            socket: self.socket,
        })
    }
}

pub struct Delete {
    socket: Socket,
}

impl Delete {
    pub fn leave(mut self) -> Result<RpcHandler, ServerIpcError> {
        let packet = DeletePacket::Leave {};
        self.socket.server_send(packet)?;
        Ok(RpcHandler {
            socket: self.socket,
        })
    }
}

pub struct Search {
    socket: Socket,
}

impl Search {
    pub fn check(&mut self, p: Pointer) -> Result<bool, ServerIpcError> {
        let packet = SearchPacket::Check { p };
        self.socket.server_send(packet)?;
        let CheckPacket::Leave { result } = self.socket.server_recv::<CheckPacket>()?;
        Ok(result)
    }
    pub fn leave(mut self, result: Vec<Pointer>) -> Result<RpcHandler, ServerIpcError> {
        let packet = SearchPacket::Leave { result };
        self.socket.server_send(packet)?;
        Ok(RpcHandler {
            socket: self.socket,
        })
    }
}

pub struct Load {
    socket: Socket,
}

impl Load {
    pub fn leave(mut self) -> Result<RpcHandler, ServerIpcError> {
        let packet = LoadPacket::Leave {};
        self.socket.server_send(packet)?;
        Ok(RpcHandler {
            socket: self.socket,
        })
    }
}

pub struct Unload {
    socket: Socket,
}

impl Unload {
    pub fn leave(mut self) -> Result<RpcHandler, ServerIpcError> {
        let packet = UnloadPacket::Leave {};
        self.socket.server_send(packet)?;
        Ok(RpcHandler {
            socket: self.socket,
        })
    }
}

pub struct Flush {
    socket: Socket,
}

impl Flush {
    pub fn leave(mut self) -> Result<RpcHandler, ServerIpcError> {
        let packet = FlushPacket::Leave {};
        self.socket.server_send(packet)?;
        Ok(RpcHandler {
            socket: self.socket,
        })
    }
}

pub struct Clean {
    socket: Socket,
}

impl Clean {
    pub fn leave(mut self) -> Result<RpcHandler, ServerIpcError> {
        let packet = CleanPacket::Leave {};
        self.socket.server_send(packet)?;
        Ok(RpcHandler {
            socket: self.socket,
        })
    }
}
