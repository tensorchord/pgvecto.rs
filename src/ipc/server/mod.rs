use super::packet::*;
use super::transport::ServerSocket;
use super::IpcError;
use service::index::IndexOptions;
use service::index::IndexStat;
use service::prelude::*;

pub struct RpcHandler {
    socket: ServerSocket,
}

impl RpcHandler {
    pub(super) fn new(socket: ServerSocket) -> Self {
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
            RpcPacket::Destory { ids } => RpcHandle::Destory {
                ids,
                x: Destory {
                    socket: self.socket,
                },
            },
            RpcPacket::Stat { id } => RpcHandle::Stat {
                id,
                x: Stat {
                    socket: self.socket,
                },
            },
            RpcPacket::Vbase { id, vbase } => RpcHandle::Vbase {
                id,
                vbase,
                x: Vbase {
                    socket: self.socket,
                },
            },
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
        search: (DynamicVector, usize),
        prefilter: bool,
        x: Search,
    },
    Insert {
        id: Id,
        insert: (DynamicVector, Pointer),
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
        ids: Vec<Id>,
        x: Destory,
    },
    Stat {
        id: Id,
        x: Stat,
    },
    Vbase {
        id: Id,
        vbase: (DynamicVector, usize),
        x: Vbase,
    },
}

pub struct Create {
    socket: ServerSocket,
}

impl Create {
    pub fn leave(mut self) -> Result<RpcHandler, IpcError> {
        let packet = create::CreatePacket::Leave {};
        self.socket.ok(packet)?;
        Ok(RpcHandler {
            socket: self.socket,
        })
    }
    pub fn reset(mut self, err: FriendlyError) -> Result<!, IpcError> {
        self.socket.err(err)
    }
}

pub struct Insert {
    socket: ServerSocket,
}

impl Insert {
    pub fn leave(mut self) -> Result<RpcHandler, IpcError> {
        let packet = insert::InsertPacket::Leave {};
        self.socket.ok(packet)?;
        Ok(RpcHandler {
            socket: self.socket,
        })
    }
    pub fn reset(mut self, err: FriendlyError) -> Result<!, IpcError> {
        self.socket.err(err)
    }
}

pub struct Delete {
    socket: ServerSocket,
}

impl Delete {
    pub fn next(&mut self, p: Pointer) -> Result<bool, IpcError> {
        let packet = delete::DeletePacket::Test { p };
        self.socket.ok(packet)?;
        let delete::DeleteTestPacket { delete } = self.socket.recv::<delete::DeleteTestPacket>()?;
        Ok(delete)
    }
    pub fn leave(mut self) -> Result<RpcHandler, IpcError> {
        let packet = delete::DeletePacket::Leave {};
        self.socket.ok(packet)?;
        Ok(RpcHandler {
            socket: self.socket,
        })
    }
    pub fn reset(mut self, err: FriendlyError) -> Result<!, IpcError> {
        self.socket.err(err)
    }
}

pub struct Search {
    socket: ServerSocket,
}

impl Search {
    pub fn check(&mut self, p: Pointer) -> Result<bool, IpcError> {
        let packet = search::SearchPacket::Check { p };
        self.socket.ok(packet)?;
        let search::SearchCheckPacket { result } =
            self.socket.recv::<search::SearchCheckPacket>()?;
        Ok(result)
    }
    pub fn leave(mut self, result: Vec<Pointer>) -> Result<RpcHandler, IpcError> {
        let packet = search::SearchPacket::Leave { result };
        self.socket.ok(packet)?;
        Ok(RpcHandler {
            socket: self.socket,
        })
    }
    pub fn reset(mut self, err: FriendlyError) -> Result<!, IpcError> {
        self.socket.err(err)
    }
}

pub struct Flush {
    socket: ServerSocket,
}

impl Flush {
    pub fn leave(mut self) -> Result<RpcHandler, IpcError> {
        let packet = flush::FlushPacket::Leave {};
        self.socket.ok(packet)?;
        Ok(RpcHandler {
            socket: self.socket,
        })
    }
    pub fn reset(mut self, err: FriendlyError) -> Result<!, IpcError> {
        self.socket.err(err)
    }
}

pub struct Destory {
    socket: ServerSocket,
}

impl Destory {
    pub fn leave(mut self) -> Result<RpcHandler, IpcError> {
        let packet = destory::DestoryPacket::Leave {};
        self.socket.ok(packet)?;
        Ok(RpcHandler {
            socket: self.socket,
        })
    }
    pub fn reset(mut self, err: FriendlyError) -> Result<!, IpcError> {
        self.socket.err(err)
    }
}

pub struct Stat {
    socket: ServerSocket,
}

impl Stat {
    pub fn leave(mut self, result: IndexStat) -> Result<RpcHandler, IpcError> {
        let packet = stat::StatPacket::Leave { result };
        self.socket.ok(packet)?;
        Ok(RpcHandler {
            socket: self.socket,
        })
    }
    pub fn reset(mut self, err: FriendlyError) -> Result<!, IpcError> {
        self.socket.err(err)
    }
}

pub struct Vbase {
    socket: ServerSocket,
}

impl Vbase {
    pub fn error(mut self) -> Result<VbaseHandler, IpcError> {
        self.socket.ok(vbase::VbaseErrorPacket {})?;
        Ok(VbaseHandler {
            socket: self.socket,
        })
    }
    pub fn reset(mut self, err: FriendlyError) -> Result<!, IpcError> {
        self.socket.err(err)
    }
}

pub struct VbaseHandler {
    socket: ServerSocket,
}

impl VbaseHandler {
    pub fn handle(mut self) -> Result<VbaseHandle, IpcError> {
        Ok(match self.socket.recv::<vbase::VbasePacket>()? {
            vbase::VbasePacket::Next {} => VbaseHandle::Next {
                x: VbaseNext {
                    socket: self.socket,
                },
            },
            vbase::VbasePacket::Leave {} => {
                self.socket.ok(vbase::VbaseLeavePacket {})?;
                VbaseHandle::Leave {
                    x: RpcHandler {
                        socket: self.socket,
                    },
                }
            }
        })
    }
}

pub enum VbaseHandle {
    Next { x: VbaseNext },
    Leave { x: RpcHandler },
}

pub struct VbaseNext {
    socket: ServerSocket,
}

impl VbaseNext {
    pub fn leave(mut self, p: Option<Pointer>) -> Result<VbaseHandler, IpcError> {
        let packet = vbase::VbaseNextPacket { p };
        self.socket.ok(packet)?;
        Ok(VbaseHandler {
            socket: self.socket,
        })
    }
}
