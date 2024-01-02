use super::packet::*;
use super::transport::ServerSocket;
use super::IpcError;
use service::index::segments::SearchGucs;
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
            RpcPacket::Create { handle, options } => RpcHandle::Create {
                handle,
                options,
                x: Create {
                    socket: self.socket,
                },
            },
            RpcPacket::Insert { handle, insert } => RpcHandle::Insert {
                handle,
                insert,
                x: Insert {
                    socket: self.socket,
                },
            },
            RpcPacket::Delete { handle } => RpcHandle::Delete {
                handle,
                x: Delete {
                    socket: self.socket,
                },
            },
            RpcPacket::Search {
                handle,
                search,
                prefilter,
                gucs,
            } => RpcHandle::Search {
                handle,
                search,
                prefilter,
                gucs,
                x: Search {
                    socket: self.socket,
                },
            },
            RpcPacket::Flush { handle } => RpcHandle::Flush {
                handle,
                x: Flush {
                    socket: self.socket,
                },
            },
            RpcPacket::Destory { handle } => RpcHandle::Destory {
                handle,
                x: Destory {
                    socket: self.socket,
                },
            },
            RpcPacket::Stat { handle } => RpcHandle::Stat {
                handle,
                x: Stat {
                    socket: self.socket,
                },
            },
            RpcPacket::Vbase { handle, vbase } => RpcHandle::Vbase {
                handle,
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
        handle: Handle,
        options: IndexOptions,
        x: Create,
    },
    Search {
        handle: Handle,
        search: (DynamicVector, usize),
        prefilter: bool,
        gucs: SearchGucs,
        x: Search,
    },
    Insert {
        handle: Handle,
        insert: (DynamicVector, Pointer),
        x: Insert,
    },
    Delete {
        handle: Handle,
        x: Delete,
    },
    Flush {
        handle: Handle,
        x: Flush,
    },
    Destory {
        handle: Handle,
        x: Destory,
    },
    Stat {
        handle: Handle,
        x: Stat,
    },
    Vbase {
        handle: Handle,
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
