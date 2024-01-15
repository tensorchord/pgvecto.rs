use super::packet::*;
use super::transport::ServerSocket;
use super::IpcError;
use service::index::IndexOptions;
use service::index::IndexStat;
use service::index::SearchOptions;
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
            RpcPacket::Basic {
                handle,
                vector,
                opts,
            } => RpcHandle::Basic {
                handle,
                vector,
                opts,
                x: Basic {
                    socket: self.socket,
                },
            },
            RpcPacket::Flush { handle } => RpcHandle::Flush {
                handle,
                x: Flush {
                    socket: self.socket,
                },
            },
            RpcPacket::Destroy { handle } => RpcHandle::Destroy {
                handle,
                x: Destroy {
                    socket: self.socket,
                },
            },
            RpcPacket::Stat { handle } => RpcHandle::Stat {
                handle,
                x: Stat {
                    socket: self.socket,
                },
            },
            RpcPacket::Vbase {
                handle,
                vector,
                opts,
            } => RpcHandle::Vbase {
                handle,
                vector,
                opts,
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
    Basic {
        handle: Handle,
        vector: DynamicVector,
        opts: SearchOptions,
        x: Basic,
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
    Destroy {
        handle: Handle,
        x: Destroy,
    },
    Stat {
        handle: Handle,
        x: Stat,
    },
    Vbase {
        handle: Handle,
        vector: DynamicVector,
        opts: SearchOptions,
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
    pub fn reset(mut self, err: ServiceError) -> Result<!, IpcError> {
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
    pub fn reset(mut self, err: ServiceError) -> Result<!, IpcError> {
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
    pub fn reset(mut self, err: ServiceError) -> Result<!, IpcError> {
        self.socket.err(err)
    }
}

pub struct Basic {
    socket: ServerSocket,
}

impl Basic {
    pub fn error(mut self) -> Result<BasicHandler, IpcError> {
        self.socket.ok(basic::BasicErrorPacket {})?;
        Ok(BasicHandler {
            socket: self.socket,
        })
    }
    pub fn reset(mut self, err: ServiceError) -> Result<!, IpcError> {
        self.socket.err(err)
    }
}

pub struct BasicHandler {
    socket: ServerSocket,
}

impl BasicHandler {
    pub fn handle(mut self) -> Result<BasicHandle, IpcError> {
        Ok(match self.socket.recv::<basic::BasicPacket>()? {
            basic::BasicPacket::Next {} => BasicHandle::Next {
                x: BasicNext {
                    socket: self.socket,
                },
            },
            basic::BasicPacket::Leave {} => {
                self.socket.ok(basic::BasicLeavePacket {})?;
                BasicHandle::Leave {
                    x: RpcHandler {
                        socket: self.socket,
                    },
                }
            }
        })
    }
}

pub enum BasicHandle {
    Next { x: BasicNext },
    Leave { x: RpcHandler },
}

pub struct BasicNext {
    socket: ServerSocket,
}

impl BasicNext {
    pub fn leave(mut self, p: Option<Pointer>) -> Result<BasicHandler, IpcError> {
        let packet = basic::BasicNextPacket { p };
        self.socket.ok(packet)?;
        Ok(BasicHandler {
            socket: self.socket,
        })
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
    pub fn reset(mut self, err: ServiceError) -> Result<!, IpcError> {
        self.socket.err(err)
    }
}

pub struct Destroy {
    socket: ServerSocket,
}

impl Destroy {
    pub fn leave(mut self) -> Result<RpcHandler, IpcError> {
        let packet = destroy::DestroyPacket::Leave {};
        self.socket.ok(packet)?;
        Ok(RpcHandler {
            socket: self.socket,
        })
    }
    pub fn reset(mut self, err: ServiceError) -> Result<!, IpcError> {
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
    pub fn reset(mut self, err: ServiceError) -> Result<!, IpcError> {
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
    pub fn reset(mut self, err: ServiceError) -> Result<!, IpcError> {
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
