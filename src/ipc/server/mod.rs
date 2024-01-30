use super::packet::*;
use super::transport::ServerSocket;
use super::ConnectionError;
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
    pub fn handle(mut self) -> Result<RpcHandle, ConnectionError> {
        Ok(match self.socket.recv::<RpcPacket>()? {
            RpcPacket::Commit {
                pending_deletes,
                pending_dirty,
            } => RpcHandle::Commit {
                pending_deletes,
                pending_dirty,
                x: Commit {
                    socket: self.socket,
                },
            },
            RpcPacket::Abort { pending_deletes } => RpcHandle::Abort {
                pending_deletes,
                x: Abort {
                    socket: self.socket,
                },
            },
            RpcPacket::Create { handle, options } => RpcHandle::Create {
                handle,
                options,
                x: Create {
                    socket: self.socket,
                },
            },
            RpcPacket::Insert {
                handle,
                vector,
                pointer,
            } => RpcHandle::Insert {
                handle,
                vector,
                pointer,
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
            RpcPacket::Upgrade {} => RpcHandle::Upgrade {
                x: Upgrade {
                    socket: self.socket,
                },
            },
        })
    }
}

pub enum RpcHandle {
    Commit {
        pending_deletes: Vec<Handle>,
        pending_dirty: Vec<Handle>,
        x: Commit,
    },
    Abort {
        pending_deletes: Vec<Handle>,
        x: Abort,
    },
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
        vector: DynamicVector,
        pointer: Pointer,
        x: Insert,
    },
    Delete {
        handle: Handle,
        x: Delete,
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
    Upgrade {
        x: Upgrade,
    },
}

pub struct Commit {
    socket: ServerSocket,
}

impl Commit {
    pub fn leave(mut self) -> Result<RpcHandler, ConnectionError> {
        let packet = commit::CommitPacket::Leave {};
        self.socket.ok(packet)?;
        Ok(RpcHandler {
            socket: self.socket,
        })
    }
    #[allow(dead_code)]
    pub fn reset(mut self, err: ServiceError) -> Result<!, ConnectionError> {
        self.socket.err(err)
    }
}

pub struct Abort {
    socket: ServerSocket,
}

impl Abort {
    pub fn leave(mut self) -> Result<RpcHandler, ConnectionError> {
        let packet = abort::AbortPacket::Leave {};
        self.socket.ok(packet)?;
        Ok(RpcHandler {
            socket: self.socket,
        })
    }
    #[allow(dead_code)]
    pub fn reset(mut self, err: ServiceError) -> Result<!, ConnectionError> {
        self.socket.err(err)
    }
}

pub struct Create {
    socket: ServerSocket,
}

impl Create {
    pub fn leave(mut self) -> Result<RpcHandler, ConnectionError> {
        let packet = create::CreatePacket::Leave {};
        self.socket.ok(packet)?;
        Ok(RpcHandler {
            socket: self.socket,
        })
    }
    pub fn reset(mut self, err: ServiceError) -> Result<!, ConnectionError> {
        self.socket.err(err)
    }
}

pub struct Insert {
    socket: ServerSocket,
}

impl Insert {
    pub fn leave(mut self) -> Result<RpcHandler, ConnectionError> {
        let packet = insert::InsertPacket::Leave {};
        self.socket.ok(packet)?;
        Ok(RpcHandler {
            socket: self.socket,
        })
    }
    pub fn reset(mut self, err: ServiceError) -> Result<!, ConnectionError> {
        self.socket.err(err)
    }
}

pub struct Delete {
    socket: ServerSocket,
}

impl Delete {
    pub fn next(&mut self, p: Pointer) -> Result<bool, ConnectionError> {
        let packet = delete::DeletePacket::Test { p };
        self.socket.ok(packet)?;
        let delete::DeleteTestPacket { delete } = self.socket.recv::<delete::DeleteTestPacket>()?;
        Ok(delete)
    }
    pub fn leave(mut self) -> Result<RpcHandler, ConnectionError> {
        let packet = delete::DeletePacket::Leave {};
        self.socket.ok(packet)?;
        Ok(RpcHandler {
            socket: self.socket,
        })
    }
    pub fn reset(mut self, err: ServiceError) -> Result<!, ConnectionError> {
        self.socket.err(err)
    }
}

pub struct Basic {
    socket: ServerSocket,
}

impl Basic {
    pub fn error(mut self) -> Result<BasicHandler, ConnectionError> {
        self.socket.ok(basic::BasicErrorPacket {})?;
        Ok(BasicHandler {
            socket: self.socket,
        })
    }
    pub fn reset(mut self, err: ServiceError) -> Result<!, ConnectionError> {
        self.socket.err(err)
    }
}

pub struct BasicHandler {
    socket: ServerSocket,
}

impl BasicHandler {
    pub fn handle(mut self) -> Result<BasicHandle, ConnectionError> {
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
    pub fn leave(mut self, p: Option<Pointer>) -> Result<BasicHandler, ConnectionError> {
        let packet = basic::BasicNextPacket { p };
        self.socket.ok(packet)?;
        Ok(BasicHandler {
            socket: self.socket,
        })
    }
}

pub struct Stat {
    socket: ServerSocket,
}

impl Stat {
    pub fn leave(mut self, result: IndexStat) -> Result<RpcHandler, ConnectionError> {
        let packet = stat::StatPacket::Leave { result };
        self.socket.ok(packet)?;
        Ok(RpcHandler {
            socket: self.socket,
        })
    }
    pub fn reset(mut self, err: ServiceError) -> Result<!, ConnectionError> {
        self.socket.err(err)
    }
}

pub struct Vbase {
    socket: ServerSocket,
}

impl Vbase {
    pub fn error(mut self) -> Result<VbaseHandler, ConnectionError> {
        self.socket.ok(vbase::VbaseErrorPacket {})?;
        Ok(VbaseHandler {
            socket: self.socket,
        })
    }
    pub fn reset(mut self, err: ServiceError) -> Result<!, ConnectionError> {
        self.socket.err(err)
    }
}

pub struct VbaseHandler {
    socket: ServerSocket,
}

impl VbaseHandler {
    pub fn handle(mut self) -> Result<VbaseHandle, ConnectionError> {
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
    pub fn leave(mut self, p: Option<Pointer>) -> Result<VbaseHandler, ConnectionError> {
        let packet = vbase::VbaseNextPacket { p };
        self.socket.ok(packet)?;
        Ok(VbaseHandler {
            socket: self.socket,
        })
    }
}

pub struct Upgrade {
    socket: ServerSocket,
}

impl Upgrade {
    pub fn leave(mut self) -> Result<RpcHandler, ConnectionError> {
        let packet = upgrade::UpgradePacket::Leave {};
        self.socket.ok(packet)?;
        Ok(RpcHandler {
            socket: self.socket,
        })
    }
    #[allow(dead_code)]
    pub fn reset(mut self, err: ServiceError) -> Result<!, ConnectionError> {
        self.socket.err(err)
    }
}
