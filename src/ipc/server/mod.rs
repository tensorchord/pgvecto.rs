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
            RpcPacket::Flush { handle } => RpcHandle::Flush {
                handle,
                x: Flush {
                    socket: self.socket,
                },
            },
            RpcPacket::Drop { handle } => RpcHandle::Drop {
                handle,
                x: Drop {
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
            RpcPacket::Delete { handle, pointer } => RpcHandle::Delete {
                handle,
                pointer,
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
            RpcPacket::List { handle } => RpcHandle::List {
                handle,
                x: List {
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
    Flush {
        handle: Handle,
        x: Flush,
    },
    Drop {
        handle: Handle,
        x: Drop,
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
        pointer: Pointer,
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
    List {
        handle: Handle,
        x: List,
    },
    Upgrade {
        x: Upgrade,
    },
}

pub struct Flush {
    socket: ServerSocket,
}

impl Flush {
    pub fn leave(mut self) -> Result<RpcHandler, ConnectionError> {
        let packet = flush::FlushPacket::Leave {};
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

pub struct Drop {
    socket: ServerSocket,
}

impl Drop {
    pub fn leave(mut self) -> Result<RpcHandler, ConnectionError> {
        let packet = drop::DropPacket::Leave {};
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

pub struct List {
    socket: ServerSocket,
}

impl List {
    pub fn error(mut self) -> Result<ListHandler, ConnectionError> {
        self.socket.ok(list::ListErrorPacket {})?;
        Ok(ListHandler {
            socket: self.socket,
        })
    }
    pub fn reset(mut self, err: ServiceError) -> Result<!, ConnectionError> {
        self.socket.err(err)
    }
}

pub struct ListHandler {
    socket: ServerSocket,
}

impl ListHandler {
    pub fn handle(mut self) -> Result<ListHandle, ConnectionError> {
        Ok(match self.socket.recv::<list::ListPacket>()? {
            list::ListPacket::Next {} => ListHandle::Next {
                x: ListNext {
                    socket: self.socket,
                },
            },
            list::ListPacket::Leave {} => {
                self.socket.ok(list::ListLeavePacket {})?;
                ListHandle::Leave {
                    x: RpcHandler {
                        socket: self.socket,
                    },
                }
            }
        })
    }
}

pub enum ListHandle {
    Next { x: ListNext },
    Leave { x: RpcHandler },
}

pub struct ListNext {
    socket: ServerSocket,
}

impl ListNext {
    pub fn leave(mut self, p: Option<Pointer>) -> Result<ListHandler, ConnectionError> {
        let packet = list::ListNextPacket { p };
        self.socket.ok(packet)?;
        Ok(ListHandler {
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
