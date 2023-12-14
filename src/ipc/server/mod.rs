use super::packet::*;
use super::transport::Socket;
use super::IpcError;
use service::index::IndexOptions;
use service::index::IndexStat;
use service::prelude::*;

pub struct RpcHandler {
    socket: Socket,
}

impl RpcHandler {
    pub(super) fn new(socket: Socket) -> Self {
        Self { socket }
    }
    pub fn handle(mut self) -> Result<RpcHandle, IpcError> {
        Ok(match self.socket.server_recv::<RpcPacket>()? {
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
            RpcPacket::Vbase { id, vector } => {
                self.socket.server_send(vbase::VbaseNopPacket {})?;
                RpcHandle::Vbase {
                    id,
                    vector,
                    x: VbaseHandler {
                        socket: self.socket,
                    },
                }
            }
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
        vector: DynamicVector,
        x: VbaseHandler,
    },
}

pub struct Create {
    socket: Socket,
}

impl Create {
    pub fn leave(mut self) -> Result<RpcHandler, IpcError> {
        let packet = create::CreatePacket::Leave {};
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
    pub fn leave(mut self, result: Result<(), FriendlyError>) -> Result<RpcHandler, IpcError> {
        let packet = insert::InsertPacket::Leave { result };
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
    pub fn next(&mut self, p: Pointer) -> Result<bool, IpcError> {
        let packet = delete::DeletePacket::Test { p };
        self.socket.server_send(packet)?;
        let delete::DeleteTestPacket { delete } =
            self.socket.server_recv::<delete::DeleteTestPacket>()?;
        Ok(delete)
    }
    pub fn leave(mut self, result: Result<(), FriendlyError>) -> Result<RpcHandler, IpcError> {
        let packet = delete::DeletePacket::Leave { result };
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
    pub fn check(&mut self, p: Pointer) -> Result<bool, IpcError> {
        let packet = search::SearchPacket::Check { p };
        self.socket.server_send(packet)?;
        let search::SearchCheckPacket { result } =
            self.socket.server_recv::<search::SearchCheckPacket>()?;
        Ok(result)
    }
    pub fn leave(
        mut self,
        result: Result<Vec<Pointer>, FriendlyError>,
    ) -> Result<RpcHandler, IpcError> {
        let packet = search::SearchPacket::Leave { result };
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
    pub fn leave(mut self, result: Result<(), FriendlyError>) -> Result<RpcHandler, IpcError> {
        let packet = flush::FlushPacket::Leave { result };
        self.socket.server_send(packet)?;
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
        let packet = destory::DestoryPacket::Leave {};
        self.socket.server_send(packet)?;
        Ok(RpcHandler {
            socket: self.socket,
        })
    }
}

pub struct Stat {
    socket: Socket,
}

impl Stat {
    pub fn leave(
        mut self,
        result: Result<IndexStat, FriendlyError>,
    ) -> Result<RpcHandler, IpcError> {
        let packet = stat::StatPacket::Leave { result };
        self.socket.server_send(packet)?;
        Ok(RpcHandler {
            socket: self.socket,
        })
    }
}

pub struct VbaseHandler {
    socket: Socket,
}

impl VbaseHandler {
    pub fn handle(mut self) -> Result<VbaseHandle, IpcError> {
        Ok(match self.socket.server_recv::<vbase::VbasePacket>()? {
            vbase::VbasePacket::Next {} => VbaseHandle::Next {
                x: VbaseNext {
                    socket: self.socket,
                },
            },
            vbase::VbasePacket::Leave {} => {
                self.socket.server_send(vbase::VbaseLeavePacket {})?;
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
    socket: Socket,
}

impl VbaseNext {
    pub fn leave(mut self, p: Option<Pointer>) -> Result<VbaseHandler, IpcError> {
        let packet = vbase::VbaseNextPacket { p };
        self.socket.server_send(packet)?;
        Ok(VbaseHandler {
            socket: self.socket,
        })
    }
}
