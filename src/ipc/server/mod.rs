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
        Ok(match self.socket.recv::<ClientPacket>()? {
            ClientPacket::Create { id, options } => RpcHandle::Create {
                id,
                options,
                x: Create {
                    socket: self.socket,
                },
            },
            ClientPacket::Insert { id, insert } => RpcHandle::Insert {
                id,
                insert,
                x: Insert {
                    socket: self.socket,
                },
            },
            ClientPacket::Delete { id } => RpcHandle::Delete {
                id,
                x: Delete {
                    socket: self.socket,
                },
            },
            ClientPacket::Search {
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
            ClientPacket::Flush { id } => RpcHandle::Flush {
                id,
                x: Flush {
                    socket: self.socket,
                },
            },
            ClientPacket::Destory { ids } => RpcHandle::Destory {
                ids,
                x: Destory {
                    socket: self.socket,
                },
            },
            ClientPacket::Stat { id } => RpcHandle::Stat {
                id,
                x: Stat {
                    socket: self.socket,
                },
            },
            ClientPacket::Vbase { id, search } => todo!(),
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
}

pub struct Create {
    socket: Socket,
}

impl Create {
    pub fn leave(mut self) -> Result<RpcHandler, IpcError> {
        let packet = create::ServerPacket::Leave {};
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
        let packet = insert::ServerPacket::Leave { result };
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
        let packet = delete::ServerPacket::Test { p };
        self.socket.send(packet)?;
        let delete::ClientTestPacket { delete } = self.socket.recv::<delete::ClientTestPacket>()?;
        Ok(delete)
    }
    pub fn leave(mut self, result: Result<(), FriendlyError>) -> Result<RpcHandler, IpcError> {
        let packet = delete::ServerPacket::Leave { result };
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
        let packet = search::ServerPacket::Check { p };
        self.socket.send(packet)?;
        let search::ClientCheckPacket { result } =
            self.socket.recv::<search::ClientCheckPacket>()?;
        Ok(result)
    }
    pub fn leave(
        mut self,
        result: Result<Vec<Pointer>, FriendlyError>,
    ) -> Result<RpcHandler, IpcError> {
        let packet = search::ServerPacket::Leave { result };
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
    pub fn leave(mut self, result: Result<(), FriendlyError>) -> Result<RpcHandler, IpcError> {
        let packet = flush::ServerPacket::Leave { result };
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
        let packet = destory::ServerPacket::Leave {};
        self.socket.send(packet)?;
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
        let packet = stat::ServerPacket::Leave { result };
        self.socket.send(packet)?;
        Ok(RpcHandler {
            socket: self.socket,
        })
    }
}
