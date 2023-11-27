use crate::index::IndexOptions;
use crate::ipc::packet::*;
use crate::ipc::transport::Socket;
use crate::ipc::IpcError;
use crate::prelude::*;

pub struct Rpc {
    socket: Socket,
}

impl Rpc {
    pub(super) fn new(socket: Socket) -> Self {
        Self { socket }
    }
    pub fn create(&mut self, id: Id, options: IndexOptions) -> Result<(), IpcError> {
        let packet = RpcPacket::Create { id, options };
        self.socket.send(packet)?;
        let CreatePacket::Leave {} = self.socket.recv::<CreatePacket>()?;
        Ok(())
    }
    pub fn search(
        mut self,
        id: Id,
        search: (Vec<Scalar>, usize),
        prefilter: bool,
    ) -> Result<SearchHandler, IpcError> {
        let packet = RpcPacket::Search {
            id,
            search,
            prefilter,
        };
        self.socket.send(packet)?;
        Ok(SearchHandler {
            socket: self.socket,
        })
    }
    pub fn delete(mut self, id: Id) -> Result<DeleteHandler, IpcError> {
        let packet = RpcPacket::Delete { id };
        self.socket.send(packet)?;
        Ok(DeleteHandler {
            socket: self.socket,
        })
    }
    pub fn insert(
        &mut self,
        id: Id,
        insert: (Vec<Scalar>, Pointer),
    ) -> Result<Result<(), FriendlyError>, IpcError> {
        let packet = RpcPacket::Insert { id, insert };
        self.socket.send(packet)?;
        let InsertPacket::Leave { result } = self.socket.recv::<InsertPacket>()?;
        Ok(result)
    }
    pub fn flush(&mut self, id: Id) -> Result<Result<(), FriendlyError>, IpcError> {
        let packet = RpcPacket::Flush { id };
        self.socket.send(packet)?;
        let FlushPacket::Leave { result } = self.socket.recv::<FlushPacket>()?;
        Ok(result)
    }
    pub fn destory(&mut self, ids: Vec<Id>) -> Result<(), IpcError> {
        let packet = RpcPacket::Destory { ids };
        self.socket.send(packet)?;
        let DestoryPacket::Leave {} = self.socket.recv::<DestoryPacket>()?;
        Ok(())
    }
    pub fn stat(&mut self, id: Id) -> Result<Result<VectorIndexInfo, FriendlyError>, IpcError> {
        let packet = RpcPacket::Stat { id };
        self.socket.send(packet)?;
        let StatPacket::Leave { result } = self.socket.recv::<StatPacket>()?;
        Ok(result)
    }
}

pub enum SearchHandle {
    Check {
        p: Pointer,
        x: SearchCheck,
    },
    Leave {
        result: Result<Vec<Pointer>, FriendlyError>,
        x: Rpc,
    },
}

pub struct SearchHandler {
    socket: Socket,
}

impl SearchHandler {
    pub fn handle(mut self) -> Result<SearchHandle, IpcError> {
        Ok(match self.socket.recv::<SearchPacket>()? {
            SearchPacket::Check { p } => SearchHandle::Check {
                p,
                x: SearchCheck {
                    socket: self.socket,
                },
            },
            SearchPacket::Leave { result } => SearchHandle::Leave {
                result,
                x: Rpc {
                    socket: self.socket,
                },
            },
        })
    }
}

pub struct SearchCheck {
    socket: Socket,
}

impl SearchCheck {
    pub fn leave(mut self, result: bool) -> Result<SearchHandler, IpcError> {
        let packet = SearchCheckPacket::Leave { result };
        self.socket.send(packet)?;
        Ok(SearchHandler {
            socket: self.socket,
        })
    }
}

pub enum DeleteHandle {
    Next {
        p: Pointer,
        x: DeleteNext,
    },
    Leave {
        result: Result<(), FriendlyError>,
        x: Rpc,
    },
}

pub struct DeleteHandler {
    socket: Socket,
}

impl DeleteHandler {
    pub fn handle(mut self) -> Result<DeleteHandle, IpcError> {
        Ok(match self.socket.recv::<DeletePacket>()? {
            DeletePacket::Next { p } => DeleteHandle::Next {
                p,
                x: DeleteNext {
                    socket: self.socket,
                },
            },
            DeletePacket::Leave { result } => DeleteHandle::Leave {
                result,
                x: Rpc {
                    socket: self.socket,
                },
            },
        })
    }
}

pub struct DeleteNext {
    socket: Socket,
}

impl DeleteNext {
    pub fn leave(mut self, delete: bool) -> Result<DeleteHandler, IpcError> {
        let packet = DeleteNextPacket::Leave { delete };
        self.socket.send(packet)?;
        Ok(DeleteHandler {
            socket: self.socket,
        })
    }
}
