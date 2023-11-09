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
        self.socket.client_send(packet)?;
        let CreatePacket::Leave {} = self.socket.client_recv::<CreatePacket>()?;
        Ok(())
    }
    pub fn search(
        mut self,
        id: Id,
        search: (Vec<Scalar>, usize),
        select: bool,
    ) -> Result<SearchHandler, IpcError> {
        let packet = RpcPacket::Search { id, search, select };
        self.socket.client_send(packet)?;
        Ok(SearchHandler {
            socket: self.socket,
        })
    }
    pub fn delete(mut self, id: Id) -> Result<DeleteHandler, IpcError> {
        let packet = RpcPacket::Delete { id };
        self.socket.client_send(packet)?;
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
        self.socket.client_send(packet)?;
        let InsertPacket::Leave { result } = self.socket.client_recv::<InsertPacket>()?;
        Ok(result)
    }
    pub fn flush(&mut self, id: Id) -> Result<(), IpcError> {
        let packet = RpcPacket::Flush { id };
        self.socket.client_send(packet)?;
        let FlushPacket::Leave {} = self.socket.client_recv::<FlushPacket>()?;
        Ok(())
    }
    pub fn destory(&mut self, id: Id) -> Result<(), IpcError> {
        let packet = RpcPacket::Destory { id };
        self.socket.client_send(packet)?;
        let DestoryPacket::Leave {} = self.socket.client_recv::<DestoryPacket>()?;
        Ok(())
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
        Ok(match self.socket.client_recv::<SearchPacket>()? {
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
        self.socket.client_send(packet)?;
        Ok(SearchHandler {
            socket: self.socket,
        })
    }
}

pub enum DeleteHandle {
    Next { p: Pointer, x: DeleteNext },
    Leave { x: Rpc },
}

pub struct DeleteHandler {
    socket: Socket,
}

impl DeleteHandler {
    pub fn handle(mut self) -> Result<DeleteHandle, IpcError> {
        Ok(match self.socket.client_recv::<DeletePacket>()? {
            DeletePacket::Next { p } => DeleteHandle::Next {
                p,
                x: DeleteNext {
                    socket: self.socket,
                },
            },
            DeletePacket::Leave {} => DeleteHandle::Leave {
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
        self.socket.client_send(packet)?;
        Ok(DeleteHandler {
            socket: self.socket,
        })
    }
}
