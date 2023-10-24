use crate::bgworker::index::IndexOptions;
use crate::ipc::packet::*;
use crate::ipc::transport::Socket;
use crate::ipc::ClientIpcError;
use crate::prelude::*;

pub struct Rpc {
    socket: Socket,
}

impl Rpc {
    pub(super) fn new(socket: Socket) -> Self {
        Self { socket }
    }
    pub fn build(mut self, id: Id, options: IndexOptions) -> Result<BuildHandler, ClientIpcError> {
        let packet = RpcPacket::Build { id, options };
        self.socket.client_send(packet)?;
        Ok(BuildHandler {
            socket: self.socket,
            reach: false,
        })
    }
    pub fn search(
        mut self,
        id: Id,
        target: Box<[Scalar]>,
        k: usize,
    ) -> Result<SearchHandler, ClientIpcError> {
        let packet = RpcPacket::Search { id, target, k };
        self.socket.client_send(packet)?;
        Ok(SearchHandler {
            socket: self.socket,
        })
    }
    pub fn delete(mut self, id: Id) -> Result<DeleteHandler, ClientIpcError> {
        let packet = RpcPacket::Delete { id };
        self.socket.client_send(packet)?;
        Ok(DeleteHandler {
            socket: self.socket,
        })
    }
    pub fn insert(
        &mut self,
        id: Id,
        insert: (Box<[Scalar]>, Pointer),
    ) -> Result<(), ClientIpcError> {
        let packet = RpcPacket::Insert { id, insert };
        self.socket.client_send(packet)?;
        let InsertPacket::Leave {} = self.socket.client_recv::<InsertPacket>()?;
        Ok(())
    }
    pub fn load(&mut self, id: Id) -> Result<(), ClientIpcError> {
        let packet = RpcPacket::Load { id };
        self.socket.client_send(packet)?;
        let LoadPacket::Leave {} = self.socket.client_recv::<LoadPacket>()?;
        Ok(())
    }
    pub fn unload(&mut self, id: Id) -> Result<(), ClientIpcError> {
        let packet = RpcPacket::Unload { id };
        self.socket.client_send(packet)?;
        let UnloadPacket::Leave {} = self.socket.client_recv::<UnloadPacket>()?;
        Ok(())
    }
    pub fn flush(&mut self, id: Id) -> Result<(), ClientIpcError> {
        let packet = RpcPacket::Flush { id };
        self.socket.client_send(packet)?;
        let FlushPacket::Leave {} = self.socket.client_recv::<FlushPacket>()?;
        Ok(())
    }
    pub fn clean(&mut self, id: Id) -> Result<(), ClientIpcError> {
        let packet = RpcPacket::Clean { id };
        self.socket.client_send(packet)?;
        let CleanPacket::Leave {} = self.socket.client_recv::<CleanPacket>()?;
        Ok(())
    }
}

pub struct BuildHandler {
    reach: bool,
    socket: Socket,
}

pub enum BuildHandle {
    Next { x: BuildNext },
    Leave { x: Rpc },
}

impl BuildHandler {
    pub fn handle(mut self) -> Result<BuildHandle, ClientIpcError> {
        if !self.reach {
            Ok(BuildHandle::Next {
                x: BuildNext {
                    socket: self.socket,
                },
            })
        } else {
            Ok(match self.socket.client_recv::<BuildPacket>()? {
                BuildPacket::Leave {} => BuildHandle::Leave {
                    x: Rpc {
                        socket: self.socket,
                    },
                },
                _ => unreachable!(),
            })
        }
    }
}

pub struct BuildNext {
    socket: Socket,
}

impl BuildNext {
    pub fn leave(
        mut self,
        data: Option<(Box<[Scalar]>, Pointer)>,
    ) -> Result<BuildHandler, ClientIpcError> {
        let end = data.is_none();
        let packet = BuildNextPacket::Leave { data };
        self.socket.client_send(packet)?;
        Ok(BuildHandler {
            socket: self.socket,
            reach: end,
        })
    }
}

pub struct SearchHandler {
    socket: Socket,
}

pub enum SearchHandle {
    Check { p: Pointer, x: Check },
    Leave { result: Vec<Pointer>, x: Rpc },
}

impl SearchHandler {
    pub fn handle(mut self) -> Result<SearchHandle, ClientIpcError> {
        Ok(match self.socket.client_recv::<SearchPacket>()? {
            SearchPacket::Check { p } => SearchHandle::Check {
                p,
                x: Check {
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

pub struct Check {
    socket: Socket,
}

impl Check {
    pub fn leave(mut self, result: bool) -> Result<SearchHandler, ClientIpcError> {
        let packet = CheckPacket::Leave { result };
        self.socket.client_send(packet)?;
        Ok(SearchHandler {
            socket: self.socket,
        })
    }
}

pub struct DeleteHandler {
    socket: Socket,
}

pub enum DeleteHandle {
    Next { p: Pointer, x: DeleteNext },
    Leave { x: Rpc },
}

impl DeleteHandler {
    pub fn handle(mut self) -> Result<DeleteHandle, ClientIpcError> {
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
    pub fn leave(mut self, delete: bool) -> Result<DeleteHandler, ClientIpcError> {
        let packet = DeleteNextPacket::Leave { delete };
        self.socket.client_send(packet)?;
        Ok(DeleteHandler {
            socket: self.socket,
        })
    }
}
