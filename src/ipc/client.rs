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
    pub fn delete(&mut self, id: Id, delete: Pointer) -> Result<(), ClientIpcError> {
        let packet = RpcPacket::Delete { id, delete };
        self.socket.client_send(packet)?;
        let DeletePacket::Leave {} = self.socket.client_recv::<DeletePacket>()?;
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

impl BuildHandler {
    pub fn handle(mut self) -> Result<BuildHandle, ClientIpcError> {
        if !self.reach {
            Ok(BuildHandle::Next {
                x: Next {
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

pub enum BuildHandle {
    Next { x: Next },
    Leave { x: Rpc },
}

pub struct Next {
    socket: Socket,
}

impl Next {
    pub fn leave(
        mut self,
        data: Option<(Box<[Scalar]>, Pointer)>,
    ) -> Result<BuildHandler, ClientIpcError> {
        let end = data.is_none();
        let packet = NextPacket::Leave { data };
        self.socket.client_send(packet)?;
        Ok(BuildHandler {
            socket: self.socket,
            reach: end,
        })
    }
}

pub enum SearchHandle {
    Check { p: Pointer, x: Check },
    Leave { result: Vec<Pointer>, x: Rpc },
}

pub struct SearchHandler {
    socket: Socket,
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
