use super::packet::*;
use super::transport::ClientSocket;
use crate::gucs::internal::{Transport, TRANSPORT};
use crate::prelude::*;
use crate::utils::cells::PgRefCell;
use service::index::IndexOptions;
use service::index::IndexStat;
use service::index::SearchOptions;
use service::prelude::*;
use std::mem::ManuallyDrop;
use std::ops::Deref;
use std::ops::DerefMut;

pub trait ClientLike: 'static {
    fn from_socket(socket: ClientSocket) -> Self;
    fn to_socket(self) -> ClientSocket;
}

pub struct ClientGuard<T: ClientLike>(pub ManuallyDrop<T>);

impl<T: ClientLike> ClientGuard<T> {
    fn map<U: ClientLike>(mut self) -> ClientGuard<U> {
        unsafe {
            let t = ManuallyDrop::take(&mut self.0);
            std::mem::forget(self);
            ClientGuard::new(U::from_socket(t.to_socket()))
        }
    }
}

impl<T: ClientLike> Deref for ClientGuard<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: ClientLike> DerefMut for ClientGuard<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

pub struct Rpc {
    socket: ClientSocket,
}

impl Rpc {
    pub fn new(socket: ClientSocket) -> Self {
        Self { socket }
    }
}

impl ClientGuard<Rpc> {
    pub fn commit(&mut self, pending_deletes: Vec<Handle>, pending_dirty: Vec<Handle>) {
        let packet = RpcPacket::Commit {
            pending_deletes,
            pending_dirty,
        };
        self.socket.ok(packet).friendly();
        let commit::CommitPacket::Leave {} = self.socket.recv().friendly();
    }
    pub fn abort(&mut self, pending_deletes: Vec<Handle>) {
        let packet = RpcPacket::Abort { pending_deletes };
        self.socket.ok(packet).friendly();
        let abort::AbortPacket::Leave {} = self.socket.recv().friendly();
    }
    pub fn create(&mut self, handle: Handle, options: IndexOptions) {
        let packet = RpcPacket::Create { handle, options };
        self.socket.ok(packet).friendly();
        let create::CreatePacket::Leave {} = self.socket.recv().friendly();
    }
    pub fn basic(
        mut self,
        handle: Handle,
        vector: DynamicVector,
        opts: SearchOptions,
    ) -> ClientGuard<Basic> {
        let packet = RpcPacket::Basic {
            handle,
            vector,
            opts,
        };
        self.socket.ok(packet).friendly();
        let vbase::VbaseErrorPacket {} = self.socket.recv().friendly();
        ClientGuard::map(self)
    }
    pub fn delete(&mut self, handle: Handle, mut t: impl Delete) {
        let packet = RpcPacket::Delete { handle };
        self.socket.ok(packet).friendly();
        loop {
            match self.socket.recv().friendly() {
                delete::DeletePacket::Test { p } => {
                    self.socket
                        .ok(delete::DeleteTestPacket { delete: t.test(p) })
                        .friendly();
                }
                delete::DeletePacket::Leave {} => {
                    return;
                }
            }
        }
    }
    pub fn insert(&mut self, handle: Handle, vector: DynamicVector, pointer: Pointer) {
        let packet = RpcPacket::Insert {
            handle,
            vector,
            pointer,
        };
        self.socket.ok(packet).friendly();
        let insert::InsertPacket::Leave {} = self.socket.recv().friendly();
    }
    pub fn stat(&mut self, handle: Handle) -> IndexStat {
        let packet = RpcPacket::Stat { handle };
        self.socket.ok(packet).friendly();
        let stat::StatPacket::Leave { result } = self.socket.recv().friendly();
        result
    }
    pub fn vbase(
        mut self,
        handle: Handle,
        vector: DynamicVector,
        opts: SearchOptions,
    ) -> ClientGuard<Vbase> {
        let packet = RpcPacket::Vbase {
            handle,
            vector,
            opts,
        };
        self.socket.ok(packet).friendly();
        let vbase::VbaseErrorPacket {} = self.socket.recv().friendly();
        ClientGuard::map(self)
    }
}

impl ClientLike for Rpc {
    fn from_socket(socket: ClientSocket) -> Self {
        Self { socket }
    }

    fn to_socket(self) -> ClientSocket {
        self.socket
    }
}

pub trait Delete {
    fn test(&mut self, p: Pointer) -> bool;
}

pub struct Vbase {
    socket: ClientSocket,
}

impl Vbase {
    pub fn next(&mut self) -> Option<Pointer> {
        let packet = vbase::VbasePacket::Next {};
        self.socket.ok(packet).friendly();
        let vbase::VbaseNextPacket { p } = self.socket.recv().friendly();
        p
    }
}

impl ClientGuard<Vbase> {
    pub fn leave(mut self) -> ClientGuard<Rpc> {
        let packet = vbase::VbasePacket::Leave {};
        self.socket.ok(packet).friendly();
        let vbase::VbaseLeavePacket {} = self.socket.recv().friendly();
        ClientGuard::map(self)
    }
}

impl ClientLike for Vbase {
    fn from_socket(socket: ClientSocket) -> Self {
        Self { socket }
    }

    fn to_socket(self) -> ClientSocket {
        self.socket
    }
}

pub struct Basic {
    socket: ClientSocket,
}

impl Basic {
    pub fn next(&mut self) -> Option<Pointer> {
        let packet = basic::BasicPacket::Next {};
        self.socket.ok(packet).friendly();
        let basic::BasicNextPacket { p } = self.socket.recv().friendly();
        p
    }
}

impl ClientGuard<Basic> {
    pub fn leave(mut self) -> ClientGuard<Rpc> {
        let packet = basic::BasicPacket::Leave {};
        self.socket.ok(packet).friendly();
        let basic::BasicLeavePacket {} = self.socket.recv().friendly();
        ClientGuard::map(self)
    }
}

impl ClientLike for Basic {
    fn from_socket(socket: ClientSocket) -> Self {
        Self { socket }
    }

    fn to_socket(self) -> ClientSocket {
        self.socket
    }
}

static CLIENTS: PgRefCell<Vec<ClientSocket>> = unsafe { PgRefCell::new(Vec::new()) };

pub fn borrow_mut() -> ClientGuard<Rpc> {
    let mut x = CLIENTS.borrow_mut();
    if let Some(socket) = x.pop() {
        return ClientGuard::new(Rpc::new(socket));
    }
    let socket = match TRANSPORT.get() {
        Transport::unix => crate::ipc::connect_unix(),
        Transport::mmap => crate::ipc::connect_mmap(),
    };
    ClientGuard::new(Rpc::new(socket))
}

impl<T: ClientLike> ClientGuard<T> {
    pub fn new(t: T) -> Self {
        Self(ManuallyDrop::new(t))
    }
}

impl<T: ClientLike> Drop for ClientGuard<T> {
    fn drop(&mut self) {
        let socket = unsafe { ManuallyDrop::take(&mut self.0).to_socket() };
        if !std::thread::panicking() && std::any::TypeId::of::<T>() == std::any::TypeId::of::<Rpc>()
        {
            let mut x = CLIENTS.borrow_mut();
            x.push(socket);
        }
    }
}
