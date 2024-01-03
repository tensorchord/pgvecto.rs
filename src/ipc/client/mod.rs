use super::packet::*;
use super::transport::ClientSocket;
use crate::gucs::{Transport, TRANSPORT};
use crate::utils::cells::PgRefCell;
use service::index::segments::SearchGucs;
use service::index::IndexOptions;
use service::index::IndexStat;
use service::prelude::*;
use std::mem::ManuallyDrop;
use std::ops::Deref;
use std::ops::DerefMut;

pub trait ClientLike: 'static {
    const RESET: bool = false;

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
    pub fn create(&mut self, handle: Handle, options: IndexOptions) {
        let packet = RpcPacket::Create { handle, options };
        self.socket.send(packet).friendly();
        let create::CreatePacket::Leave {} = self.socket.recv().friendly();
    }
    pub fn search(
        &mut self,
        handle: Handle,
        search: (DynamicVector, usize),
        prefilter: bool,
        gucs: SearchGucs,
        mut t: impl Search,
    ) -> Vec<Pointer> {
        let packet = RpcPacket::Search {
            handle,
            search,
            prefilter,
            gucs,
        };
        self.socket.send(packet).friendly();
        loop {
            match self.socket.recv().friendly() {
                search::SearchPacket::Check { p } => {
                    self.socket
                        .send(search::SearchCheckPacket { result: t.check(p) })
                        .friendly();
                }
                search::SearchPacket::Leave { result } => {
                    return result;
                }
            }
        }
    }
    pub fn delete(&mut self, handle: Handle, mut t: impl Delete) {
        let packet = RpcPacket::Delete { handle };
        self.socket.send(packet).friendly();
        loop {
            match self.socket.recv().friendly() {
                delete::DeletePacket::Test { p } => {
                    self.socket
                        .send(delete::DeleteTestPacket { delete: t.test(p) })
                        .friendly();
                }
                delete::DeletePacket::Leave {} => {
                    return;
                }
            }
        }
    }
    pub fn insert(&mut self, handle: Handle, insert: (DynamicVector, Pointer)) {
        let packet = RpcPacket::Insert { handle, insert };
        self.socket.send(packet).friendly();
        let insert::InsertPacket::Leave {} = self.socket.recv().friendly();
    }
    pub fn flush(&mut self, handle: Handle) {
        let packet = RpcPacket::Flush { handle };
        self.socket.send(packet).friendly();
        let flush::FlushPacket::Leave {} = self.socket.recv().friendly();
    }
    pub fn destory(&mut self, handle: Handle) {
        let packet = RpcPacket::Destory { handle };
        self.socket.send(packet).friendly();
        let destory::DestoryPacket::Leave {} = self.socket.recv().friendly();
    }
    pub fn stat(&mut self, handle: Handle) -> IndexStat {
        let packet = RpcPacket::Stat { handle };
        self.socket.send(packet).friendly();
        let stat::StatPacket::Leave { result } = self.socket.recv().friendly();
        result
    }
    pub fn vbase(mut self, handle: Handle, vbase: (DynamicVector, usize)) -> ClientGuard<Vbase> {
        let packet = RpcPacket::Vbase { handle, vbase };
        self.socket.send(packet).friendly();
        let vbase::VbaseErrorPacket {} = self.socket.recv().friendly();
        ClientGuard::map(self)
    }
}

impl ClientLike for Rpc {
    const RESET: bool = true;

    fn from_socket(socket: ClientSocket) -> Self {
        Self { socket }
    }

    fn to_socket(self) -> ClientSocket {
        self.socket
    }
}

pub trait Search {
    fn check(&mut self, p: Pointer) -> bool;
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
        self.socket.send(packet).friendly();
        let vbase::VbaseNextPacket { p } = self.socket.recv().friendly();
        p
    }
}

impl ClientGuard<Vbase> {
    pub fn leave(mut self) -> ClientGuard<Rpc> {
        let packet = vbase::VbasePacket::Leave {};
        self.socket.send(packet).friendly();
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

enum Status {
    Borrowed,
    Lost,
    Reset(ClientSocket),
}

static CLIENT: PgRefCell<Status> = unsafe { PgRefCell::new(Status::Lost) };

pub fn borrow_mut() -> ClientGuard<Rpc> {
    let mut x = CLIENT.borrow_mut();
    match &mut *x {
        Status::Borrowed => {
            panic!("borrowed when borrowed");
        }
        Status::Lost => {
            let socket = match TRANSPORT.get() {
                Transport::unix => crate::ipc::connect_unix(),
                Transport::mmap => crate::ipc::connect_mmap(),
            };
            *x = Status::Borrowed;
            ClientGuard::new(Rpc::new(socket))
        }
        x @ Status::Reset(_) => {
            let Status::Reset(socket) = std::mem::replace(x, Status::Borrowed) else {
                unreachable!()
            };
            ClientGuard::new(Rpc::new(socket))
        }
    }
}

impl<T: ClientLike> ClientGuard<T> {
    pub fn new(t: T) -> Self {
        Self(ManuallyDrop::new(t))
    }
}

impl<T: ClientLike> Drop for ClientGuard<T> {
    fn drop(&mut self) {
        let mut x = CLIENT.borrow_mut();
        match *x {
            Status::Borrowed => {
                let socket = unsafe { ManuallyDrop::take(&mut self.0).to_socket() };
                if T::RESET && socket.test() {
                    *x = Status::Reset(socket);
                } else {
                    *x = Status::Lost;
                }
            }
            Status::Lost => unreachable!(),
            Status::Reset(_) => unreachable!(),
        }
    }
}
