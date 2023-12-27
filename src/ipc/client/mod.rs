use super::packet::*;
use super::transport::Socket;
use crate::gucs::{Transport, TRANSPORT};
use crate::utils::cells::PgRefCell;
use service::index::IndexOptions;
use service::index::IndexStat;
use service::prelude::*;
use std::mem::ManuallyDrop;
use std::ops::Deref;
use std::ops::DerefMut;

pub trait ClientLike: 'static {
    const RESET: bool = false;

    fn from_socket(socket: Socket) -> Self;
    fn to_socket(self) -> Socket;
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
    socket: Socket,
}

impl Rpc {
    pub fn new(socket: Socket) -> Self {
        Self { socket }
    }
    pub fn create(self: &mut ClientGuard<Self>, id: Id, options: IndexOptions) {
        let packet = RpcPacket::Create { id, options };
        self.socket.client_send(packet).friendly();
        let create::CreatePacket::Leave {} = self.socket.client_recv().friendly();
    }
    pub fn search(
        self: &mut ClientGuard<Self>,
        id: Id,
        search: (DynamicVector, usize),
        prefilter: bool,
        mut t: impl Search,
    ) -> Vec<Pointer> {
        let packet = RpcPacket::Search {
            id,
            search,
            prefilter,
        };
        self.socket.client_send(packet).friendly();
        loop {
            match self.socket.client_recv().friendly() {
                search::SearchPacket::Check { p } => {
                    self.socket
                        .client_send(search::SearchCheckPacket { result: t.check(p) })
                        .friendly();
                }
                search::SearchPacket::Leave { result } => {
                    return result.friendly();
                }
            }
        }
    }
    pub fn delete(self: &mut ClientGuard<Self>, id: Id, mut t: impl Delete) {
        let packet = RpcPacket::Delete { id };
        self.socket.client_send(packet).friendly();
        loop {
            match self.socket.client_recv().friendly() {
                delete::DeletePacket::Test { p } => {
                    self.socket
                        .client_send(delete::DeleteTestPacket { delete: t.test(p) })
                        .friendly();
                }
                delete::DeletePacket::Leave { result } => {
                    return result.friendly();
                }
            }
        }
    }
    pub fn insert(self: &mut ClientGuard<Self>, id: Id, insert: (DynamicVector, Pointer)) {
        let packet = RpcPacket::Insert { id, insert };
        self.socket.client_send(packet).friendly();
        let insert::InsertPacket::Leave { result } = self.socket.client_recv().friendly();
        result.friendly()
    }
    pub fn flush(self: &mut ClientGuard<Self>, id: Id) {
        let packet = RpcPacket::Flush { id };
        self.socket.client_send(packet).friendly();
        let flush::FlushPacket::Leave { result } = self.socket.client_recv().friendly();
        result.friendly()
    }
    pub fn destory(self: &mut ClientGuard<Self>, ids: Vec<Id>) {
        let packet = RpcPacket::Destory { ids };
        self.socket.client_send(packet).friendly();
        let destory::DestoryPacket::Leave {} = self.socket.client_recv().friendly();
    }
    pub fn stat(self: &mut ClientGuard<Self>, id: Id) -> IndexStat {
        let packet = RpcPacket::Stat { id };
        self.socket.client_send(packet).friendly();
        let stat::StatPacket::Leave { result } = self.socket.client_recv().friendly();
        result.friendly()
    }
    pub fn vbase(
        mut self: ClientGuard<Self>,
        id: Id,
        vbase: (DynamicVector, usize),
    ) -> ClientGuard<Vbase> {
        let packet = RpcPacket::Vbase { id, vbase };
        self.socket.client_send(packet).friendly();
        let vbase::VbaseErrorPacket { result } = self.socket.client_recv().friendly();
        result.friendly();
        ClientGuard::map(self)
    }
}

impl ClientLike for Rpc {
    const RESET: bool = true;

    fn from_socket(socket: Socket) -> Self {
        Self { socket }
    }

    fn to_socket(self) -> Socket {
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
    socket: Socket,
}

impl Vbase {
    pub fn next(self: &mut ClientGuard<Self>) -> Option<Pointer> {
        let packet = vbase::VbasePacket::Next {};
        self.socket.client_send(packet).friendly();
        let vbase::VbaseNextPacket { p } = self.socket.client_recv().friendly();
        p
    }
    pub fn leave(mut self: ClientGuard<Self>) -> ClientGuard<Rpc> {
        let packet = vbase::VbasePacket::Leave {};
        self.socket.client_send(packet).friendly();
        let vbase::VbaseLeavePacket {} = self.socket.client_recv().friendly();
        ClientGuard::map(self)
    }
}

impl ClientLike for Vbase {
    fn from_socket(socket: Socket) -> Self {
        Self { socket }
    }

    fn to_socket(self) -> Socket {
        self.socket
    }
}

enum Status {
    Borrowed,
    Lost,
    Reset(Socket),
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
                if T::RESET {
                    unsafe {
                        *x = Status::Reset(ManuallyDrop::take(&mut self.0).to_socket());
                    }
                } else {
                    *x = Status::Lost;
                }
            }
            Status::Lost => unreachable!(),
            Status::Reset(_) => unreachable!(),
        }
    }
}
