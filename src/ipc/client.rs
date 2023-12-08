use super::packet::*;
use super::transport::Socket;
use service::index::IndexOptions;
use service::index::IndexStat;
use service::prelude::*;

pub struct Client {
    socket: Socket,
}

impl Client {
    pub fn new(socket: Socket) -> Self {
        Self { socket }
    }
    pub fn create(&mut self, id: Id, options: IndexOptions) {
        let packet = RpcPacket::Create { id, options };
        self.socket.send(packet).friendly();
        let CreatePacket::Leave {} = self.socket.recv::<CreatePacket>().friendly();
    }
    pub fn search(
        &mut self,
        id: Id,
        search: (DynamicVector, usize),
        prefilter: bool,
        mut t: impl ClientSearch,
    ) -> Vec<Pointer> {
        let packet = RpcPacket::Search {
            id,
            search,
            prefilter,
        };
        self.socket.send(packet).friendly();
        loop {
            match self.socket.recv::<SearchPacket>().friendly() {
                SearchPacket::Check { p } => {
                    self.socket
                        .send(SearchCheckPacket::Leave { result: t.check(p) })
                        .friendly();
                }
                SearchPacket::Leave { result } => {
                    return result.friendly();
                }
            }
        }
    }
    pub fn delete(&mut self, id: Id, mut t: impl ClientDelete) {
        let packet = RpcPacket::Delete { id };
        self.socket.send(packet).friendly();
        loop {
            match self.socket.recv::<DeletePacket>().friendly() {
                DeletePacket::Test { p } => {
                    self.socket
                        .send(DeleteTestPacket::Leave { delete: t.test(p) })
                        .friendly();
                }
                DeletePacket::Leave { result } => {
                    return result.friendly();
                }
            }
        }
    }
    pub fn insert(&mut self, id: Id, insert: (DynamicVector, Pointer)) {
        let packet = RpcPacket::Insert { id, insert };
        self.socket.send(packet).friendly();
        let InsertPacket::Leave { result } = self.socket.recv::<InsertPacket>().friendly();
        result.friendly()
    }
    pub fn flush(&mut self, id: Id) {
        let packet = RpcPacket::Flush { id };
        self.socket.send(packet).friendly();
        let FlushPacket::Leave { result } = self.socket.recv::<FlushPacket>().friendly();
        result.friendly()
    }
    pub fn destory(&mut self, ids: Vec<Id>) {
        let packet = RpcPacket::Destory { ids };
        self.socket.send(packet).friendly();
        let DestoryPacket::Leave {} = self.socket.recv::<DestoryPacket>().friendly();
    }
    pub fn stat(&mut self, id: Id) -> IndexStat {
        let packet = RpcPacket::Stat { id };
        self.socket.send(packet).friendly();
        let StatPacket::Leave { result } = self.socket.recv::<StatPacket>().friendly();
        result.friendly()
    }
}

pub trait ClientSearch {
    fn check(&mut self, p: Pointer) -> bool;
}

pub trait ClientDelete {
    fn test(&mut self, p: Pointer) -> bool;
}
