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
        let packet = ClientPacket::Create { id, options };
        self.socket.send(packet).friendly();
        let create::ServerPacket::Leave {} = self.socket.recv::<create::ServerPacket>().friendly();
    }
    pub fn search(
        &mut self,
        id: Id,
        search: (DynamicVector, usize),
        prefilter: bool,
        mut t: impl ClientSearch,
    ) -> Vec<Pointer> {
        let packet = ClientPacket::Search {
            id,
            search,
            prefilter,
        };
        self.socket.send(packet).friendly();
        loop {
            match self.socket.recv::<search::ServerPacket>().friendly() {
                search::ServerPacket::Check { p } => {
                    self.socket
                        .send(search::ClientCheckPacket { result: t.check(p) })
                        .friendly();
                }
                search::ServerPacket::Leave { result } => {
                    return result.friendly();
                }
            }
        }
    }
    pub fn delete(&mut self, id: Id, mut t: impl ClientDelete) {
        let packet = ClientPacket::Delete { id };
        self.socket.send(packet).friendly();
        loop {
            match self.socket.recv::<delete::ServerPacket>().friendly() {
                delete::ServerPacket::Test { p } => {
                    self.socket
                        .send(delete::ClientTestPacket { delete: t.test(p) })
                        .friendly();
                }
                delete::ServerPacket::Leave { result } => {
                    return result.friendly();
                }
            }
        }
    }
    pub fn insert(&mut self, id: Id, insert: (DynamicVector, Pointer)) {
        let packet = ClientPacket::Insert { id, insert };
        self.socket.send(packet).friendly();
        let insert::ServerPacket::Leave { result } =
            self.socket.recv::<insert::ServerPacket>().friendly();
        result.friendly()
    }
    pub fn flush(&mut self, id: Id) {
        let packet = ClientPacket::Flush { id };
        self.socket.send(packet).friendly();
        let flush::ServerPacket::Leave { result } =
            self.socket.recv::<flush::ServerPacket>().friendly();
        result.friendly()
    }
    pub fn destory(&mut self, ids: Vec<Id>) {
        let packet = ClientPacket::Destory { ids };
        self.socket.send(packet).friendly();
        let destory::ServerPacket::Leave {} =
            self.socket.recv::<destory::ServerPacket>().friendly();
    }
    pub fn stat(&mut self, id: Id) -> IndexStat {
        let packet = ClientPacket::Stat { id };
        self.socket.send(packet).friendly();
        let stat::ServerPacket::Leave { result } =
            self.socket.recv::<stat::ServerPacket>().friendly();
        result.friendly()
    }
    pub fn vbase(&mut self, id: Id, search: (DynamicVector, usize)) -> ClientVbase<'_> {
        let packet = ClientPacket::Vbase { id, search };
        self.socket.send(packet).friendly();
        let vbase::ServerPacket::Leave {} = self.socket.recv::<vbase::ServerPacket>().friendly();
        ClientVbase(self)
    }
}

pub trait ClientSearch {
    fn check(&mut self, p: Pointer) -> bool;
}

pub trait ClientDelete {
    fn test(&mut self, p: Pointer) -> bool;
}

pub struct ClientVbase<'a>(&'a mut Client);

impl ClientVbase<'_> {
    pub fn next(&mut self) -> Pointer {
        let packet = vbase::ClientPacket::Next {};
        self.0.socket.send(packet).friendly();
        let vbase::ServerNextPacket { p } =
            self.0.socket.recv::<vbase::ServerNextPacket>().friendly();
        p
    }
    pub fn leave(self) {
        let packet = vbase::ClientPacket::Leave {};
        self.0.socket.send(packet).friendly();
        let vbase::ServerLeavePacket {} =
            self.0.socket.recv::<vbase::ServerLeavePacket>().friendly();
    }
}
