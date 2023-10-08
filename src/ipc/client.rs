use std::error::Error;

use crate::bgworker::index::IndexOptions;
use crate::ipc::packet::*;
use crate::prelude::*;

use super::{Channel, ChannelWithSerialize};

pub struct Rpc {
    channel: Channel,
}

impl Rpc {
    pub(super) fn new(channel: Channel) -> Self {
        Self { channel }
    }
    pub fn build(mut self, id: Id, options: IndexOptions) -> Result<BuildHandler, Box<dyn Error>> {
        let packet = RpcPacket::Build { id, options };
        self.channel.send(packet)?;
        Ok(BuildHandler {
            socket: self.channel,
            reach: false,
        })
    }
    pub fn search(
        mut self,
        id: Id,
        target: Box<[Scalar]>,
        k: usize,
    ) -> Result<SearchHandler, Box<dyn Error>> {
        let packet = RpcPacket::Search { id, target, k };
        self.channel.send(packet)?;
        Ok(SearchHandler {
            channel: self.channel,
        })
    }
    pub fn insert(
        &mut self,
        id: Id,
        insert: (Box<[Scalar]>, Pointer),
    ) -> Result<(), Box<dyn Error>> {
        let packet = RpcPacket::Insert { id, insert };
        self.channel.send(packet)?;
        let InsertPacket::Leave {} = self.channel.recv::<InsertPacket>()?;
        Ok(())
    }
    pub fn delete(&mut self, id: Id, delete: Pointer) -> Result<(), Box<dyn Error>> {
        let packet = RpcPacket::Delete { id, delete };
        self.channel.send(packet)?;
        let DeletePacket::Leave {} = self.channel.recv::<DeletePacket>()?;
        Ok(())
    }
    pub fn load(&mut self, id: Id) -> Result<(), Box<dyn Error>> {
        let packet = RpcPacket::Load { id };
        self.channel.send(packet)?;
        let LoadPacket::Leave {} = self.channel.recv::<LoadPacket>()?;
        Ok(())
    }
    pub fn unload(&mut self, id: Id) -> Result<(), Box<dyn Error>> {
        let packet = RpcPacket::Unload { id };
        self.channel.send(packet)?;
        let UnloadPacket::Leave {} = self.channel.recv::<UnloadPacket>()?;
        Ok(())
    }
    pub fn flush(&mut self, id: Id) -> Result<(), Box<dyn Error>> {
        let packet = RpcPacket::Flush { id };
        self.channel.send(packet)?;
        let FlushPacket::Leave {} = self.channel.recv::<FlushPacket>()?;
        Ok(())
    }
    pub fn clean(&mut self, id: Id) -> Result<(), Box<dyn Error>> {
        let packet = RpcPacket::Clean { id };
        self.channel.send(packet)?;
        let CleanPacket::Leave {} = self.channel.recv::<CleanPacket>()?;
        Ok(())
    }
}

pub struct BuildHandler {
    reach: bool,
    socket: Channel,
}

impl BuildHandler {
    pub fn handle(mut self) -> Result<BuildHandle, Box<dyn Error>> {
        if !self.reach {
            Ok(BuildHandle::Next {
                x: Next {
                    channel: self.socket,
                },
            })
        } else {
            Ok(match self.socket.recv::<BuildPacket>()? {
                BuildPacket::Leave {} => BuildHandle::Leave {
                    x: Rpc {
                        channel: self.socket,
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
    channel: Channel,
}

impl Next {
    pub fn leave(
        mut self,
        data: Option<(Box<[Scalar]>, Pointer)>,
    ) -> Result<BuildHandler, Box<dyn Error>> {
        let end = data.is_none();
        let packet = NextPacket::Leave { data };
        self.channel.send(packet)?;
        Ok(BuildHandler {
            socket: self.channel,
            reach: end,
        })
    }
}

pub enum SearchHandle {
    Check { p: Pointer, x: Check },
    Leave { result: Vec<Pointer>, x: Rpc },
}

pub struct SearchHandler {
    channel: Channel,
}

impl SearchHandler {
    pub fn handle(mut self) -> Result<SearchHandle, Box<dyn Error>> {
        Ok(match self.channel.recv::<SearchPacket>()? {
            SearchPacket::Check { p } => SearchHandle::Check {
                p,
                x: Check {
                    channel: self.channel,
                },
            },
            SearchPacket::Leave { result } => SearchHandle::Leave {
                result,
                x: Rpc {
                    channel: self.channel,
                },
            },
        })
    }
}

pub struct Check {
    channel: Channel,
}

impl Check {
    pub fn leave(mut self, result: bool) -> Result<SearchHandler, Box<dyn Error>> {
        let packet = CheckPacket::Leave { result };
        self.channel.send(packet)?;
        Ok(SearchHandler {
            channel: self.channel,
        })
    }
}
