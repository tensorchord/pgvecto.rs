use std::error::Error;

use crate::bgworker::index::IndexOptions;
use crate::ipc::packet::*;
use crate::prelude::*;

use super::{Channel, ChannelWithSerialize};

pub struct RpcHandler {
    channel: Channel,
}

impl RpcHandler {
    pub(super) fn new(channel: Channel) -> Self {
        Self { channel }
    }
    pub fn handle(mut self) -> Result<RpcHandle, Box<dyn Error>> {
        Ok(match self.channel.recv::<RpcPacket>()? {
            RpcPacket::Build { id, options } => RpcHandle::Build {
                id,
                options,
                x: Build {
                    channel: self.channel,
                    reach: false,
                },
            },
            RpcPacket::Insert { id, insert } => RpcHandle::Insert {
                id,
                insert,
                x: Insert {
                    channel: self.channel,
                },
            },
            RpcPacket::Delete { id, delete } => RpcHandle::Delete {
                id,
                delete,
                x: Delete {
                    channel: self.channel,
                },
            },
            RpcPacket::Search { id, target, k } => RpcHandle::Search {
                id,
                target,
                k,
                x: Search {
                    channel: self.channel,
                },
            },
            RpcPacket::Load { id } => RpcHandle::Load {
                id,
                x: Load {
                    channel: self.channel,
                },
            },
            RpcPacket::Unload { id } => RpcHandle::Unload {
                id,
                x: Unload {
                    channel: self.channel,
                },
            },
            RpcPacket::Flush { id } => RpcHandle::Flush {
                id,
                x: Flush {
                    channel: self.channel,
                },
            },
            RpcPacket::Clean { id } => RpcHandle::Clean {
                id,
                x: Clean {
                    channel: self.channel,
                },
            },
            RpcPacket::Leave {} => RpcHandle::Leave {},
        })
    }
}

pub enum RpcHandle {
    Build {
        id: Id,
        options: IndexOptions,
        x: Build,
    },
    Search {
        id: Id,
        target: Box<[Scalar]>,
        k: usize,
        x: Search,
    },
    Insert {
        id: Id,
        insert: (Box<[Scalar]>, Pointer),
        x: Insert,
    },
    Delete {
        id: Id,
        delete: Pointer,
        x: Delete,
    },
    Load {
        id: Id,
        x: Load,
    },
    Unload {
        id: Id,
        x: Unload,
    },
    Flush {
        id: Id,
        x: Flush,
    },
    Clean {
        id: Id,
        x: Clean,
    },
    Leave {},
}

pub struct Build {
    channel: Channel,
    reach: bool,
}

impl Build {
    pub fn next(&mut self) -> Result<Option<(Box<[Scalar]>, Pointer)>, Box<dyn Error>> {
        if !self.reach {
            let packet = self.channel.recv::<NextPacket>()?;
            match packet {
                NextPacket::Leave { data: Some(data) } => Ok(Some(data)),
                NextPacket::Leave { data: None } => {
                    self.reach = true;
                    Ok(None)
                }
            }
        } else {
            Ok(None)
        }
    }
    pub fn leave(mut self) -> Result<RpcHandler, Box<dyn Error>> {
        let packet = BuildPacket::Leave {};
        self.channel.send(packet)?;
        Ok(RpcHandler {
            channel: self.channel,
        })
    }
}

pub struct Insert {
    channel: Channel,
}

impl Insert {
    pub fn leave(mut self) -> Result<RpcHandler, Box<dyn Error>> {
        let packet = InsertPacket::Leave {};
        self.channel.send(packet)?;
        Ok(RpcHandler {
            channel: self.channel,
        })
    }
}

pub struct Delete {
    channel: Channel,
}

impl Delete {
    pub fn leave(mut self) -> Result<RpcHandler, Box<dyn Error>> {
        let packet = DeletePacket::Leave {};
        self.channel.send(packet)?;
        Ok(RpcHandler {
            channel: self.channel,
        })
    }
}

pub struct Search {
    channel: Channel,
}

impl Search {
    pub fn check(&mut self, p: Pointer) -> Result<bool, Box<dyn Error>> {
        let packet = SearchPacket::Check { p };
        self.channel.send(packet)?;
        let CheckPacket::Leave { result } = self.channel.recv::<CheckPacket>()?;
        Ok(result)
    }
    pub fn leave(mut self, result: Vec<Pointer>) -> Result<RpcHandler, Box<dyn Error>> {
        let packet = SearchPacket::Leave { result };
        self.channel.send(packet)?;
        Ok(RpcHandler {
            channel: self.channel,
        })
    }
}

pub struct Load {
    channel: Channel,
}

impl Load {
    pub fn leave(mut self) -> Result<RpcHandler, Box<dyn Error>> {
        let packet = LoadPacket::Leave {};
        self.channel.send(packet)?;
        Ok(RpcHandler {
            channel: self.channel,
        })
    }
}

pub struct Unload {
    channel: Channel,
}

impl Unload {
    pub fn leave(mut self) -> Result<RpcHandler, Box<dyn Error>> {
        let packet = UnloadPacket::Leave {};
        self.channel.send(packet)?;
        Ok(RpcHandler {
            channel: self.channel,
        })
    }
}

pub struct Flush {
    channel: Channel,
}

impl Flush {
    pub fn leave(mut self) -> Result<RpcHandler, Box<dyn Error>> {
        let packet = FlushPacket::Leave {};
        self.channel.send(packet)?;
        Ok(RpcHandler {
            channel: self.channel,
        })
    }
}

pub struct Clean {
    channel: Channel,
}

impl Clean {
    pub fn leave(mut self) -> Result<RpcHandler, Box<dyn Error>> {
        let packet = CleanPacket::Leave {};
        self.channel.send(packet)?;
        Ok(RpcHandler {
            channel: self.channel,
        })
    }
}
