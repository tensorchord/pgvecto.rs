use parking_lot::Mutex;

pub struct VisitedPool {
    n: u32,
    locked_buffers: Mutex<Vec<VisitedBuffer>>,
}

impl VisitedPool {
    pub fn new(n: u32) -> Self {
        Self {
            n,
            locked_buffers: Mutex::new(Vec::new()),
        }
    }
    pub fn fetch(&self) -> VisitedGuard {
        let buffer = self
            .locked_buffers
            .lock()
            .pop()
            .unwrap_or_else(|| VisitedBuffer::new(self.n as _));
        VisitedGuard { buffer, pool: self }
    }

    pub fn fetch2(&self) -> VisitedGuardChecker {
        let mut buffer = self
            .locked_buffers
            .lock()
            .pop()
            .unwrap_or_else(|| VisitedBuffer::new(self.n as _));
        {
            buffer.version = buffer.version.wrapping_add(1);
            if buffer.version == 0 {
                buffer.data.fill(0);
            }
        }
        VisitedGuardChecker { buffer, pool: self }
    }
}

pub struct VisitedGuard<'a> {
    buffer: VisitedBuffer,
    pool: &'a VisitedPool,
}

impl<'a> VisitedGuard<'a> {
    pub fn fetch(&mut self) -> VisitedChecker<'_> {
        self.buffer.version = self.buffer.version.wrapping_add(1);
        if self.buffer.version == 0 {
            self.buffer.data.fill(0);
        }
        VisitedChecker {
            buffer: &mut self.buffer,
        }
    }
}

impl<'a> Drop for VisitedGuard<'a> {
    fn drop(&mut self) {
        let src = VisitedBuffer {
            version: 0,
            data: Vec::new(),
        };
        let buffer = std::mem::replace(&mut self.buffer, src);
        self.pool.locked_buffers.lock().push(buffer);
    }
}

pub struct VisitedChecker<'a> {
    buffer: &'a mut VisitedBuffer,
}

impl<'a> VisitedChecker<'a> {
    pub fn check(&mut self, i: u32) -> bool {
        self.buffer.data[i as usize] != self.buffer.version
    }
    pub fn mark(&mut self, i: u32) {
        self.buffer.data[i as usize] = self.buffer.version;
    }
}

pub struct VisitedGuardChecker<'a> {
    buffer: VisitedBuffer,
    pool: &'a VisitedPool,
}

impl<'a> VisitedGuardChecker<'a> {
    pub fn check(&mut self, i: u32) -> bool {
        self.buffer.data[i as usize] != self.buffer.version
    }
    pub fn mark(&mut self, i: u32) {
        self.buffer.data[i as usize] = self.buffer.version;
    }
}

impl<'a> Drop for VisitedGuardChecker<'a> {
    fn drop(&mut self) {
        let src = VisitedBuffer {
            version: 0,
            data: Vec::new(),
        };
        let buffer = std::mem::replace(&mut self.buffer, src);
        self.pool.locked_buffers.lock().push(buffer);
    }
}

pub struct VisitedBuffer {
    version: usize,
    data: Vec<usize>,
}

impl VisitedBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            version: 0,
            data: bytemuck::zeroed_vec(capacity),
        }
    }
}
