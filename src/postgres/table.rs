use std::collections::HashMap;

const PG_PAGE_SIZE: usize = 8192;
const PG_PAGE_HEADER_SIZE: usize = 24;
const PG_PAGE_TUPLE_ALIGN: usize = 8;
const PG_PAGE_SPECIAL_ALIGN: usize = 8;
const PG_PAGE_SPECIAL_MAXSIZE: usize = 8168;

#[repr(C, align(8))]
#[derive(Default, Debug, Clone, Copy, Hash, PartialEq, PartialOrd, Eq, Ord)]
pub struct HeapPointer {
    pub newtype: u64,
}

impl HeapPointer {
    pub fn from_sys(sys: pgrx::pg_sys::ItemPointerData) -> Self {
        let mut newtype = 0;
        newtype |= (sys.ip_blkid.bi_hi as u64) << 48;
        newtype |= (sys.ip_blkid.bi_lo as u64) << 32;
        newtype |= (sys.ip_posid as u64) << 0;
        Self { newtype }
    }
    pub fn into_sys(self) -> pgrx::pg_sys::ItemPointerData {
        pgrx::pg_sys::ItemPointerData {
            ip_blkid: pgrx::pg_sys::BlockIdData {
                bi_hi: ((self.newtype >> 48) & 0xffff) as u16,
                bi_lo: ((self.newtype >> 32) & 0xffff) as u16,
            },
            ip_posid: ((self.newtype >> 0) & 0xffff) as u16,
        }
    }
}

#[repr(C, align(8))]
#[derive(Default, Debug, Clone, Copy, Hash, PartialEq, PartialOrd, Eq, Ord)]
pub struct Pointer {
    pub page: u32,
    pub id: u16,
}

static_assertions::assert_eq_size!(Pointer, u64);
static_assertions::assert_eq_align!(Pointer, u64);

impl Pointer {
    pub const NULL: Self = Self { page: 0, id: 0 };

    pub fn new(page: u32, id: u16) -> Self {
        Self { page, id }
    }

    pub fn is_null(self) -> bool {
        self.page == 0 && self.id == 0
    }
}

#[repr(C, align(4))]
#[derive(Default, Debug, Clone, Copy, Hash, PartialEq, PartialOrd, Eq, Ord)]
pub struct LinePointer {
    pub offset: u16,
    pub size: u16,
}

static_assertions::assert_eq_size!(LinePointer, u32);
static_assertions::assert_eq_align!(LinePointer, u32);

impl LinePointer {
    pub fn offset(&self) -> usize {
        self.offset as usize
    }
    pub fn size(&self) -> usize {
        self.size as usize
    }
}

#[repr(C, align(8))]
#[derive(Debug, Clone)]
pub struct PageHeader {
    pub lsn: (u32, u32),
    pub checksum: u16,
    pub flags: u16,
    pub lower: u16,
    pub upper: u16,
    pub special: u16,
    pub pagesize_version: u16,
    pub prune_xid: i32,
}

static_assertions::const_assert_eq!(std::mem::size_of::<PageHeader>(), PG_PAGE_HEADER_SIZE);

#[repr(C, align(128))]
#[derive(Debug, Clone)]
pub struct Page {
    pub header: PageHeader,
    pub content: [u8; 8168],
}

static_assertions::const_assert_eq!(std::mem::size_of::<Page>(), PG_PAGE_SIZE);

impl Page {
    unsafe fn check(self: *mut Self) {
        assert!(PG_PAGE_HEADER_SIZE <= (*self).header.lower as usize);
        assert!((*self).header.lower as usize <= (*self).header.upper as usize);
        assert!((*self).header.upper as usize <= (*self).header.special as usize);
        assert!((*self).header.special as usize <= PG_PAGE_SIZE);
        assert!((*self).header.lower as usize % std::mem::align_of::<LinePointer>() == 0);
        assert!((*self).header.upper as usize % PG_PAGE_TUPLE_ALIGN == 0);
        assert!((*self).header.special as usize % PG_PAGE_SPECIAL_ALIGN == 0);
    }
    unsafe fn get_line_pointer(self: *mut Self, i: u16) -> Option<LinePointer> {
        if i >= self.len() {
            return None;
        }
        let offset = PG_PAGE_HEADER_SIZE + i as usize * std::mem::size_of::<LinePointer>();
        let ptr = (self as *mut u8).add(offset);
        let line_pointer = (ptr as *mut LinePointer).read();
        Some(line_pointer)
    }
    pub unsafe fn initialize(self: *mut Self, special: u16) {
        assert!(special as usize % PG_PAGE_SPECIAL_ALIGN == 0);
        assert!(special as usize <= PG_PAGE_SPECIAL_MAXSIZE);
        std::ptr::write_bytes(self as *mut u8, 0, PG_PAGE_SIZE);
        (*self).header.flags = 0;
        (*self).header.lower = PG_PAGE_HEADER_SIZE as u16;
        (*self).header.upper = PG_PAGE_SIZE as u16 - special;
        (*self).header.special = PG_PAGE_SIZE as u16 - special;
        (*self).header.pagesize_version =
            PG_PAGE_SIZE as u16 | pgrx::pg_sys::PG_PAGE_LAYOUT_VERSION as u16;
        (*self).header.prune_xid = pgrx::pg_sys::InvalidTransactionId as i32;
    }
    pub unsafe fn lsn_set(self: *mut Self, lsn: u64) {
        (*self).header.lsn.0 = (lsn >> 32) as u32;
        (*self).header.lsn.1 = (lsn >> 0) as u32;
    }
    #[allow(dead_code)]
    pub unsafe fn lsn_get(self: *mut Self) -> u64 {
        (((*self).header.lsn.0 as u64) << 32) | (*self).header.lsn.1 as u64
    }
    #[allow(dead_code)]
    pub unsafe fn special(self: *mut Self) -> *mut [u8] {
        self.check();
        let offset = (*self).header.special;
        let len = PG_PAGE_SIZE as u16 - (*self).header.special;
        std::slice::from_raw_parts_mut((self as *mut u8).add(offset as usize).cast(), len as usize)
    }
    pub unsafe fn len(self: *mut Self) -> u16 {
        self.check();
        let n = ((*self).header.lower as usize - PG_PAGE_HEADER_SIZE)
            / std::mem::size_of::<LinePointer>();
        n as u16
    }
    pub unsafe fn tuples_get<'a>(self: *mut Self, i: u16) -> Option<&'a [u8]> {
        self.check();
        let line_pointer = self.get_line_pointer(i)?;
        let ptr = (self as *mut u8).add(line_pointer.offset());
        Some(std::slice::from_raw_parts(ptr, line_pointer.size()))
    }
    pub unsafe fn tuples_get_mut<'a>(self: *mut Self, i: u16) -> Option<&'a mut [u8]> {
        self.check();
        let line_pointer = self.get_line_pointer(i)?;
        let ptr = (self as *mut u8).add(line_pointer.offset());
        Some(std::slice::from_raw_parts_mut(ptr, line_pointer.size()))
    }
    pub unsafe fn tuples_push<'a>(self: *mut Self, tuple_size: u16) -> Option<(u16, &'a mut [u8])> {
        self.check();
        assert!(tuple_size as usize % PG_PAGE_TUPLE_ALIGN == 0);
        let space = (*self).header.upper - (*self).header.lower;
        if (space as usize) < std::mem::size_of::<LinePointer>() + tuple_size as usize {
            return None;
        }
        let tuple_offset = (*self).header.upper - tuple_size;
        let n = self.len();
        {
            let offset = PG_PAGE_HEADER_SIZE + n as usize * std::mem::size_of::<LinePointer>();
            let ptr = (self as *mut u8).add(offset);
            let line_pointer = LinePointer {
                offset: tuple_offset,
                size: tuple_size,
            };
            (ptr as *mut LinePointer).write(line_pointer);
        }
        let slice = {
            let ptr = (self as *mut u8).add(tuple_offset as usize);
            std::slice::from_raw_parts_mut(ptr, tuple_size as usize)
        };
        (*self).header.lower += std::mem::size_of::<LinePointer>() as u16;
        (*self).header.upper -= tuple_size as u16;
        Some((n, slice))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum GrandLocking {
    Exclusive,
    Shared,
}

pub struct Table {
    relation: pgrx::pg_sys::Relation,
    locking: GrandLocking,
    needs_wal: bool,
    buffers: HashMap<u32, (pgrx::pg_sys::Buffer, bool)>,
}

impl Table {
    pub unsafe fn new(relation: pgrx::pg_sys::Relation, locking: GrandLocking) -> Self {
        use GrandLocking::*;
        let needs_wal = (*(*relation).rd_rel).relpersistence
            == pgrx::pg_sys::RELPERSISTENCE_PERMANENT as i8
            && (pgrx::pg_sys::wal_level > pgrx::pg_sys::WalLevel_WAL_LEVEL_MINIMAL as _
                || ((*relation).rd_createSubid == 0 && (*relation).rd_firstRelfilenodeSubid == 0));
        match locking {
            Exclusive => {
                pgrx::pg_sys::LockRelationForExtension(relation, pgrx::pg_sys::ExclusiveLock as _);
            }
            Shared => {
                pgrx::pg_sys::LockRelationForExtension(
                    relation,
                    pgrx::pg_sys::AccessShareLock as _,
                );
            }
        }
        Self {
            relation,
            locking,
            needs_wal,
            buffers: HashMap::new(),
        }
    }
    pub fn pages(&mut self) -> u32 {
        unsafe {
            pgrx::pg_sys::RelationGetNumberOfBlocksInFork(
                self.relation,
                pgrx::pg_sys::ForkNumber_MAIN_FORKNUM,
            )
        }
    }
    pub fn read(&mut self, pointer: Pointer) -> Option<&[u8]> {
        unsafe {
            if let Some((buffer, _dirty)) = self.buffers.get_mut(&pointer.page) {
                fetch(*buffer).tuples_get(pointer.id)
            } else {
                let buffer = pgrx::pg_sys::ReadBuffer(self.relation, pointer.page);
                pgrx::pg_sys::LockBuffer(buffer, pgrx::pg_sys::BUFFER_LOCK_SHARE as _);
                self.buffers.insert(pointer.page, (buffer, false));
                fetch(buffer).tuples_get(pointer.id)
            }
        }
    }
    pub fn write(&mut self, pointer: Pointer) -> Option<&mut [u8]> {
        unsafe {
            if let Some((buffer, dirty)) = self.buffers.get_mut(&pointer.page) {
                if *dirty != true {
                    pgrx::pg_sys::LockBuffer(*buffer, pgrx::pg_sys::BUFFER_LOCK_UNLOCK as _);
                    pgrx::pg_sys::LockBuffer(*buffer, pgrx::pg_sys::BUFFER_LOCK_EXCLUSIVE as _);
                    *dirty = true;
                }
                fetch(*buffer).tuples_get_mut(pointer.id)
            } else {
                let buffer = pgrx::pg_sys::ReadBuffer(self.relation, pointer.page);
                pgrx::pg_sys::LockBuffer(buffer, pgrx::pg_sys::BUFFER_LOCK_EXCLUSIVE as _);
                self.buffers.insert(pointer.page, (buffer, true));
                fetch(buffer).tuples_get_mut(pointer.id)
            }
        }
    }
    pub fn append(&mut self, data: &[u8]) -> Pointer {
        assert_eq!(self.locking, GrandLocking::Exclusive);
        assert!(data.len() as usize <= PG_PAGE_SPECIAL_MAXSIZE);
        unsafe {
            let n = pgrx::pg_sys::RelationGetNumberOfBlocksInFork(
                self.relation,
                pgrx::pg_sys::ForkNumber_MAIN_FORKNUM,
            );
            if n >= 1 {
                let page = n - 1;
                let page = if let Some((buffer, dirty)) = self.buffers.get_mut(&page) {
                    if *dirty != true {
                        pgrx::pg_sys::LockBuffer(*buffer, pgrx::pg_sys::BUFFER_LOCK_UNLOCK as _);
                        pgrx::pg_sys::LockBuffer(*buffer, pgrx::pg_sys::BUFFER_LOCK_EXCLUSIVE as _);
                        *dirty = true;
                    }
                    fetch(*buffer)
                } else {
                    let buffer = pgrx::pg_sys::ReadBuffer(self.relation, page);
                    pgrx::pg_sys::LockBuffer(buffer, pgrx::pg_sys::BUFFER_LOCK_EXCLUSIVE as _);
                    self.buffers.insert(page, (buffer, true));
                    fetch(buffer)
                };
                if let Some((id, raw)) = page.tuples_push(data.len() as _) {
                    let pointer = Pointer::new(n - 1, id);
                    raw.copy_from_slice(data);
                    return pointer;
                }
            }
            let buffer = pgrx::pg_sys::ReadBuffer(self.relation, pgrx::pg_sys::InvalidBlockNumber);
            pgrx::pg_sys::LockBuffer(buffer, pgrx::pg_sys::BUFFER_LOCK_EXCLUSIVE as _);
            self.buffers.insert(n, (buffer, true));
            let page = fetch(buffer);
            page.initialize(0);
            let (id, raw) = page.tuples_push(data.len() as _).unwrap();
            let pointer = Pointer::new(n, id);
            raw.copy_from_slice(data);
            pointer
        }
    }
}

impl Drop for Table {
    fn drop(&mut self) {
        unsafe {
            let buffers = self.buffers.clone();
            let count = buffers.values().filter(|(_, d)| *d).count();
            if self.needs_wal && count > 0 {
                if let Err(_panic) = std::panic::catch_unwind(move || {
                    const RM_GENERIC_ID: pgrx::pg_sys::RmgrId = 20;
                    pgrx::pg_sys::XLogEnsureRecordSpace(count as _, 20);
                    pgrx::pg_sys::XLogBeginInsert();
                    for (i, (buffer, _)) in buffers.values().filter(|(_, d)| *d).enumerate() {
                        let flags = pgrx::pg_sys::REGBUF_FORCE_IMAGE as u8
                            | pgrx::pg_sys::REGBUF_STANDARD as u8;
                        pgrx::pg_sys::XLogRegisterBuffer(i as _, *buffer, flags);
                    }
                    let lsn = pgrx::pg_sys::XLogInsert(RM_GENERIC_ID, 0);
                    for (buffer, _) in buffers.values().filter(|(_, d)| *d) {
                        fetch(*buffer).lsn_set(lsn);
                        pgrx::pg_sys::MarkBufferDirty(*buffer);
                        pgrx::pg_sys::UnlockReleaseBuffer(*buffer);
                    }
                }) {
                    pgrx::PANIC!("Error while writing xlog.");
                }
            }
            for (buffer, _) in self.buffers.values().filter(|(_, d)| !*d) {
                pgrx::pg_sys::UnlockReleaseBuffer(*buffer);
            }
            use GrandLocking::*;
            match self.locking {
                Exclusive => {
                    pgrx::pg_sys::UnlockRelationForExtension(
                        self.relation,
                        pgrx::pg_sys::ExclusiveLock as _,
                    );
                }
                Shared => {
                    pgrx::pg_sys::UnlockRelationForExtension(
                        self.relation,
                        pgrx::pg_sys::AccessShareLock as _,
                    );
                }
            }
        }
    }
}

unsafe fn fetch(buffer: pgrx::pg_sys::Buffer) -> *mut Page {
    assert_ne!(buffer, 0);
    assert!(buffer <= pgrx::pg_sys::NBuffers && buffer >= -pgrx::pg_sys::NLocBuffer);
    if buffer < 0 {
        pgrx::pg_sys::LocalBufferBlockPointers.add((-buffer - 1) as usize) as *mut u8
    } else {
        pgrx::pg_sys::BufferBlocks.add(8192 * (buffer - 1) as usize) as *mut u8
    }
    .cast()
}
