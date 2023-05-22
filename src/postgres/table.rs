use std::ops::Deref;
use std::ops::DerefMut;

const PG_PAGE_ALIGN: usize = 128;
const PG_PAGE_SIZE: usize = 8192;
const PG_PAGE_HEADER_ALIGN: usize = 8;
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

static_assertions::const_assert_eq!(std::mem::align_of::<PageHeader>(), PG_PAGE_HEADER_ALIGN);
static_assertions::const_assert_eq!(std::mem::size_of::<PageHeader>(), PG_PAGE_HEADER_SIZE);

#[repr(C, align(128))]
#[derive(Debug, Clone)]
pub struct Page {
    pub header: PageHeader,
    pub content: [u8; 8168],
}

static_assertions::const_assert_eq!(std::mem::align_of::<Page>(), PG_PAGE_ALIGN);
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
    unsafe fn get_line_pointer(self: *mut Self, i: u16) -> LinePointer {
        assert!(i < self.len());
        let offset = PG_PAGE_HEADER_SIZE + i as usize * std::mem::size_of::<LinePointer>();
        let ptr = (self as *mut u8).add(offset);
        let line_pointer = (ptr as *mut LinePointer).read();
        line_pointer
    }
    unsafe fn try_to_get_line_pointer(self: *mut Self, i: u16) -> Option<LinePointer> {
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
    pub unsafe fn lsn_get(self: *mut Self) -> u64 {
        (((*self).header.lsn.0 as u64) << 32) | (*self).header.lsn.1 as u64
    }
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
    pub unsafe fn tuples_get<'a>(self: *mut Self, i: u16) -> &'a [u8] {
        self.check();
        let line_pointer = self.get_line_pointer(i);
        let ptr = (self as *mut u8).add(line_pointer.offset());
        std::slice::from_raw_parts(ptr, line_pointer.size())
    }
    pub unsafe fn tuples_try_to_get<'a>(self: *mut Self, i: u16) -> Option<&'a [u8]> {
        self.check();
        let line_pointer = self.get_line_pointer(i);
        let ptr = (self as *mut u8).add(line_pointer.offset());
        Some(std::slice::from_raw_parts(ptr, line_pointer.size()))
    }
    pub unsafe fn tuples_get_mut<'a>(self: *mut Self, i: u16) -> &'a mut [u8] {
        self.check();
        let line_pointer = self.get_line_pointer(i);
        let ptr = (self as *mut u8).add(line_pointer.offset());
        std::slice::from_raw_parts_mut(ptr, line_pointer.size())
    }
    pub unsafe fn tuples_try_to_get_mut<'a>(self: *mut Self, i: u16) -> Option<&'a mut [u8]> {
        self.check();
        let line_pointer = self.get_line_pointer(i);
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

pub struct RegularReadGuard<'a> {
    pointer: Pointer,
    buffer: i32,
    data: &'a [u8],
}

impl<'a> RegularReadGuard<'a> {
    pub fn pointer(&self) -> Pointer {
        self.pointer
    }
    pub fn cast<T>(&self) -> *const T {
        self.data.as_ptr() as _
    }
}

impl<'a> Deref for RegularReadGuard<'a> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.data
    }
}

impl<'a> Drop for RegularReadGuard<'a> {
    fn drop(&mut self) {
        unsafe {
            pgrx::pg_sys::UnlockReleaseBuffer(self.buffer);
        }
    }
}

pub struct BuildReadGuard<'a> {
    pointer: Pointer,
    buffer: i32,
    data: &'a [u8],
}

impl<'a> BuildReadGuard<'a> {
    pub fn pointer(&self) -> Pointer {
        self.pointer
    }
    pub fn cast<T>(&self) -> *const T {
        self.data.as_ptr() as _
    }
}

impl<'a> Deref for BuildReadGuard<'a> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.data
    }
}

impl<'a> Drop for BuildReadGuard<'a> {
    fn drop(&mut self) {
        unsafe {
            pgrx::pg_sys::UnlockReleaseBuffer(self.buffer);
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum FlushType {
    Maximum,
    Append,
    Write,
}

pub struct RegularWriteGuard<'a> {
    relation: pgrx::pg_sys::Relation,
    pointer: Pointer,
    buffer: i32,
    flush: FlushType,
    data: &'a mut [u8],
}

impl<'a> RegularWriteGuard<'a> {
    pub fn pointer(&self) -> Pointer {
        self.pointer
    }
    pub fn cast<T>(&self) -> *const T {
        self.data.as_ptr() as _
    }
    pub fn cast_mut<T>(&mut self) -> *mut T {
        self.data.as_ptr() as _
    }
}

impl<'a> Deref for RegularWriteGuard<'a> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.data
    }
}

impl<'a> DerefMut for RegularWriteGuard<'a> {
    fn deref_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }
}

impl<'a> Drop for RegularWriteGuard<'a> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            pgrx::PANIC!("Dropping while a write guard is held.");
        }
        let buffer = self.buffer;
        let flush = self.flush;
        let tdata = unsafe { &*(fetch(buffer) as *const [u8; PG_PAGE_SIZE]) };
        let wal_offset = unsafe { self.data.as_ptr().offset_from(fetch(buffer) as _) as usize };
        let wal_len = self.data.len();
        let wal_offset_2 =
            PG_PAGE_HEADER_SIZE + self.pointer.id as usize * std::mem::size_of::<LinePointer>();
        let wal_len_2 = std::mem::size_of::<LinePointer>();
        if let Err(_panic) = std::panic::catch_unwind(move || unsafe {
            use FlushType::*;
            // todo: register an id for pgvecto.rs
            const RM_GENERIC_ID: pgrx::pg_sys::RmgrId = 20;
            pgrx::pg_sys::XLogBeginInsert();
            let lsn;
            match flush {
                Maximum => {
                    let flags = pgrx::pg_sys::REGBUF_FORCE_IMAGE as u8
                        | pgrx::pg_sys::REGBUF_STANDARD as u8;
                    pgrx::pg_sys::XLogRegisterBuffer(0, buffer, flags);
                    lsn = pgrx::pg_sys::XLogInsert(RM_GENERIC_ID, 0);
                }
                Append => {
                    let flags = pgrx::pg_sys::REGBUF_STANDARD as u8;
                    pgrx::pg_sys::XLogRegisterBuffer(0, buffer, flags);
                    let mut data = vec![0u8; 0];
                    data.extend_from_slice(&(wal_offset as u16).to_le_bytes());
                    data.extend_from_slice(&(wal_len as u16).to_le_bytes());
                    data.extend_from_slice(&tdata[wal_offset..wal_offset + wal_len]);
                    data.extend_from_slice(&(wal_offset_2 as u16).to_le_bytes());
                    data.extend_from_slice(&(wal_len_2 as u16).to_le_bytes());
                    data.extend_from_slice(&tdata[wal_offset_2..wal_offset + wal_len_2]);
                    lsn = pgrx::pg_sys::XLogInsert(RM_GENERIC_ID, 0);
                }
                Write => {
                    let flags = pgrx::pg_sys::REGBUF_STANDARD as u8;
                    pgrx::pg_sys::XLogRegisterBuffer(0, buffer, flags);
                    let mut data = vec![0u8; 0];
                    data.extend_from_slice(&(wal_offset as u16).to_le_bytes());
                    data.extend_from_slice(&(wal_len as u16).to_le_bytes());
                    data.extend_from_slice(&tdata[wal_offset..wal_offset + wal_len]);
                    pgrx::pg_sys::XLogRegisterBufData(0, data.as_mut_ptr() as _, data.len() as _);
                    lsn = pgrx::pg_sys::XLogInsert(RM_GENERIC_ID, 0);
                }
            }
            fetch(buffer).lsn_set(lsn);
            pgrx::pg_sys::MarkBufferDirty(buffer);
            pgrx::pg_sys::UnlockReleaseBuffer(buffer);
        }) {
            pgrx::PANIC!("Error while write xlog.");
        }
    }
}

pub struct BuildWriteGuard<'a> {
    relation: pgrx::pg_sys::Relation,
    pointer: Pointer,
    buffer: i32,
    data: &'a mut [u8],
}

impl<'a> BuildWriteGuard<'a> {
    pub fn pointer(&self) -> Pointer {
        self.pointer
    }
    pub fn cast<T>(&self) -> *const T {
        self.data.as_ptr() as _
    }
    pub fn cast_mut<T>(&mut self) -> *mut T {
        self.data.as_ptr() as _
    }
}

impl<'a> Deref for BuildWriteGuard<'a> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.data
    }
}

impl<'a> DerefMut for BuildWriteGuard<'a> {
    fn deref_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }
}

impl<'a> Drop for BuildWriteGuard<'a> {
    fn drop(&mut self) {
        unsafe {
            pgrx::pg_sys::MarkBufferDirty(self.buffer);
            pgrx::pg_sys::UnlockReleaseBuffer(self.buffer);
        }
    }
}

pub struct RegularTable {
    relation: pgrx::pg_sys::Relation,
}

impl RegularTable {
    pub unsafe fn new(relation: pgrx::pg_sys::Relation) -> Self {
        if (*(*relation).rd_rel).relpersistence != pgrx::pg_sys::RELPERSISTENCE_PERMANENT as i8 {
            panic!("Temporary tables are not supported yet.");
        }
        assert!((*relation).rd_createSubid == 0);
        assert!((*relation).rd_firstRelfilenodeSubid == 0);
        Self { relation }
    }
    pub fn pages(&mut self) -> u32 {
        unsafe {
            pgrx::pg_sys::RelationGetNumberOfBlocksInFork(
                self.relation,
                pgrx::pg_sys::ForkNumber_MAIN_FORKNUM,
            )
        }
    }
    pub fn read(&mut self, pointer: Pointer) -> Option<RegularReadGuard<'_>> {
        unsafe {
            let buffer = pgrx::pg_sys::ReadBuffer(self.relation, pointer.page);
            pgrx::pg_sys::LockBuffer(buffer, pgrx::pg_sys::BUFFER_LOCK_SHARE as _);
            let data = fetch(buffer).tuples_try_to_get(pointer.id)?;
            Some(RegularReadGuard {
                pointer,
                buffer,
                data,
            })
        }
    }
    pub fn write(&mut self, pointer: Pointer) -> Option<RegularWriteGuard<'_>> {
        unsafe {
            let buffer = pgrx::pg_sys::ReadBuffer(self.relation, pointer.page);
            pgrx::pg_sys::LockBuffer(buffer, pgrx::pg_sys::BUFFER_LOCK_EXCLUSIVE as _);
            let data = fetch(buffer).tuples_try_to_get_mut(pointer.id)?;
            Some(RegularWriteGuard {
                pointer,
                buffer,
                data,
                flush: FlushType::Write,
                relation: self.relation,
            })
        }
    }
    pub fn append(&mut self, size: u16) -> RegularWriteGuard<'_> {
        assert!(size as usize <= PG_PAGE_SPECIAL_MAXSIZE);
        unsafe {
            pgrx::pg_sys::LockRelationForExtension(self.relation, pgrx::pg_sys::ExclusiveLock as _);
            let n = pgrx::pg_sys::RelationGetNumberOfBlocksInFork(
                self.relation,
                pgrx::pg_sys::ForkNumber_MAIN_FORKNUM,
            );
            if n >= 1 {
                let buffer = pgrx::pg_sys::ReadBuffer(self.relation, n - 1);
                pgrx::pg_sys::LockBuffer(buffer, pgrx::pg_sys::BUFFER_LOCK_EXCLUSIVE as _);
                if let Some((id, data)) = fetch(buffer).tuples_push(size) {
                    pgrx::pg_sys::UnlockRelationForExtension(
                        self.relation,
                        pgrx::pg_sys::ExclusiveLock as _,
                    );
                    let pointer = Pointer { page: n - 1, id };
                    return RegularWriteGuard {
                        pointer,
                        buffer,
                        relation: self.relation,
                        flush: FlushType::Maximum,
                        data,
                    };
                }
                pgrx::pg_sys::UnlockReleaseBuffer(buffer);
            }
            let buffer = pgrx::pg_sys::ReadBuffer(self.relation, pgrx::pg_sys::InvalidBlockNumber);
            pgrx::pg_sys::LockBuffer(buffer, pgrx::pg_sys::BUFFER_LOCK_EXCLUSIVE as _);
            let page = fetch(buffer);
            page.initialize(0);
            let (id, data) = page.tuples_push(size).unwrap();
            pgrx::pg_sys::UnlockRelationForExtension(
                self.relation,
                pgrx::pg_sys::ExclusiveLock as _,
            );
            let pointer = Pointer { page: n, id };
            RegularWriteGuard {
                pointer,
                relation: self.relation,
                buffer,
                flush: FlushType::Append,
                data,
            }
        }
    }
}

pub struct BuildTable {
    relation: pgrx::pg_sys::Relation,
}

impl BuildTable {
    pub unsafe fn new(relation: pgrx::pg_sys::Relation) -> Self {
        if (*(*relation).rd_rel).relpersistence != pgrx::pg_sys::RELPERSISTENCE_PERMANENT as i8 {
            panic!("Temporary tables are not supported yet.");
        }
        assert!((*relation).rd_createSubid != 0 || (*relation).rd_firstRelfilenodeSubid != 0);
        Self { relation }
    }
    pub fn pages(&mut self) -> u32 {
        unsafe {
            pgrx::pg_sys::RelationGetNumberOfBlocksInFork(
                self.relation,
                pgrx::pg_sys::ForkNumber_MAIN_FORKNUM,
            )
        }
    }
    pub fn read(&mut self, pointer: Pointer) -> Option<BuildReadGuard<'_>> {
        unsafe {
            let buffer = pgrx::pg_sys::ReadBuffer(self.relation, pointer.page);
            pgrx::pg_sys::LockBuffer(buffer, pgrx::pg_sys::BUFFER_LOCK_SHARE as _);
            let data = fetch(buffer).tuples_try_to_get(pointer.id)?;
            Some(BuildReadGuard {
                pointer,
                buffer,
                data,
            })
        }
    }
    pub fn write(&mut self, pointer: Pointer) -> Option<BuildWriteGuard<'_>> {
        unsafe {
            let buffer = pgrx::pg_sys::ReadBuffer(self.relation, pointer.page);
            pgrx::pg_sys::LockBuffer(buffer, pgrx::pg_sys::BUFFER_LOCK_EXCLUSIVE as _);
            let data = fetch(buffer).tuples_try_to_get_mut(pointer.id)?;
            Some(BuildWriteGuard {
                pointer,
                buffer,
                data,
                relation: self.relation,
            })
        }
    }
    pub fn append(&mut self, size: u16) -> BuildWriteGuard<'_> {
        assert!(size as usize <= PG_PAGE_SPECIAL_MAXSIZE);
        unsafe {
            pgrx::pg_sys::LockRelationForExtension(self.relation, pgrx::pg_sys::ExclusiveLock as _);
            let n = pgrx::pg_sys::RelationGetNumberOfBlocksInFork(
                self.relation,
                pgrx::pg_sys::ForkNumber_MAIN_FORKNUM,
            );
            if n >= 1 {
                let buffer = pgrx::pg_sys::ReadBuffer(self.relation, n - 1);
                pgrx::pg_sys::LockBuffer(buffer, pgrx::pg_sys::BUFFER_LOCK_EXCLUSIVE as _);
                if let Some((id, data)) = fetch(buffer).tuples_push(size) {
                    pgrx::pg_sys::UnlockRelationForExtension(
                        self.relation,
                        pgrx::pg_sys::ExclusiveLock as _,
                    );
                    let pointer = Pointer { page: n - 1, id };
                    return BuildWriteGuard {
                        pointer,
                        buffer,
                        relation: self.relation,
                        data,
                    };
                }
                pgrx::pg_sys::UnlockReleaseBuffer(buffer);
            }
            let buffer = pgrx::pg_sys::ReadBuffer(self.relation, pgrx::pg_sys::InvalidBlockNumber);
            pgrx::pg_sys::LockBuffer(buffer, pgrx::pg_sys::BUFFER_LOCK_EXCLUSIVE as _);
            let page = fetch(buffer);
            page.initialize(0);
            let (id, data) = page.tuples_push(size).unwrap();
            pgrx::pg_sys::UnlockRelationForExtension(
                self.relation,
                pgrx::pg_sys::ExclusiveLock as _,
            );
            let pointer = Pointer { page: n, id };
            BuildWriteGuard {
                pointer,
                relation: self.relation,
                buffer,
                data,
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
