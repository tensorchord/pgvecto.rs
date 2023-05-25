use pgrx::pg_sys::Buffer;
use std::cell::RefCell;
use std::collections::HashMap;

const PG_PAGE_SIZE: usize = 8192;
const PG_PAGE_HEADER_SIZE: usize = 24;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LockMode {
    Exclusive = 0,
    Shared = 1,
}

pub struct GiantLockGuard {
    kind: LockMode,
    relation: pgrx::pg_sys::Relation,
}

impl GiantLockGuard {
    pub unsafe fn new(relation: pgrx::pg_sys::Relation, kind: LockMode) -> Self {
        use LockMode::*;
        unsafe {
            match kind {
                Exclusive => {
                    let lockmode = pgrx::pg_sys::ExclusiveLock as i32;
                    pgrx::pg_sys::LockRelationForExtension(relation, lockmode);
                }
                Shared => {
                    let lockmode = pgrx::pg_sys::AccessShareLock as i32;
                    pgrx::pg_sys::LockRelationForExtension(relation, lockmode);
                }
            }
        }
        Self { kind, relation }
    }
    pub fn kind(&self) -> LockMode {
        self.kind
    }
    pub fn read(&mut self) -> Read<'_> {
        Read {
            relation: self.relation,
            locking: self,
            active: RefCell::new(HashMap::new()),
        }
    }
    pub fn write(&mut self) -> Write<'_> {
        Write {
            relation: self.relation,
            locking: self,
            active: RefCell::new(HashMap::new()),
        }
    }
}

impl Drop for GiantLockGuard {
    fn drop(&mut self) {
        use LockMode::*;
        match self.kind {
            Exclusive => {
                let lockmode = pgrx::pg_sys::ExclusiveLock as i32;
                unsafe {
                    pgrx::pg_sys::UnlockRelationForExtension(self.relation, lockmode);
                }
            }
            Shared => {
                let lockmode = pgrx::pg_sys::AccessShareLock as i32;
                unsafe {
                    pgrx::pg_sys::UnlockRelationForExtension(self.relation, lockmode);
                }
            }
        }
    }
}

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
pub struct Span {
    pub offset: u16,
    pub size: u16,
}

static_assertions::assert_eq_size!(Span, u32);
static_assertions::assert_eq_align!(Span, u32);

impl Span {
    pub fn new(offset: usize, size: usize) -> Self {
        assert!(offset <= 8192);
        assert!(size <= 8192);
        assert!(offset + size <= 8192);
        Self {
            offset: offset as _,
            size: size as _,
        }
    }
    pub fn offset(&self) -> usize {
        self.offset as usize
    }
    pub fn size(&self) -> usize {
        self.size as usize
    }
    pub fn end(&self) -> usize {
        self.offset as usize + self.size as usize
    }
    pub fn subspan(&self, offset: usize, size: usize) -> Self {
        assert!(offset + size <= self.size as usize);
        Self {
            offset: self.offset + offset as u16,
            size: size as u16,
        }
    }
}

#[repr(C, align(8))]
pub struct TupleHeader {
    magic: u32,
    immutable: u16,
    mutable: u16,
}

static_assertions::assert_eq_size!(TupleHeader, u64);

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
    unsafe fn span_cast<T>(self: *mut Self, span: Span) -> *mut T {
        assert!(std::mem::size_of::<T>() <= span.size());
        assert!((std::mem::align_of::<T>() - 1) & span.offset() == 0);
        (self as *mut u8).add(span.offset()) as *mut T
    }
    unsafe fn span_bytes(self: *mut Self, span: Span) -> *mut [u8] {
        std::ptr::from_raw_parts_mut((self as *mut u8).add(span.offset()) as *mut (), span.size())
    }
    pub const fn new() -> Self {
        Self {
            header: PageHeader {
                lsn: (0, 0),
                checksum: 0,
                flags: 0,
                lower: PG_PAGE_HEADER_SIZE as u16,
                upper: PG_PAGE_SIZE as u16,
                special: PG_PAGE_SIZE as u16,
                pagesize_version: PG_PAGE_SIZE as u16 | pgrx::pg_sys::PG_PAGE_LAYOUT_VERSION as u16,
                prune_xid: pgrx::pg_sys::InvalidTransactionId as i32,
            },
            content: [0; 8168],
        }
    }
    pub unsafe fn lower(self: *mut Self) -> Span {
        check(self);
        Span::new(0, (*self).header.lower as usize)
    }
    pub unsafe fn linps(self: *mut Self) -> Span {
        check(self);
        let start_linps = std::mem::size_of::<PageHeader>();
        let end_linps = (*self).header.lower as usize;
        Span::new(start_linps, end_linps - start_linps)
    }
    pub unsafe fn len(self: *mut Self) -> u16 {
        (self.linps().size() / std::mem::size_of::<Span>()) as u16
    }
    pub unsafe fn linp(self: *mut Self, i: u16) -> Option<Span> {
        let n = self.len();
        if i >= n {
            return None;
        }
        let offset_linp = PG_PAGE_HEADER_SIZE + i as usize * std::mem::size_of::<Span>();
        let size_linp = std::mem::size_of::<Span>();
        Some(Span::new(offset_linp, size_linp))
    }
    pub unsafe fn tuple(self: *mut Self, i: u16) -> Option<Span> {
        let span_linp = self.linp(i)?;
        let span_tuple = (self.span_cast::<Span>(span_linp)).read();
        Some(span_tuple)
    }
}

#[derive(Debug, Clone)]
struct ReadState {
    buffer: i32,
    locks: [u8; 16],
}

impl ReadState {
    pub fn is_free(&self) -> bool {
        self.locks == [0; 16]
    }
    pub fn lock_read(&mut self, i: u16) {
        assert_ne!(self.locks[i as usize], u8::MAX);
        self.locks[i as usize] += 1;
    }
    pub fn unlock_read(&mut self, i: u16) {
        assert_ne!(self.locks[i as usize], u8::MIN);
        self.locks[i as usize] -= 1;
    }
}

pub struct ReadReadGuard<'a, 'b> {
    table: &'b Read<'a>,
    buffer: Buffer,
    pointer: Pointer,
    span: Span,
    span_immutable: Span,
    span_mutable: Span,
}

impl<'a, 'b> ReadGuardLike for ReadReadGuard<'a, 'b> {
    fn immutable(&self) -> &[u8] {
        unsafe { &*fetch(self.buffer).span_bytes(self.span_immutable) }
    }
    fn mutable(&self) -> &[u8] {
        unsafe { &*fetch(self.buffer).span_bytes(self.span_mutable) }
    }

    fn pointer(&self) -> Pointer {
        self.pointer
    }
}

impl<'a, 'b> Drop for ReadReadGuard<'a, 'b> {
    fn drop(&mut self) {
        unsafe {
            let mut active = self.table.active.borrow_mut();
            let state = active.get_mut(&self.pointer.page).unwrap();
            state.unlock_read(self.pointer.id);
            if state.is_free() {
                pgrx::pg_sys::UnlockReleaseBuffer(self.buffer);
                active.remove(&self.pointer.page);
            }
        }
    }
}

pub struct Read<'a> {
    locking: &'a mut GiantLockGuard,
    relation: pgrx::pg_sys::Relation,
    active: RefCell<HashMap<u32, ReadState>>,
}

impl<'a> ReadLike for Read<'a> {
    type ReadGuard<'b> = ReadReadGuard<'a, 'b>
    where 'a: 'b;

    fn pages(&mut self) -> u32 {
        unsafe { npages(self.relation) }
    }

    fn read(&self, pointer: Pointer) -> ReadReadGuard<'a, '_> {
        unsafe {
            let mut active = self.active.borrow_mut();
            if !active.contains_key(&pointer.page) {
                let buffer = pgrx::pg_sys::ReadBuffer(self.relation, pointer.page);
                pgrx::pg_sys::LockBuffer(buffer, pgrx::pg_sys::BUFFER_LOCK_SHARE as _);
                let state = ReadState {
                    buffer,
                    locks: [0; 16],
                };
                active.insert(pointer.page, state);
            }
            let state = active.get_mut(&pointer.page).unwrap();
            state.lock_read(pointer.id);
            let span_tuple = fetch(state.buffer).tuple(pointer.id).unwrap();
            let span_immutable = immutable_of_tuple(fetch(state.buffer), span_tuple);
            let span_mutable = mutable_of_tuple(fetch(state.buffer), span_tuple);
            ReadReadGuard {
                table: self,
                buffer: state.buffer,
                pointer,
                span: span_tuple,
                span_immutable,
                span_mutable,
            }
        }
    }
}

impl<'a> Drop for Read<'a> {
    fn drop(&mut self) {
        assert!(self.active.borrow().len() == 0);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LowerDirty {
    Clean,
    Full,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TupleMutableDirty {
    Clean,
    Full,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TupleDirty {
    Partial(TupleMutableDirty),
    Full,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PageDirty {
    Partial {
        lower: LowerDirty,
        tuples: [TupleDirty; 16],
    },
    Full,
}

impl PageDirty {
    const CLEAN: Self = Self::Partial {
        lower: LowerDirty::Clean,
        tuples: [TupleDirty::Partial(TupleMutableDirty::Clean); 16],
    };
}

#[derive(Debug, Clone)]
struct WriteState {
    buffer: i32,
    locks: [Option<u8>; 16],
    dirty: PageDirty,
}

impl WriteState {
    fn new(buffer: Buffer) -> Self {
        Self {
            buffer,
            locks: [Some(0); 16],
            dirty: PageDirty::CLEAN,
        }
    }
    pub fn is_free(&self) -> bool {
        self.locks == [Some(0); 16]
    }
    pub fn is_dirty(&self) -> bool {
        self.dirty != PageDirty::CLEAN
    }
    pub fn mark_dirty(&mut self) {
        self.dirty = PageDirty::Full;
    }
    pub fn mark_lower_dirty(&mut self) {
        if let PageDirty::Partial { lower, .. } = &mut self.dirty {
            *lower = LowerDirty::Full;
        }
    }
    pub fn mark_tuple_dirty(&mut self, i: u16) {
        if let PageDirty::Partial { tuples, .. } = &mut self.dirty {
            tuples[i as usize] = TupleDirty::Full;
        }
    }
    pub fn mark_tuple_mutable_dirty(&mut self, i: u16) {
        if let PageDirty::Partial { tuples, .. } = &mut self.dirty {
            if let TupleDirty::Partial(mutable) = &mut tuples[i as usize] {
                *mutable = TupleMutableDirty::Full;
            }
        }
    }
    pub fn lock_read(&mut self, i: u16) {
        assert_ne!(self.locks[i as usize].unwrap(), u8::MAX);
        *self.locks[i as usize].as_mut().unwrap() += 1;
    }
    pub fn unlock_read(&mut self, i: u16) {
        assert_ne!(self.locks[i as usize].unwrap(), u8::MIN);
        *self.locks[i as usize].as_mut().unwrap() -= 1;
    }
    pub fn lock_write(&mut self, i: u16) {
        assert_eq!(self.locks[i as usize], Some(0));
        self.locks[i as usize] = None;
    }
    pub fn unlock_write(&mut self, i: u16) {
        assert_eq!(self.locks[i as usize], None);
        self.locks[i as usize] = Some(0);
    }
}

pub struct WriteReadGuard<'a, 'b> {
    table: &'b Write<'a>,
    buffer: Buffer,
    pointer: Pointer,
    span: Span,
    span_immutable: Span,
    span_mutable: Span,
}

impl<'a, 'b> ReadGuardLike for WriteReadGuard<'a, 'b> {
    fn immutable(&self) -> &[u8] {
        unsafe { &*fetch(self.buffer).span_bytes(self.span_immutable) }
    }
    fn mutable(&self) -> &[u8] {
        unsafe { &*fetch(self.buffer).span_bytes(self.span_mutable) }
    }
    fn pointer(&self) -> Pointer {
        self.pointer
    }
}

impl<'a, 'b> Drop for WriteReadGuard<'a, 'b> {
    fn drop(&mut self) {
        let mut active = self.table.active.borrow_mut();
        let state = active.get_mut(&self.pointer.page).unwrap();
        state.unlock_read(self.pointer.id);
        if !state.is_dirty() && state.is_free() {
            unsafe {
                pgrx::pg_sys::UnlockReleaseBuffer(self.buffer);
            }
            active.remove(&self.pointer.page);
        }
    }
}

pub struct WriteWriteGuard<'a, 'b> {
    table: &'b Write<'a>,
    buffer: Buffer,
    pointer: Pointer,
    span: Span,
    span_immutable: Span,
    span_mutable: Span,
}

impl<'a, 'b> ReadGuardLike for WriteWriteGuard<'a, 'b> {
    fn immutable(&self) -> &[u8] {
        unsafe { &*fetch(self.buffer).span_bytes(self.span_immutable) }
    }
    fn mutable(&self) -> &[u8] {
        unsafe { &*fetch(self.buffer).span_bytes(self.span_mutable) }
    }
    fn pointer(&self) -> Pointer {
        self.pointer
    }
}

impl<'a, 'b> WriteGuardLike for WriteWriteGuard<'a, 'b> {
    fn mutable_mut(&mut self) -> &mut [u8] {
        unsafe { &mut *fetch(self.buffer).span_bytes(self.span_mutable) }
    }
}

impl<'a, 'b> Drop for WriteWriteGuard<'a, 'b> {
    fn drop(&mut self) {
        let mut active = self.table.active.borrow_mut();
        let state = active.get_mut(&self.pointer.page).unwrap();
        state.unlock_write(self.pointer.id);
    }
}

pub struct Write<'a> {
    locking: &'a mut GiantLockGuard,
    relation: pgrx::pg_sys::Relation,
    active: RefCell<HashMap<u32, WriteState>>,
}

impl<'a> ReadLike for Write<'a> {
    type ReadGuard<'b> = WriteReadGuard<'a, 'b>
    where 'a: 'b;

    fn pages(&mut self) -> u32 {
        unsafe { npages(self.relation) }
    }
    fn read(&self, pointer: Pointer) -> WriteReadGuard<'a, '_> {
        unsafe {
            let mut active = self.active.borrow_mut();
            if !active.contains_key(&pointer.page) {
                let buffer = pgrx::pg_sys::ReadBuffer(self.relation, pointer.page);
                pgrx::pg_sys::LockBuffer(buffer, pgrx::pg_sys::BUFFER_LOCK_SHARE as _);
                let state = WriteState::new(buffer);
                active.insert(pointer.page, state);
            }
            let state = active.get_mut(&pointer.page).unwrap();
            state.lock_read(pointer.id);
            let span_tuple = fetch(state.buffer).tuple(pointer.id).unwrap();
            let span_immutable = immutable_of_tuple(fetch(state.buffer), span_tuple);
            let span_mutable = mutable_of_tuple(fetch(state.buffer), span_tuple);
            WriteReadGuard {
                table: self,
                buffer: state.buffer,
                pointer,
                span: span_tuple,
                span_immutable,
                span_mutable,
            }
        }
    }
}

impl<'a> WriteLike for Write<'a> {
    type WriteGuard<'b> = WriteWriteGuard<'a, 'b>
    where 'a: 'b;

    fn write(&self, pointer: Pointer) -> WriteWriteGuard<'a, '_> {
        unsafe {
            let mut active = self.active.borrow_mut();
            if !active.contains_key(&pointer.page) {
                let buffer = pgrx::pg_sys::ReadBuffer(self.relation, pointer.page);
                pgrx::pg_sys::LockBuffer(buffer, pgrx::pg_sys::BUFFER_LOCK_SHARE as _);
                let state = WriteState::new(buffer);
                active.insert(pointer.page, state);
            }
            let state = active.get_mut(&pointer.page).unwrap();
            if !state.is_dirty() {
                pgrx::pg_sys::LockBuffer(state.buffer, pgrx::pg_sys::BUFFER_LOCK_UNLOCK as _);
                pgrx::pg_sys::LockBuffer(state.buffer, pgrx::pg_sys::BUFFER_LOCK_EXCLUSIVE as _);
            }
            state.mark_tuple_mutable_dirty(pointer.id);
            state.lock_write(pointer.id);
            let span_tuple = fetch(state.buffer).tuple(pointer.id).unwrap();
            let span_immutable = immutable_of_tuple(fetch(state.buffer), span_tuple);
            let span_mutable = mutable_of_tuple(fetch(state.buffer), span_tuple);
            WriteWriteGuard {
                table: self,
                buffer: state.buffer,
                pointer,
                span: span_tuple,
                span_immutable,
                span_mutable,
            }
        }
    }

    fn append(&self, immutable: &[u8], mutable: &[u8]) -> WriteWriteGuard<'a, '_> {
        unsafe {
            assert_eq!(self.locking.kind(), LockMode::Exclusive);
            assert!(immutable.len() % 8 == 0);
            assert!(mutable.len() % 8 == 0);
            let mut active = self.active.borrow_mut();
            let n = npages(self.relation);
            if n >= 1 && !active.contains_key(&(n - 1)) {
                let buffer = pgrx::pg_sys::ReadBuffer(self.relation, n - 1);
                pgrx::pg_sys::LockBuffer(buffer, pgrx::pg_sys::BUFFER_LOCK_EXCLUSIVE as _);
                if let Some(id) = can_push(fetch(buffer), immutable, mutable) {
                    let pointer = Pointer::new(n - 1, id);
                    let span_tuple = push(fetch(buffer), immutable, mutable);
                    let span_immutable = immutable_of_tuple(fetch(buffer), span_tuple);
                    let span_mutable = mutable_of_tuple(fetch(buffer), span_tuple);
                    let mut state = WriteState::new(buffer);
                    state.mark_lower_dirty();
                    state.mark_tuple_dirty(id);
                    state.lock_write(id);
                    active.insert(pointer.page, state);
                    return WriteWriteGuard {
                        table: self,
                        buffer,
                        pointer,
                        span: span_tuple,
                        span_immutable,
                        span_mutable,
                    };
                }
                pgrx::pg_sys::UnlockReleaseBuffer(buffer);
            } else if n >= 1 {
                let state = active.get_mut(&(n - 1)).unwrap();
                if let Some(id) = can_push(fetch(state.buffer), immutable, mutable) {
                    let pointer = Pointer::new(n - 1, id);
                    let span_tuple = push(fetch(state.buffer), immutable, mutable);
                    let span_immutable = immutable_of_tuple(fetch(state.buffer), span_tuple);
                    let span_mutable = mutable_of_tuple(fetch(state.buffer), span_tuple);
                    state.mark_lower_dirty();
                    state.mark_tuple_dirty(pointer.id);
                    state.lock_write(pointer.id);
                    return WriteWriteGuard {
                        table: self,
                        buffer: state.buffer,
                        pointer,
                        span: span_tuple,
                        span_immutable,
                        span_mutable,
                    };
                }
            }
            let buffer = pgrx::pg_sys::ReadBuffer(self.relation, pgrx::pg_sys::InvalidBlockNumber);
            pgrx::pg_sys::LockBuffer(buffer, pgrx::pg_sys::BUFFER_LOCK_EXCLUSIVE as _);
            fetch(buffer).write(Page::new());
            let pointer = Pointer::new(n, 0);
            let span_tuple = push(fetch(buffer), immutable, mutable);
            let span_immutable = immutable_of_tuple(fetch(buffer), span_tuple);
            let span_mutable = mutable_of_tuple(fetch(buffer), span_tuple);
            let mut state = WriteState::new(buffer);
            state.mark_dirty();
            state.lock_write(pointer.id);
            active.insert(n, state);
            WriteWriteGuard {
                table: self,
                buffer,
                pointer,
                span: span_tuple,
                span_immutable,
                span_mutable,
            }
        }
    }
}

impl<'a> Drop for Write<'a> {
    fn drop(&mut self) {
        let active = self.active.borrow();
        let clean_pages = active.values().filter(|state| !state.is_dirty()).count();
        let dirty_pages = active.values().filter(|state| state.is_dirty()).count();
        assert!(clean_pages == 0);
        unsafe {
            let lsn = if needs_wal(self.relation, dirty_pages) {
                let active = active.clone();
                pgrx::pg_sys::XLogEnsureRecordSpace(dirty_pages as _, 20);
                match std::panic::catch_unwind(move || {
                    const RM_GENERIC_ID: pgrx::pg_sys::RmgrId = 20;
                    pgrx::pg_sys::XLogBeginInsert();
                    let mut counter = 0u8;
                    for state in active.values() {
                        match state.dirty {
                            PageDirty::Partial { lower, tuples } => {
                                let flags = pgrx::pg_sys::REGBUF_FORCE_IMAGE as u8
                                    | pgrx::pg_sys::REGBUF_STANDARD as u8;
                                let mut delta = Vec::<u8>::new();

                                for j in 0..16 {
                                    match tuples[j] {
                                        TupleDirty::Partial(TupleMutableDirty::Clean) => {}
                                        TupleDirty::Partial(TupleMutableDirty::Full) => {
                                            let span = fetch(state.buffer).tuple(j as _).unwrap();
                                            let span_immutable =
                                                immutable_of_tuple(fetch(state.buffer), span);
                                            register_span(state.buffer, &mut delta, span_immutable);
                                        }
                                        TupleDirty::Full => {
                                            let span = fetch(state.buffer).tuple(j as _).unwrap();
                                            register_span(state.buffer, &mut delta, span);
                                        }
                                    }
                                }
                                match lower {
                                    LowerDirty::Full => {
                                        register_span(
                                            state.buffer,
                                            &mut delta,
                                            fetch(state.buffer).lower(),
                                        );
                                    }
                                    LowerDirty::Clean => {}
                                }
                                if delta.len() != 0 {
                                    pgrx::pg_sys::XLogRegisterBuffer(counter, state.buffer, flags);
                                    pgrx::pg_sys::XLogRegisterBufData(
                                        counter,
                                        delta.as_mut_ptr() as _,
                                        delta.len() as i32,
                                    );
                                    counter += 1;
                                }
                            }
                            PageDirty::Full => {
                                let flags = pgrx::pg_sys::REGBUF_FORCE_IMAGE as u8
                                    | pgrx::pg_sys::REGBUF_STANDARD as u8;
                                pgrx::pg_sys::XLogRegisterBuffer(counter, state.buffer, flags);
                                counter += 1;
                            }
                        }
                    }
                    pgrx::pg_sys::XLogInsert(RM_GENERIC_ID, 0)
                }) {
                    Ok(lsn) => Some(lsn),
                    Err(_) => pgrx::PANIC!("Error while writing xlog."),
                }
            } else {
                None
            };
            for state in active.values() {
                if let Some(lsn) = lsn {
                    page_set_lsn(fetch(state.buffer), lsn);
                    pgrx::pg_sys::MarkBufferDirty(state.buffer);
                    pgrx::pg_sys::UnlockReleaseBuffer(state.buffer);
                } else {
                    pgrx::pg_sys::UnlockReleaseBuffer(state.buffer);
                }
            }
        }
    }
}

unsafe fn immutable_of_tuple(page: *mut Page, span: Span) -> Span {
    let p = page.span_cast::<TupleHeader>(span);
    let size_immutable = (*p).immutable as usize;
    span.subspan(8, size_immutable)
}

unsafe fn mutable_of_tuple(page: *mut Page, span: Span) -> Span {
    let p = page.span_cast::<TupleHeader>(span);
    let size_immutable = (*p).immutable as usize;
    let size_mutable = (*p).mutable as usize;
    span.subspan(8 + size_immutable, size_mutable)
}

pub unsafe fn page_set_lsn(page: *mut Page, lsn: u64) {
    (*page).header.lsn.0 = (lsn >> 32) as u32;
    (*page).header.lsn.1 = (lsn >> 0) as u32;
}

unsafe fn register_span(buffer: Buffer, delta: &mut Vec<u8>, span: Span) {
    let page = fetch(buffer);
    delta.extend_from_slice(&span.offset.to_le_bytes());
    delta.extend_from_slice(&span.size.to_le_bytes());
    delta.extend_from_slice(&*page.span_bytes(span));
}

unsafe fn can_push(page: *mut Page, immutable: &[u8], mutable: &[u8]) -> Option<u16> {
    let size = 8 + immutable.len() + mutable.len();
    let n = page.linps().size() / std::mem::size_of::<Span>();
    if n >= 16 {
        return None;
    }
    let space = ((*page).header.upper - (*page).header.lower) as usize;
    if size > space {
        return None;
    }
    Some(n as _)
}

unsafe fn push(page: *mut Page, immutable: &[u8], mutable: &[u8]) -> Span {
    assert!(can_push(page, immutable, mutable).is_some());
    let size = 8 + immutable.len() + mutable.len();
    let n = page.linps().size() / std::mem::size_of::<Span>();
    let offset = (*page).header.upper as usize - size;
    let span_tuple = Span::new(offset, size);
    let linp = PG_PAGE_HEADER_SIZE + n as usize * std::mem::size_of::<Span>();
    ((page as *mut u8).add(linp) as *mut Span).write(span_tuple);
    let p = page.span_cast::<TupleHeader>(span_tuple);
    (*p).magic = 998244353;
    (*p).immutable = immutable.len() as u16;
    (*p).mutable = mutable.len() as u16;
    (*page).header.lower += 4;
    (*page).header.upper -= size as u16;
    (*page.span_bytes(immutable_of_tuple(page, span_tuple))).copy_from_slice(immutable);
    (*page.span_bytes(mutable_of_tuple(page, span_tuple))).copy_from_slice(mutable);
    span_tuple
}

unsafe fn needs_wal(relation: pgrx::pg_sys::Relation, dirty_pages: usize) -> bool {
    ((*(*relation).rd_rel).relpersistence == pgrx::pg_sys::RELPERSISTENCE_PERMANENT as i8
        && (pgrx::pg_sys::wal_level > pgrx::pg_sys::WalLevel_WAL_LEVEL_MINIMAL as _
            || ((*relation).rd_createSubid == 0 && (*relation).rd_firstRelfilenodeSubid == 0)))
        && dirty_pages > 0
}

unsafe fn npages(relation: pgrx::pg_sys::Relation) -> u32 {
    pgrx::pg_sys::RelationGetNumberOfBlocksInFork(relation, pgrx::pg_sys::ForkNumber_MAIN_FORKNUM)
}

unsafe fn check(page: *mut Page) {
    assert!(PG_PAGE_HEADER_SIZE <= (*page).header.lower as usize);
    assert!((*page).header.lower as usize <= (*page).header.upper as usize);
    assert!((*page).header.upper as usize <= (*page).header.special as usize);
    assert!((*page).header.special as usize <= PG_PAGE_SIZE);
    assert!((*page).header.lower as usize % std::mem::align_of::<Span>() == 0);
    assert!((*page).header.upper as usize % 8 == 0);
    assert!((*page).header.special as usize % 8 == 0);
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

pub trait ReadGuardLike {
    fn pointer(&self) -> Pointer;
    fn immutable(&self) -> &[u8];
    fn mutable(&self) -> &[u8];
}

pub trait WriteGuardLike: ReadGuardLike {
    fn mutable_mut(&mut self) -> &mut [u8];
}

pub trait ReadLike {
    type ReadGuard<'b>: ReadGuardLike
    where
        Self: 'b;
    fn pages(&mut self) -> u32;
    fn read(&self, pointer: Pointer) -> Self::ReadGuard<'_>;
}

pub trait WriteLike: ReadLike {
    type WriteGuard<'b>: WriteGuardLike
    where
        Self: 'b;
    fn write(&self, pointer: Pointer) -> Self::WriteGuard<'_>;
    fn append(&self, immutable: &[u8], mutable: &[u8]) -> Self::WriteGuard<'_>;
}
