use crate::error::*;
use crate::index::*;
use crate::search::*;
use crate::vector::*;

pub trait WorkerOperations {
    fn create(&self, handle: Handle, options: IndexOptions) -> Result<(), CreateError>;
    fn drop(&self, handle: Handle) -> Result<(), DropError>;
    fn flush(&self, handle: Handle) -> Result<(), FlushError>;
    fn insert(
        &self,
        handle: Handle,
        vector: OwnedVector,
        pointer: Pointer,
    ) -> Result<(), InsertError>;
    fn delete(&self, handle: Handle, pointer: Pointer) -> Result<(), DeleteError>;
    fn view_basic(&self, handle: Handle) -> Result<impl ViewBasicOperations, BasicError>;
    fn view_vbase(&self, handle: Handle) -> Result<impl ViewVbaseOperations, VbaseError>;
    fn view_list(&self, handle: Handle) -> Result<impl ViewListOperations, ListError>;
    fn stat(&self, handle: Handle) -> Result<IndexStat, StatError>;
}

pub trait ViewBasicOperations {
    fn basic<'a, F: Fn(Pointer) -> bool + Clone + 'a>(
        &'a self,
        vector: &'a OwnedVector,
        opts: &'a SearchOptions,
        filter: F,
    ) -> Result<Box<dyn Iterator<Item = Pointer> + 'a>, BasicError>;
}

pub trait ViewVbaseOperations {
    fn vbase<'a, F: FnMut(Pointer) -> bool + Clone + 'a>(
        &'a self,
        vector: &'a OwnedVector,
        opts: &'a SearchOptions,
        filter: F,
    ) -> Result<Box<dyn Iterator<Item = Pointer> + 'a>, VbaseError>;
}

pub trait ViewListOperations {
    fn list(&self) -> Result<Box<dyn Iterator<Item = Pointer> + '_>, ListError>;
}
