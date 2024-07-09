use crate::index::*;
use crate::scalar::F32;
use crate::search::*;
use crate::vector::*;

pub trait WorkerOperations {
    fn create(
        &self,
        handle: Handle,
        options: IndexOptions,
        alterable_options: IndexAlterableOptions,
    ) -> Result<(), CreateError>;
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
    fn alter(&self, handle: Handle, key: &str, value: &str) -> Result<(), AlterError>;
    fn stop(&self, handle: Handle) -> Result<(), StopError>;
    fn start(&self, handle: Handle) -> Result<(), StartError>;
}

pub trait ViewBasicOperations {
    fn basic<'a>(
        &'a self,
        vector: &'a OwnedVector,
        opts: &'a SearchOptions,
    ) -> Result<Box<dyn Iterator<Item = (F32, Pointer)> + 'a>, BasicError>;
}

pub trait ViewVbaseOperations {
    fn vbase<'a>(
        &'a self,
        vector: &'a OwnedVector,
        opts: &'a SearchOptions,
    ) -> Result<Box<dyn Iterator<Item = (F32, Pointer)> + 'a>, VbaseError>;
}

pub trait ViewListOperations {
    fn list(&self) -> Result<Box<dyn Iterator<Item = Pointer> + '_>, ListError>;
}
