use base::vector::*;
use pgrx::datum::FromDatum;
use pgrx::datum::IntoDatum;
use pgrx::pg_sys::Datum;
use pgrx::pg_sys::Oid;
use pgrx::pgrx_sql_entity_graph::metadata::ArgumentError;
use pgrx::pgrx_sql_entity_graph::metadata::Returns;
use pgrx::pgrx_sql_entity_graph::metadata::ReturnsError;
use pgrx::pgrx_sql_entity_graph::metadata::SqlMapping;
use pgrx::pgrx_sql_entity_graph::metadata::SqlTranslatable;
use std::alloc::Layout;
use std::ops::Deref;
use std::ops::DerefMut;
use std::ptr::NonNull;

pub const HEADER_MAGIC: u16 = 3;

#[repr(C, align(8))]
pub struct BVectorHeader {
    varlena: u32,
    dims: u16,
    magic: u16,
    phantom: [u64; 0],
}

impl BVectorHeader {
    fn varlena(size: usize) -> u32 {
        (size << 2) as u32
    }
    fn layout(len: usize) -> Layout {
        u16::try_from(len).expect("Vector is too large.");
        let layout_alpha = Layout::new::<BVectorHeader>();
        let layout_beta = Layout::array::<u64>((len as u32).div_ceil(BVECTOR_WIDTH) as _).unwrap();
        let layout = layout_alpha.extend(layout_beta).unwrap().0;
        layout.pad_to_align()
    }
    pub fn dims(&self) -> u32 {
        self.dims as u32
    }
    pub fn data(&self) -> &[u64] {
        unsafe {
            std::slice::from_raw_parts(
                self.phantom.as_ptr(),
                (self.dims as u32).div_ceil(BVECTOR_WIDTH) as usize,
            )
        }
    }
    pub fn as_borrowed(&self) -> BVectBorrowed<'_> {
        unsafe { BVectBorrowed::new_unchecked(self.dims(), self.data()) }
    }
}

pub enum BVectorInput<'a> {
    Owned(BVectorOutput),
    Borrowed(&'a BVectorHeader),
}

impl<'a> BVectorInput<'a> {
    pub unsafe fn new(p: NonNull<BVectorHeader>) -> Self {
        let q = unsafe {
            NonNull::new(pgrx::pg_sys::pg_detoast_datum(p.cast().as_ptr()).cast()).unwrap()
        };
        if p != q {
            BVectorInput::Owned(BVectorOutput(q))
        } else {
            unsafe { BVectorInput::Borrowed(p.as_ref()) }
        }
    }
}

impl Deref for BVectorInput<'_> {
    type Target = BVectorHeader;

    fn deref(&self) -> &Self::Target {
        match self {
            BVectorInput::Owned(x) => x,
            BVectorInput::Borrowed(x) => x,
        }
    }
}

pub struct BVectorOutput(NonNull<BVectorHeader>);

impl BVectorOutput {
    pub fn new(vector: BVectBorrowed<'_>) -> BVectorOutput {
        unsafe {
            let dims = vector.dims();
            let internal_dims = dims as u16;
            let layout = BVectorHeader::layout(dims as usize);
            let ptr = pgrx::pg_sys::palloc(layout.size()) as *mut BVectorHeader;
            ptr.cast::<u8>().add(layout.size() - 8).write_bytes(0, 8);
            (&raw mut (*ptr).varlena).write(BVectorHeader::varlena(layout.size()));
            (&raw mut (*ptr).magic).write(HEADER_MAGIC);
            (&raw mut (*ptr).dims).write(internal_dims);
            std::ptr::copy_nonoverlapping(
                vector.data().as_ptr(),
                (*ptr).phantom.as_mut_ptr(),
                dims.div_ceil(BVECTOR_WIDTH) as usize,
            );
            BVectorOutput(NonNull::new(ptr).unwrap())
        }
    }
    pub fn into_raw(self) -> *mut BVectorHeader {
        let result = self.0.as_ptr();
        std::mem::forget(self);
        result
    }
}

impl Deref for BVectorOutput {
    type Target = BVectorHeader;

    fn deref(&self) -> &Self::Target {
        unsafe { self.0.as_ref() }
    }
}

impl DerefMut for BVectorOutput {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.0.as_mut() }
    }
}

impl Drop for BVectorOutput {
    fn drop(&mut self) {
        unsafe {
            pgrx::pg_sys::pfree(self.0.as_ptr() as _);
        }
    }
}

impl<'a> FromDatum for BVectorInput<'a> {
    unsafe fn from_polymorphic_datum(datum: Datum, is_null: bool, _typoid: Oid) -> Option<Self> {
        if is_null {
            None
        } else {
            let ptr = NonNull::new(datum.cast_mut_ptr::<BVectorHeader>()).unwrap();
            unsafe { Some(BVectorInput::new(ptr)) }
        }
    }
}

impl IntoDatum for BVectorOutput {
    fn into_datum(self) -> Option<Datum> {
        Some(Datum::from(self.into_raw() as *mut ()))
    }

    fn type_oid() -> Oid {
        let namespace =
            pgrx::pg_catalog::PgNamespace::search_namespacename(crate::SCHEMA_C_STR).unwrap();
        let namespace = namespace.get().expect("pgvecto.rs is not installed.");
        let t = pgrx::pg_catalog::PgType::search_typenamensp(c"bvector", namespace.oid()).unwrap();
        let t = t.get().expect("pg_catalog is broken.");
        t.oid()
    }
}

impl FromDatum for BVectorOutput {
    unsafe fn from_polymorphic_datum(datum: Datum, is_null: bool, _typoid: Oid) -> Option<Self> {
        if is_null {
            None
        } else {
            let p = NonNull::new(datum.cast_mut_ptr::<BVectorHeader>())?;
            let q =
                unsafe { NonNull::new(pgrx::pg_sys::pg_detoast_datum(p.cast().as_ptr()).cast())? };
            if p != q {
                Some(BVectorOutput(q))
            } else {
                let header = p.as_ptr();
                let vector = unsafe { (*header).as_borrowed() };
                Some(BVectorOutput::new(vector))
            }
        }
    }
}

unsafe impl pgrx::datum::UnboxDatum for BVectorOutput {
    type As<'src> = BVectorOutput;
    #[inline]
    unsafe fn unbox<'src>(d: pgrx::datum::Datum<'src>) -> Self::As<'src>
    where
        Self: 'src,
    {
        let p = NonNull::new(d.sans_lifetime().cast_mut_ptr::<BVectorHeader>()).unwrap();
        let q = unsafe {
            NonNull::new(pgrx::pg_sys::pg_detoast_datum(p.cast().as_ptr()).cast()).unwrap()
        };
        if p != q {
            BVectorOutput(q)
        } else {
            let header = p.as_ptr();
            let vector = unsafe { (*header).as_borrowed() };
            BVectorOutput::new(vector)
        }
    }
}

unsafe impl SqlTranslatable for BVectorInput<'_> {
    fn argument_sql() -> Result<SqlMapping, ArgumentError> {
        Ok(SqlMapping::As(String::from("bvector")))
    }
    fn return_sql() -> Result<Returns, ReturnsError> {
        Ok(Returns::One(SqlMapping::As(String::from("bvector"))))
    }
}

unsafe impl SqlTranslatable for BVectorOutput {
    fn argument_sql() -> Result<SqlMapping, ArgumentError> {
        Ok(SqlMapping::As(String::from("bvector")))
    }
    fn return_sql() -> Result<Returns, ReturnsError> {
        Ok(Returns::One(SqlMapping::As(String::from("bvector"))))
    }
}

unsafe impl<'fcx> pgrx::callconv::ArgAbi<'fcx> for BVectorInput<'fcx> {
    unsafe fn unbox_arg_unchecked(arg: pgrx::callconv::Arg<'_, 'fcx>) -> Self {
        unsafe { arg.unbox_arg_using_from_datum().unwrap() }
    }
}

unsafe impl pgrx::callconv::BoxRet for BVectorOutput {
    unsafe fn box_into<'fcx>(
        self,
        fcinfo: &mut pgrx::callconv::FcInfo<'fcx>,
    ) -> pgrx::datum::Datum<'fcx> {
        unsafe { fcinfo.return_raw_datum(Datum::from(self.into_raw() as *mut ())) }
    }
}
