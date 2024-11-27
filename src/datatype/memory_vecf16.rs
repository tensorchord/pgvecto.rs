use base::vector::*;
use half::f16;
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
use std::ptr::NonNull;

pub const HEADER_MAGIC: u16 = 1;

#[repr(C, align(8))]
pub struct Vecf16Header {
    varlena: u32,
    dims: u16,
    magic: u16,
    phantom: [f16; 0],
}

impl Vecf16Header {
    fn varlena(size: usize) -> u32 {
        (size << 2) as u32
    }
    fn layout(len: usize) -> Layout {
        u16::try_from(len).expect("Vector is too large.");
        let layout_alpha = Layout::new::<Vecf16Header>();
        let layout_beta = Layout::array::<f16>(len).unwrap();
        let layout = layout_alpha.extend(layout_beta).unwrap().0;
        layout.pad_to_align()
    }
    pub fn dims(&self) -> u32 {
        self.dims as u32
    }
    pub fn slice(&self) -> &[f16] {
        unsafe { std::slice::from_raw_parts(self.phantom.as_ptr(), self.dims as usize) }
    }
    pub fn as_borrowed(&self) -> VectBorrowed<'_, f16> {
        unsafe { VectBorrowed::new_unchecked(self.slice()) }
    }
}

impl Deref for Vecf16Header {
    type Target = [f16];

    fn deref(&self) -> &Self::Target {
        self.slice()
    }
}

pub enum Vecf16Input<'a> {
    Owned(Vecf16Output),
    Borrowed(&'a Vecf16Header),
}

impl<'a> Vecf16Input<'a> {
    unsafe fn new(p: NonNull<Vecf16Header>) -> Self {
        let q = unsafe {
            NonNull::new(pgrx::pg_sys::pg_detoast_datum(p.cast().as_ptr()).cast()).unwrap()
        };
        if p != q {
            Vecf16Input::Owned(Vecf16Output(q))
        } else {
            unsafe { Vecf16Input::Borrowed(p.as_ref()) }
        }
    }
}

impl Deref for Vecf16Input<'_> {
    type Target = Vecf16Header;

    fn deref(&self) -> &Self::Target {
        match self {
            Vecf16Input::Owned(x) => x,
            Vecf16Input::Borrowed(x) => x,
        }
    }
}

pub struct Vecf16Output(NonNull<Vecf16Header>);

impl Vecf16Output {
    pub fn new(vector: VectBorrowed<'_, f16>) -> Vecf16Output {
        unsafe {
            let slice = vector.slice();
            let layout = Vecf16Header::layout(slice.len());
            let dims = vector.dims();
            let internal_dims = dims as u16;
            let ptr = pgrx::pg_sys::palloc(layout.size()) as *mut Vecf16Header;
            ptr.cast::<u8>().add(layout.size() - 8).write_bytes(0, 8);
            (&raw mut (*ptr).varlena).write(Vecf16Header::varlena(layout.size()));
            (&raw mut (*ptr).magic).write(HEADER_MAGIC);
            (&raw mut (*ptr).dims).write(internal_dims);
            std::ptr::copy_nonoverlapping(slice.as_ptr(), (*ptr).phantom.as_mut_ptr(), slice.len());
            Vecf16Output(NonNull::new(ptr).unwrap())
        }
    }
    pub fn into_raw(self) -> *mut Vecf16Header {
        let result = self.0.as_ptr();
        std::mem::forget(self);
        result
    }
}

impl Deref for Vecf16Output {
    type Target = Vecf16Header;

    fn deref(&self) -> &Self::Target {
        unsafe { self.0.as_ref() }
    }
}

impl Drop for Vecf16Output {
    fn drop(&mut self) {
        unsafe {
            pgrx::pg_sys::pfree(self.0.as_ptr() as _);
        }
    }
}

impl<'a> FromDatum for Vecf16Input<'a> {
    unsafe fn from_polymorphic_datum(datum: Datum, is_null: bool, _typoid: Oid) -> Option<Self> {
        if is_null {
            None
        } else {
            let ptr = NonNull::new(datum.cast_mut_ptr::<Vecf16Header>()).unwrap();
            unsafe { Some(Vecf16Input::new(ptr)) }
        }
    }
}

impl IntoDatum for Vecf16Output {
    fn into_datum(self) -> Option<Datum> {
        Some(Datum::from(self.into_raw() as *mut ()))
    }

    fn type_oid() -> Oid {
        let namespace =
            pgrx_catalog::PgNamespace::search_namespacename(crate::SCHEMA_C_STR).unwrap();
        let namespace = namespace.get().expect("pgvecto.rs is not installed.");
        let t = pgrx_catalog::PgType::search_typenamensp(c"vecf16", namespace.oid()).unwrap();
        let t = t.get().expect("pg_catalog is broken.");
        t.oid()
    }
}

impl FromDatum for Vecf16Output {
    unsafe fn from_polymorphic_datum(datum: Datum, is_null: bool, _typoid: Oid) -> Option<Self> {
        if is_null {
            None
        } else {
            let p = NonNull::new(datum.cast_mut_ptr::<Vecf16Header>())?;
            let q =
                unsafe { NonNull::new(pgrx::pg_sys::pg_detoast_datum(p.cast().as_ptr()).cast())? };
            if p != q {
                Some(Vecf16Output(q))
            } else {
                let header = p.as_ptr();
                let vector = unsafe { (*header).as_borrowed() };
                Some(Vecf16Output::new(vector))
            }
        }
    }
}

unsafe impl pgrx::datum::UnboxDatum for Vecf16Output {
    type As<'src> = Vecf16Output;
    #[inline]
    unsafe fn unbox<'src>(d: pgrx::datum::Datum<'src>) -> Self::As<'src>
    where
        Self: 'src,
    {
        let p = NonNull::new(d.sans_lifetime().cast_mut_ptr::<Vecf16Header>()).unwrap();
        let q = unsafe {
            NonNull::new(pgrx::pg_sys::pg_detoast_datum(p.cast().as_ptr()).cast()).unwrap()
        };
        if p != q {
            Vecf16Output(q)
        } else {
            let header = p.as_ptr();
            let vector = unsafe { (*header).as_borrowed() };
            Vecf16Output::new(vector)
        }
    }
}

unsafe impl SqlTranslatable for Vecf16Input<'_> {
    fn argument_sql() -> Result<SqlMapping, ArgumentError> {
        Ok(SqlMapping::As(String::from("vecf16")))
    }
    fn return_sql() -> Result<Returns, ReturnsError> {
        Ok(Returns::One(SqlMapping::As(String::from("vecf16"))))
    }
}

unsafe impl SqlTranslatable for Vecf16Output {
    fn argument_sql() -> Result<SqlMapping, ArgumentError> {
        Ok(SqlMapping::As(String::from("vecf16")))
    }
    fn return_sql() -> Result<Returns, ReturnsError> {
        Ok(Returns::One(SqlMapping::As(String::from("vecf16"))))
    }
}

unsafe impl<'fcx> pgrx::callconv::ArgAbi<'fcx> for Vecf16Input<'fcx> {
    unsafe fn unbox_arg_unchecked(arg: pgrx::callconv::Arg<'_, 'fcx>) -> Self {
        unsafe { arg.unbox_arg_using_from_datum().unwrap() }
    }
}

unsafe impl pgrx::callconv::BoxRet for Vecf16Output {
    unsafe fn box_into<'fcx>(
        self,
        fcinfo: &mut pgrx::callconv::FcInfo<'fcx>,
    ) -> pgrx::datum::Datum<'fcx> {
        unsafe { fcinfo.return_raw_datum(Datum::from(self.into_raw() as *mut ())) }
    }
}
