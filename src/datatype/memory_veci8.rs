use base::scalar::*;
use base::vector::*;
use pgrx::pg_sys::Datum;
use pgrx::pg_sys::Oid;
use pgrx::pgrx_sql_entity_graph::metadata::ArgumentError;
use pgrx::pgrx_sql_entity_graph::metadata::Returns;
use pgrx::pgrx_sql_entity_graph::metadata::ReturnsError;
use pgrx::pgrx_sql_entity_graph::metadata::SqlMapping;
use pgrx::pgrx_sql_entity_graph::metadata::SqlTranslatable;
use pgrx::FromDatum;
use pgrx::IntoDatum;
use pgrx::UnboxDatum;
use std::alloc::Layout;
use std::ops::Deref;
use std::ops::DerefMut;
use std::ptr::NonNull;

pub const HEADER_MAGIC: u16 = 4;

#[repr(C, align(8))]
pub struct Veci8Header {
    varlena: u32,
    dims: u16,
    magic: u16,
    alpha: F32,
    offset: F32,
    sum: F32,
    l2_norm: F32,
    phantom: [I8; 0],
}

impl Veci8Header {
    fn varlena(size: usize) -> u32 {
        // varattrib_4b type with compress is not set
        (size << 2) as u32
    }

    fn layout(len: usize) -> Layout {
        u16::try_from(len).expect("Vector is too large.");
        // size of struct VecI8
        let layout_alpha = Layout::new::<Veci8Header>();
        // size of data in VecI8
        let layout_beta = Layout::array::<I8>(len).unwrap();
        let layout = layout_alpha.extend(layout_beta).unwrap().0;
        layout.pad_to_align()
    }

    pub fn dims(&self) -> u32 {
        self.dims as u32
    }

    pub fn alpha(&self) -> F32 {
        self.alpha
    }

    pub fn offset(&self) -> F32 {
        self.offset
    }

    pub fn sum(&self) -> F32 {
        self.sum
    }

    pub fn l2_norm(&self) -> F32 {
        self.l2_norm
    }

    pub fn dequantization(&self) -> Vec<F32> {
        self.data()
            .iter()
            .map(|&x| x.to_f32() * self.alpha() + self.offset())
            .collect()
    }

    /// return value after dequantization by index
    /// since `index<usize>` return &Output, we can't create a new Output and return it as a reference, so we need to use this function to return a new Output directly
    #[inline(always)]
    pub fn get(&self, index: u32) -> F32 {
        self.data()[index as usize].to_f32() * self.alpha + self.offset
    }

    pub fn data(&self) -> &[I8] {
        unsafe { std::slice::from_raw_parts(self.phantom.as_ptr(), self.dims as usize) }
    }

    pub fn as_borrowed(&self) -> Veci8Borrowed<'_> {
        unsafe {
            Veci8Borrowed::new_unchecked(
                self.data(),
                self.alpha,
                self.offset,
                self.sum,
                self.l2_norm,
            )
        }
    }
}

pub enum Veci8Input<'a> {
    Owned(Veci8Output),
    Borrowed(&'a Veci8Header),
}

impl<'a> Veci8Input<'a> {
    pub unsafe fn new(p: NonNull<Veci8Header>) -> Self {
        let q = unsafe {
            NonNull::new(pgrx::pg_sys::pg_detoast_datum(p.cast().as_ptr()).cast()).unwrap()
        };
        if p != q {
            Veci8Input::Owned(Veci8Output(q))
        } else {
            unsafe { Veci8Input::Borrowed(p.as_ref()) }
        }
    }
}

impl Deref for Veci8Input<'_> {
    type Target = Veci8Header;

    fn deref(&self) -> &Self::Target {
        match self {
            Veci8Input::Owned(x) => x,
            Veci8Input::Borrowed(x) => x,
        }
    }
}

pub struct Veci8Output(NonNull<Veci8Header>);

impl Veci8Output {
    pub fn new(vector: Veci8Borrowed<'_>) -> Veci8Output {
        unsafe {
            let slice = vector.data();
            let layout = Veci8Header::layout(slice.len());
            let dims = vector.dims();
            let internal_dims = dims as u16;
            let ptr = pgrx::pg_sys::palloc(layout.size()) as *mut Veci8Header;
            ptr.cast::<u8>().add(layout.size() - 8).write_bytes(0, 8);
            std::ptr::addr_of_mut!((*ptr).varlena).write(Veci8Header::varlena(layout.size()));
            std::ptr::addr_of_mut!((*ptr).dims).write(internal_dims);
            std::ptr::addr_of_mut!((*ptr).magic).write(HEADER_MAGIC);
            std::ptr::addr_of_mut!((*ptr).alpha).write(vector.alpha());
            std::ptr::addr_of_mut!((*ptr).offset).write(vector.offset());
            std::ptr::addr_of_mut!((*ptr).sum).write(vector.sum());
            std::ptr::addr_of_mut!((*ptr).l2_norm).write(vector.l2_norm());
            std::ptr::copy_nonoverlapping(slice.as_ptr(), (*ptr).phantom.as_mut_ptr(), slice.len());
            Veci8Output(NonNull::new(ptr).unwrap())
        }
    }

    pub fn into_raw(self) -> *mut Veci8Header {
        let result = self.0.as_ptr();
        std::mem::forget(self);
        result
    }
}

impl Deref for Veci8Output {
    type Target = Veci8Header;

    fn deref(&self) -> &Self::Target {
        unsafe { self.0.as_ref() }
    }
}

impl DerefMut for Veci8Output {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.0.as_mut() }
    }
}

impl Drop for Veci8Output {
    fn drop(&mut self) {
        unsafe {
            pgrx::pg_sys::pfree(self.0.as_ptr() as _);
        }
    }
}

impl<'a> FromDatum for Veci8Input<'a> {
    unsafe fn from_polymorphic_datum(datum: Datum, is_null: bool, _typoid: Oid) -> Option<Self> {
        if is_null {
            None
        } else {
            let ptr = NonNull::new(datum.cast_mut_ptr::<Veci8Header>()).unwrap();
            unsafe { Some(Veci8Input::new(ptr)) }
        }
    }
}

impl IntoDatum for Veci8Output {
    fn into_datum(self) -> Option<Datum> {
        Some(Datum::from(self.into_raw() as *mut ()))
    }

    fn type_oid() -> Oid {
        let namespace =
            pgrx::pg_catalog::PgNamespace::search_namespacename(crate::SCHEMA_C_STR).unwrap();
        let namespace = namespace.get().expect("pgvecto.rs is not installed.");
        let t = pgrx::pg_catalog::PgType::search_typenamensp(c"veci8", namespace.oid()).unwrap();
        let t = t.get().expect("pg_catalog is broken.");
        t.oid()
    }
}

impl FromDatum for Veci8Output {
    unsafe fn from_polymorphic_datum(datum: Datum, is_null: bool, _typoid: Oid) -> Option<Self> {
        if is_null {
            None
        } else {
            let p = NonNull::new(datum.cast_mut_ptr::<Veci8Header>())?;
            let q =
                unsafe { NonNull::new(pgrx::pg_sys::pg_detoast_datum(p.cast().as_ptr()).cast())? };
            if p != q {
                Some(Veci8Output(q))
            } else {
                let header = p.as_ptr();
                let vector = unsafe { (*header).as_borrowed() };
                Some(Veci8Output::new(vector))
            }
        }
    }
}

unsafe impl UnboxDatum for Veci8Output {
    type As<'src> = Veci8Output;
    #[inline]
    unsafe fn unbox<'src>(d: pgrx::Datum<'src>) -> Self::As<'src>
    where
        Self: 'src,
    {
        let p = NonNull::new(d.sans_lifetime().cast_mut_ptr::<Veci8Header>()).unwrap();
        let q = unsafe {
            NonNull::new(pgrx::pg_sys::pg_detoast_datum(p.cast().as_ptr()).cast()).unwrap()
        };
        if p != q {
            Veci8Output(q)
        } else {
            let header = p.as_ptr();
            let vector = unsafe { (*header).as_borrowed() };
            Veci8Output::new(vector)
        }
    }
}

unsafe impl SqlTranslatable for Veci8Input<'_> {
    fn argument_sql() -> Result<SqlMapping, ArgumentError> {
        Ok(SqlMapping::As(String::from("veci8")))
    }

    fn return_sql() -> Result<Returns, ReturnsError> {
        Ok(Returns::One(SqlMapping::As(String::from("veci8"))))
    }
}

unsafe impl SqlTranslatable for Veci8Output {
    fn argument_sql() -> Result<SqlMapping, ArgumentError> {
        Ok(SqlMapping::As(String::from("veci8")))
    }

    fn return_sql() -> Result<Returns, ReturnsError> {
        Ok(Returns::One(SqlMapping::As(String::from("veci8"))))
    }
}

unsafe impl pgrx::callconv::BoxRet for Veci8Output {
    unsafe fn box_in_fcinfo(self, fcinfo: pgrx::pg_sys::FunctionCallInfo) -> Datum {
        self.into_datum()
            .unwrap_or_else(|| unsafe { pgrx::fcinfo::pg_return_null(fcinfo) })
    }
}
