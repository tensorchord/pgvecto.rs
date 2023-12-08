use crate::datatype::typmod::Typmod;
use pgrx::pg_sys::Datum;
use pgrx::pg_sys::Oid;
use pgrx::pgrx_sql_entity_graph::metadata::ArgumentError;
use pgrx::pgrx_sql_entity_graph::metadata::Returns;
use pgrx::pgrx_sql_entity_graph::metadata::ReturnsError;
use pgrx::pgrx_sql_entity_graph::metadata::SqlMapping;
use pgrx::pgrx_sql_entity_graph::metadata::SqlTranslatable;
use pgrx::FromDatum;
use pgrx::IntoDatum;
use service::prelude::*;
use std::alloc::Layout;
use std::cmp::Ordering;
use std::ffi::CStr;
use std::ffi::CString;
use std::ops::Deref;
use std::ops::DerefMut;
use std::ops::Index;
use std::ops::IndexMut;
use std::ptr::NonNull;

pgrx::extension_sql!(
    r#"
CREATE TYPE vecf16 (
    INPUT     = vecf16_in,
    OUTPUT    = vecf16_out,
    TYPMOD_IN = typmod_in,
    TYPMOD_OUT = typmod_out,
    STORAGE   = EXTENDED,
    INTERNALLENGTH = VARIABLE,
    ALIGNMENT = double
);
"#,
    name = "vecf16",
    creates = [Type(Vecf16)],
    requires = [vecf16_in, vecf16_out, typmod_in, typmod_out],
);

#[repr(C, align(8))]
pub struct Vecf16 {
    varlena: u32,
    kind: u8,
    pad: u8,
    len: u16,
    phantom: [F16; 0],
}

impl Vecf16 {
    fn varlena(size: usize) -> u32 {
        (size << 2) as u32
    }
    fn layout(len: usize) -> Layout {
        u16::try_from(len).expect("Vector is too large.");
        let layout_alpha = Layout::new::<Vecf16>();
        let layout_beta = Layout::array::<F16>(len).unwrap();
        let layout = layout_alpha.extend(layout_beta).unwrap().0;
        layout.pad_to_align()
    }
    pub fn new_in_postgres(slice: &[F16]) -> Vecf16Output {
        unsafe {
            assert!(u16::try_from(slice.len()).is_ok());
            let layout = Vecf16::layout(slice.len());
            let ptr = pgrx::pg_sys::palloc(layout.size()) as *mut Vecf16;
            ptr.cast::<u8>().add(layout.size() - 8).write_bytes(0, 8);
            std::ptr::addr_of_mut!((*ptr).varlena).write(Vecf16::varlena(layout.size()));
            std::ptr::addr_of_mut!((*ptr).kind).write(16);
            std::ptr::addr_of_mut!((*ptr).pad).write(0);
            std::ptr::addr_of_mut!((*ptr).len).write(slice.len() as u16);
            std::ptr::copy_nonoverlapping(slice.as_ptr(), (*ptr).phantom.as_mut_ptr(), slice.len());
            Vecf16Output(NonNull::new(ptr).unwrap())
        }
    }
    pub fn len(&self) -> usize {
        self.len as usize
    }
    pub fn data(&self) -> &[F16] {
        debug_assert_eq!(self.varlena & 3, 0);
        debug_assert_eq!(self.kind, 16);
        unsafe { std::slice::from_raw_parts(self.phantom.as_ptr(), self.len as usize) }
    }
    pub fn data_mut(&mut self) -> &mut [F16] {
        debug_assert_eq!(self.varlena & 3, 0);
        debug_assert_eq!(self.kind, 16);
        unsafe { std::slice::from_raw_parts_mut(self.phantom.as_mut_ptr(), self.len as usize) }
    }
}

impl Deref for Vecf16 {
    type Target = [F16];

    fn deref(&self) -> &Self::Target {
        self.data()
    }
}

impl DerefMut for Vecf16 {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.data_mut()
    }
}

impl AsRef<[F16]> for Vecf16 {
    fn as_ref(&self) -> &[F16] {
        self.data()
    }
}

impl AsMut<[F16]> for Vecf16 {
    fn as_mut(&mut self) -> &mut [F16] {
        self.data_mut()
    }
}

impl Index<usize> for Vecf16 {
    type Output = F16;

    fn index(&self, index: usize) -> &Self::Output {
        self.data().index(index)
    }
}

impl IndexMut<usize> for Vecf16 {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        self.data_mut().index_mut(index)
    }
}

impl PartialEq for Vecf16 {
    fn eq(&self, other: &Self) -> bool {
        if self.len() != other.len() {
            return false;
        }
        let n = self.len();
        for i in 0..n {
            if self[i] != other[i] {
                return false;
            }
        }
        true
    }
}

impl Eq for Vecf16 {}

impl PartialOrd for Vecf16 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Vecf16 {
    fn cmp(&self, other: &Self) -> Ordering {
        use Ordering::*;
        if let x @ Less | x @ Greater = self.len().cmp(&other.len()) {
            return x;
        }
        let n = self.len();
        for i in 0..n {
            if let x @ Less | x @ Greater = self[i].cmp(&other[i]) {
                return x;
            }
        }
        Equal
    }
}

pub enum Vecf16Input<'a> {
    Owned(Vecf16Output),
    Borrowed(&'a Vecf16),
}

impl<'a> Vecf16Input<'a> {
    pub unsafe fn new(p: NonNull<Vecf16>) -> Self {
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
    type Target = Vecf16;

    fn deref(&self) -> &Self::Target {
        match self {
            Vecf16Input::Owned(x) => x,
            Vecf16Input::Borrowed(x) => x,
        }
    }
}

pub struct Vecf16Output(NonNull<Vecf16>);

impl Vecf16Output {
    pub fn into_raw(self) -> *mut Vecf16 {
        let result = self.0.as_ptr();
        std::mem::forget(self);
        result
    }
}

impl Deref for Vecf16Output {
    type Target = Vecf16;

    fn deref(&self) -> &Self::Target {
        unsafe { self.0.as_ref() }
    }
}

impl DerefMut for Vecf16Output {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.0.as_mut() }
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
            let ptr = NonNull::new(datum.cast_mut_ptr::<Vecf16>()).unwrap();
            unsafe { Some(Vecf16Input::new(ptr)) }
        }
    }
}

impl IntoDatum for Vecf16Output {
    fn into_datum(self) -> Option<Datum> {
        Some(Datum::from(self.into_raw() as *mut ()))
    }

    fn type_oid() -> Oid {
        pgrx::wrappers::regtypein("vecf16")
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

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn vecf16_in(input: &CStr, _oid: Oid, typmod: i32) -> Vecf16Output {
    fn solve<T>(option: Option<T>, hint: &str) -> T {
        if let Some(x) = option {
            x
        } else {
            FriendlyError::BadLiteral {
                hint: hint.to_string(),
            }
            .friendly()
        }
    }
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum State {
        MatchingLeft,
        Reading,
        MatchedRight,
    }
    use State::*;
    let input = input.to_bytes();
    let typmod = Typmod::parse_from_i32(typmod).unwrap();
    let mut vector = Vec::<F16>::with_capacity(typmod.dims().unwrap_or(0) as usize);
    let mut state = MatchingLeft;
    let mut token: Option<String> = None;
    for &c in input {
        match (state, c) {
            (MatchingLeft, b'[') => {
                state = Reading;
            }
            (Reading, b'0'..=b'9' | b'.' | b'e' | b'+' | b'-') => {
                let token = token.get_or_insert(String::new());
                token.push(char::from_u32(c as u32).unwrap());
            }
            (Reading, b',') => {
                let token = solve(token.take(), "Expect a number.");
                vector.push(solve(token.parse().ok(), "Bad number."));
            }
            (Reading, b']') => {
                if let Some(token) = token.take() {
                    vector.push(solve(token.parse().ok(), "Bad number."));
                }
                state = MatchedRight;
            }
            (_, b' ') => {}
            _ => {
                FriendlyError::BadLiteral {
                    hint: format!("Bad charactor with ascii {:#x}.", c),
                }
                .friendly();
            }
        }
    }
    if state != MatchedRight {
        FriendlyError::BadLiteral {
            hint: "Bad sequence.".to_string(),
        }
        .friendly();
    }
    if vector.is_empty() || vector.len() > 65535 {
        FriendlyError::BadValueDimensions.friendly();
    }
    Vecf16::new_in_postgres(&vector)
}

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn vecf16_out(vector: Vecf16Input<'_>) -> CString {
    let mut buffer = String::new();
    buffer.push('[');
    if let Some(&x) = vector.data().first() {
        buffer.push_str(format!("{}", x).as_str());
    }
    for &x in vector.data().iter().skip(1) {
        buffer.push_str(format!(", {}", x).as_str());
    }
    buffer.push(']');
    CString::new(buffer).unwrap()
}
