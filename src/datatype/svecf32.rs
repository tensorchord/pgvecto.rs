use crate::prelude::*;
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

#[repr(C, align(8))]
pub struct SVecf32 {
    varlena: u32,
    len: u16,
    kind: u8,
    reserved: u8,
    dims: u16,
    phantom: [SparseF32Element; 0],
}

impl SVecf32 {
    fn varlena(size: usize) -> u32 {
        (size << 2) as u32
    }
    fn layout(len: usize) -> Layout {
        u16::try_from(len).expect("Vector is too large.");
        let layout_alpha = Layout::new::<SVecf32>();
        let layout_beta = Layout::array::<SparseF32Element>(len).unwrap();
        let layout = layout_alpha.extend(layout_beta).unwrap().0;
        layout.pad_to_align()
    }
    pub fn new_in_postgres(slice: &[SparseF32Element], dims: u16) -> SVecf32Output {
        unsafe {
            assert!(u16::try_from(slice.len()).is_ok());
            let layout = SVecf32::layout(slice.len());
            let ptr = pgrx::pg_sys::palloc(layout.size()) as *mut SVecf32;
            ptr.cast::<u8>().add(layout.size() - 8).write_bytes(0, 8);
            std::ptr::addr_of_mut!((*ptr).varlena).write(SVecf32::varlena(layout.size()));
            std::ptr::addr_of_mut!((*ptr).dims).write(dims);
            std::ptr::addr_of_mut!((*ptr).kind).write(2);
            std::ptr::addr_of_mut!((*ptr).reserved).write(0);
            std::ptr::addr_of_mut!((*ptr).len).write(slice.len() as u16);
            std::ptr::copy_nonoverlapping(slice.as_ptr(), (*ptr).phantom.as_mut_ptr(), slice.len());
            SVecf32Output(NonNull::new(ptr).unwrap())
        }
    }
    pub fn dims(&self) -> u16 {
        self.dims
    }
    pub fn len(&self) -> usize {
        self.len as usize
    }
    pub fn data(&self) -> &[SparseF32Element] {
        debug_assert_eq!(self.varlena & 3, 0);
        debug_assert_eq!(self.kind, 2);
        unsafe { std::slice::from_raw_parts(self.phantom.as_ptr(), self.len as usize) }
    }
    pub fn data_mut(&mut self) -> &mut [SparseF32Element] {
        debug_assert_eq!(self.varlena & 3, 0);
        debug_assert_eq!(self.kind, 2);
        unsafe { std::slice::from_raw_parts_mut(self.phantom.as_mut_ptr(), self.len as usize) }
    }
}

impl PartialEq for SVecf32 {
    fn eq(&self, other: &Self) -> bool {
        self.data() == other.data()
    }
}

impl Eq for SVecf32 {}

impl PartialOrd for SVecf32 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SVecf32 {
    fn cmp(&self, other: &Self) -> Ordering {
        use Ordering::*;
        let n = std::cmp::min(self.len(), other.len());
        if let x @ Less | x @ Greater = self.data()[..n].cmp(&other.data()[..n]) {
            return x;
        }
        self.len().cmp(&other.len())
    }
}

impl Deref for SVecf32 {
    type Target = [SparseF32Element];

    fn deref(&self) -> &Self::Target {
        self.data()
    }
}

impl DerefMut for SVecf32 {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.data_mut()
    }
}

impl AsRef<[SparseF32Element]> for SVecf32 {
    fn as_ref(&self) -> &[SparseF32Element] {
        self.data()
    }
}

impl AsMut<[SparseF32Element]> for SVecf32 {
    fn as_mut(&mut self) -> &mut [SparseF32Element] {
        self.data_mut()
    }
}

impl Index<usize> for SVecf32 {
    type Output = SparseF32Element;

    fn index(&self, index: usize) -> &Self::Output {
        self.data().index(index)
    }
}

impl IndexMut<usize> for SVecf32 {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        self.data_mut().index_mut(index)
    }
}

pub enum SVecf32Input<'a> {
    Owned(SVecf32Output),
    Borrowed(&'a SVecf32),
}

impl<'a> SVecf32Input<'a> {
    pub unsafe fn new(p: NonNull<SVecf32>) -> Self {
        let q = unsafe {
            NonNull::new(pgrx::pg_sys::pg_detoast_datum(p.cast().as_ptr()).cast()).unwrap()
        };
        if p != q {
            SVecf32Input::Owned(SVecf32Output(q))
        } else {
            unsafe { SVecf32Input::Borrowed(p.as_ref()) }
        }
    }
}

impl Deref for SVecf32Input<'_> {
    type Target = SVecf32;

    fn deref(&self) -> &Self::Target {
        match self {
            SVecf32Input::Owned(x) => x,
            SVecf32Input::Borrowed(x) => x,
        }
    }
}

pub struct SVecf32Output(NonNull<SVecf32>);

impl SVecf32Output {
    pub fn into_raw(self) -> *mut SVecf32 {
        let result = self.0.as_ptr();
        std::mem::forget(self);
        result
    }
}

impl Deref for SVecf32Output {
    type Target = SVecf32;

    fn deref(&self) -> &Self::Target {
        unsafe { self.0.as_ref() }
    }
}

impl DerefMut for SVecf32Output {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.0.as_mut() }
    }
}

impl Drop for SVecf32Output {
    fn drop(&mut self) {
        unsafe {
            pgrx::pg_sys::pfree(self.0.as_ptr() as _);
        }
    }
}

impl<'a> FromDatum for SVecf32Input<'a> {
    unsafe fn from_polymorphic_datum(datum: Datum, is_null: bool, _typoid: Oid) -> Option<Self> {
        if is_null {
            None
        } else {
            let ptr = NonNull::new(datum.cast_mut_ptr::<SVecf32>()).unwrap();
            unsafe { Some(SVecf32Input::new(ptr)) }
        }
    }
}

impl IntoDatum for SVecf32Output {
    fn into_datum(self) -> Option<Datum> {
        Some(Datum::from(self.into_raw() as *mut ()))
    }

    fn type_oid() -> Oid {
        pgrx::wrappers::regtypein("vectors.svecf32")
    }
}

unsafe impl SqlTranslatable for SVecf32Input<'_> {
    fn argument_sql() -> Result<SqlMapping, ArgumentError> {
        Ok(SqlMapping::As(String::from("svector")))
    }
    fn return_sql() -> Result<Returns, ReturnsError> {
        Ok(Returns::One(SqlMapping::As(String::from("svector"))))
    }
}

unsafe impl SqlTranslatable for SVecf32Output {
    fn argument_sql() -> Result<SqlMapping, ArgumentError> {
        Ok(SqlMapping::As(String::from("svector")))
    }
    fn return_sql() -> Result<Returns, ReturnsError> {
        Ok(Returns::One(SqlMapping::As(String::from("svector"))))
    }
}

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn _vectors_svecf32_in(input: &CStr, _oid: Oid, _typmod: i32) -> SVecf32Output {
    fn solve<T>(option: Option<T>, hint: &str) -> T {
        if let Some(x) = option {
            x
        } else {
            SessionError::BadLiteral {
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
    let mut vector = Vec::<SparseF32Element>::new();
    let mut state = MatchingLeft;
    let mut token: Option<String> = None;
    let mut index = 0;
    for &c in input {
        match (state, c) {
            (MatchingLeft, b'[') => {
                state = Reading;
            }
            (Reading, b'0'..=b'9' | b'a'..=b'z' | b'A'..=b'Z' | b'.' | b'+' | b'-') => {
                let token = token.get_or_insert(String::new());
                token.push(char::from_u32(c as u32).unwrap());
            }
            (Reading, b',') => {
                let token = solve(token.take(), "Expect a number.");
                let value: F32 = solve(token.parse().ok(), "Bad number.");
                if !value.is_zero() {
                    vector.push(SparseF32Element { index, value });
                }
                index += 1;
            }
            (Reading, b']') => {
                if let Some(token) = token.take() {
                    let value: F32 = solve(token.parse().ok(), "Bad number.");
                    if !value.is_zero() {
                        vector.push(SparseF32Element { index, value });
                    }
                    index += 1;
                }
                state = MatchedRight;
            }
            (_, b' ') => {}
            _ => {
                SessionError::BadLiteral {
                    hint: format!("Bad character with ascii {:#x}.", c),
                }
                .friendly();
            }
        }
    }
    if state != MatchedRight {
        SessionError::BadLiteral {
            hint: "Bad sequence.".to_string(),
        }
        .friendly();
    }
    if index > 65535 {
        SessionError::BadValueDimensions.friendly();
    }
    SVecf32::new_in_postgres(&vector, index as u16)
}

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn _vectors_svecf32_out(vector: SVecf32Input<'_>) -> CString {
    let mut buffer = String::new();
    buffer.push('[');
    let mut iter = vector.iter();
    if let Some(x) = iter.next() {
        buffer.push_str(format!("{}", x).as_str());
    }
    for x in iter {
        buffer.push_str(format!(", {}", x).as_str());
    }
    buffer.push(']');
    CString::new(buffer).unwrap()
}
