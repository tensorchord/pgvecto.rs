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
use std::ptr::NonNull;

#[repr(C, align(8))]
pub struct SVecf32 {
    varlena: u32,
    len: u16,
    kind: u8,
    reserved: u8,
    dims: u16,
    padding: u16,
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
    pub fn new_in_postgres(vector: SparseF32Ref<'_>) -> SVecf32Output {
        unsafe {
            let layout = SVecf32::layout(vector.elements.len());
            let ptr = pgrx::pg_sys::palloc(layout.size()) as *mut SVecf32;
            ptr.cast::<u8>().add(layout.size() - 8).write_bytes(0, 8);
            std::ptr::addr_of_mut!((*ptr).varlena).write(SVecf32::varlena(layout.size()));
            std::ptr::addr_of_mut!((*ptr).dims).write(vector.dims);
            std::ptr::addr_of_mut!((*ptr).kind).write(2);
            std::ptr::addr_of_mut!((*ptr).reserved).write(0);
            std::ptr::addr_of_mut!((*ptr).padding).write(0);
            std::ptr::addr_of_mut!((*ptr).len).write(vector.elements.len() as u16);
            std::ptr::copy_nonoverlapping(
                vector.elements.as_ptr(),
                (*ptr).phantom.as_mut_ptr(),
                vector.elements.len(),
            );
            SVecf32Output(NonNull::new(ptr).unwrap())
        }
    }
    pub fn dims(&self) -> u16 {
        self.dims
    }
    pub fn len(&self) -> usize {
        self.len as usize
    }
    pub fn data(&self) -> SparseF32Ref<'_> {
        debug_assert_eq!(self.varlena & 3, 0);
        debug_assert_eq!(self.kind, 2);
        let elements =
            unsafe { std::slice::from_raw_parts(self.phantom.as_ptr(), self.len as usize) };
        SparseF32Ref {
            dims: self.dims,
            elements,
        }
    }
    pub fn iter(&self) -> std::slice::Iter<'_, SparseF32Element> {
        self.data().elements.iter()
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
        assert!(self.dims() == other.dims());
        self.data().elements.cmp(other.data().elements)
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
        pgrx::wrappers::regtypein("vectors.svector")
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
    SVecf32::new_in_postgres(SparseF32Ref {
        dims: index as u16,
        elements: &vector,
    })
}

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn _vectors_svecf32_out(vector: SVecf32Input<'_>) -> CString {
    let mut buffer = String::new();
    buffer.push('[');
    let vec = vector.data().to_dense();
    let mut iter = vec.iter();
    if let Some(x) = iter.next() {
        buffer.push_str(format!("{}", x).as_str());
    }
    for x in iter {
        buffer.push_str(format!(", {}", x).as_str());
    }
    buffer.push(']');
    CString::new(buffer).unwrap()
}

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn svector_from_kv_string(dims: i32, input: &str) -> SVecf32Output {
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
        Start,
        Stop,
        MatchingLeftBracket,
        MatchingComma,
        MatchingKey,
        MatchingValue(u16),
    }
    use State::*;
    let input = input.as_bytes();
    let mut vector = Vec::<SparseF32Element>::new();
    let mut state = Start;
    let mut token: Option<String> = None;
    for &c in input {
        match (state, c) {
            (Start, b'[') => {
                state = MatchingLeftBracket;
            }
            (MatchingLeftBracket, b'{') => {
                state = MatchingKey;
            }
            (
                MatchingKey | MatchingValue(_),
                b'0'..=b'9' | b'a'..=b'z' | b'A'..=b'Z' | b'.' | b'+' | b'-',
            ) => {
                let token = token.get_or_insert(String::new());
                token.push(char::from_u32(c as u32).unwrap());
            }
            (MatchingKey, b':') => {
                let token = solve(token.take(), "Expect a number.");
                let key: u16 = solve(token.parse().ok(), "Bad number.");
                state = MatchingValue(key);
            }
            (MatchingValue(key), b'}') => {
                let token = solve(token.take(), "Expect a number.");
                let value: F32 = solve(token.parse().ok(), "Bad number.");
                if !value.is_zero() {
                    vector.push(SparseF32Element {
                        index: key as u32,
                        value,
                    });
                }
                state = MatchingComma;
            }
            (MatchingComma, b',') => {
                state = MatchingLeftBracket;
            }
            (MatchingComma, b']') => {
                if let Some(token) = token.take() {
                    SessionError::BadLiteral {
                        hint: format!("Unexpected token {}.", token),
                    }
                    .friendly()
                }
                state = Stop;
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
    if state != Stop {
        SessionError::BadLiteral {
            hint: "Bad sequence.".to_string(),
        }
        .friendly();
    }

    vector.sort_unstable_by_key(|x| x.index);
    if vector.len() > 1 {
        for i in 0..vector.len() - 1 {
            if vector[i].index == vector[i + 1].index {
                SessionError::ConstructError {
                    dst: "svector".to_string(),
                    hint: "Duplicated index.".to_string(),
                }
                .friendly();
            }
        }
    }
    let dims: u16 = match dims.try_into() {
        Ok(x) => x,
        Err(_) => SessionError::BadValueDimensions.friendly(),
    };
    if !vector.is_empty() && vector[vector.len() - 1].index >= dims as u32 {
        SessionError::BadValueDimensions.friendly();
    }
    SVecf32::new_in_postgres(SparseF32Ref {
        dims,
        elements: &vector,
    })
}

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn svector_from_split_array(
    dims: i32,
    index: pgrx::Array<i32>,
    value: pgrx::Array<f32>,
) -> SVecf32Output {
    let dims: u16 = match dims.try_into() {
        Ok(x) => x,
        Err(_) => SessionError::BadValueDimensions.friendly(),
    };
    if index.len() != value.len() {
        SessionError::ConstructError {
            dst: "svector".to_string(),
            hint: "Lengths of index and value are not matched.".to_string(),
        }
        .friendly();
    }
    if index.contains_nulls() || value.contains_nulls() {
        SessionError::ConstructError {
            dst: "svector".to_string(),
            hint: "Index or value contains nulls.".to_string(),
        }
        .friendly();
    }
    let mut vector: Vec<SparseF32Element> = index
        .iter_deny_null()
        .zip(value.iter_deny_null())
        .map(|(index, value)| {
            if index < 0 || index >= dims as i32 {
                SessionError::BadValueDimensions.friendly();
            }
            SparseF32Element {
                index: index as u32,
                value: F32(value),
            }
        })
        .collect();
    vector.sort_unstable_by_key(|x| x.index);
    if vector.len() > 1 {
        for i in 0..vector.len() - 1 {
            if vector[i].index == vector[i + 1].index {
                SessionError::ConstructError {
                    dst: "svector".to_string(),
                    hint: "Duplicated index.".to_string(),
                }
                .friendly();
            }
        }
    }
    SVecf32::new_in_postgres(SparseF32Ref {
        dims,
        elements: &vector,
    })
}
