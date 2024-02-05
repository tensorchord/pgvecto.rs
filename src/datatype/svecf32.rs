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
    padding: [u8; 6],
    phantom: [u8; 0],
}

impl SVecf32 {
    fn varlena(size: usize) -> u32 {
        (size << 2) as u32
    }
    fn layout(len: usize) -> Layout {
        u16::try_from(len).expect("Vector is too large.");
        let layout = Layout::new::<SVecf32>();
        let layout1 = Layout::array::<u16>(len).unwrap();
        let layout2 = Layout::array::<F32>(len).unwrap();
        let layout = layout.extend(layout1).unwrap().0.pad_to_align();
        let layout = layout.extend(layout2).unwrap().0.pad_to_align();
        layout
    }
    pub fn new_in_postgres(vector: SparseF32Ref<'_>) -> SVecf32Output {
        unsafe {
            let layout = SVecf32::layout(vector.length() as usize);
            let ptr = pgrx::pg_sys::palloc(layout.size()) as *mut SVecf32;
            ptr.cast::<u8>().add(layout.size() - 8).write_bytes(0, 8);
            std::ptr::addr_of_mut!((*ptr).varlena).write(SVecf32::varlena(layout.size()));
            std::ptr::addr_of_mut!((*ptr).dims).write(vector.dims);
            std::ptr::addr_of_mut!((*ptr).kind).write(2);
            std::ptr::addr_of_mut!((*ptr).reserved).write(0);
            std::ptr::addr_of_mut!((*ptr).len).write(vector.length());
            std::ptr::addr_of_mut!((*ptr).padding).write_bytes(0, 6);
            let mut data_ptr = (*ptr).phantom.as_mut_ptr().cast::<u16>();
            std::ptr::copy_nonoverlapping(
                vector.indexes.as_ptr(),
                data_ptr,
                vector.length() as usize,
            );
            data_ptr = data_ptr.add(vector.length() as usize);
            let offset = data_ptr.align_offset(8);
            std::ptr::write_bytes(data_ptr, 0, offset * std::mem::size_of::<u16>());
            data_ptr = data_ptr.add(offset);
            std::ptr::copy_nonoverlapping(
                vector.values.as_ptr(),
                data_ptr.cast(),
                vector.length() as usize,
            );
            SVecf32Output(NonNull::new(ptr).unwrap())
        }
    }
    pub fn new_zeroed_in_postgres(len: usize) -> SVecf32Output {
        unsafe {
            let layout = SVecf32::layout(len);
            let ptr = pgrx::pg_sys::palloc0(layout.size()) as *mut SVecf32;
            ptr.cast::<u8>().add(layout.size() - 8).write_bytes(0, 8);
            std::ptr::addr_of_mut!((*ptr).varlena).write(SVecf32::varlena(layout.size()));
            std::ptr::addr_of_mut!((*ptr).kind).write(2);
            std::ptr::addr_of_mut!((*ptr).reserved).write(0);
            std::ptr::addr_of_mut!((*ptr).len).write(len as u16);
            SVecf32Output(NonNull::new(ptr).unwrap())
        }
    }
    pub fn dims(&self) -> u16 {
        self.dims
    }
    pub fn len(&self) -> usize {
        self.len as usize
    }
    fn indexes(&self) -> *const u16 {
        self.phantom.as_ptr().cast()
    }
    fn values(&self) -> *const F32 {
        let len = self.len as usize;
        unsafe {
            let ptr = self.phantom.as_ptr().cast::<u16>().add(len);
            let offset = ptr.align_offset(8);
            ptr.add(offset).cast()
        }
    }
    fn indexes_mut(&mut self) -> *mut u16 {
        self.phantom.as_mut_ptr().cast()
    }
    fn values_mut(&mut self) -> *mut F32 {
        let len = self.len as usize;
        unsafe {
            let ptr = self.phantom.as_mut_ptr().cast::<u16>().add(len);
            let offset = ptr.align_offset(8);
            ptr.add(offset).cast()
        }
    }
    pub fn data(&self) -> SparseF32Ref<'_> {
        debug_assert_eq!(self.varlena & 3, 0);
        debug_assert_eq!(self.kind, 2);
        let len = self.len as usize;
        unsafe {
            let indexes = std::slice::from_raw_parts(self.indexes(), len);
            let values = std::slice::from_raw_parts(self.values(), len);
            SparseF32Ref {
                dims: self.dims,
                indexes,
                values,
            }
        }
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
        self.data().iter().cmp(other.data().iter())
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
    let mut indexes = Vec::<u16>::new();
    let mut values = Vec::<F32>::new();
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
                    indexes.push(index);
                    values.push(value);
                }
                index = match index.checked_add(1) {
                    Some(x) => x,
                    None => SessionError::BadValueDimensions.friendly(),
                };
            }
            (Reading, b']') => {
                if let Some(token) = token.take() {
                    let value: F32 = solve(token.parse().ok(), "Bad number.");
                    if !value.is_zero() {
                        indexes.push(index);
                        values.push(value);
                    }
                    index = match index.checked_add(1) {
                        Some(x) => x,
                        None => SessionError::BadValueDimensions.friendly(),
                    };
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
    SVecf32::new_in_postgres(SparseF32Ref {
        dims: index,
        indexes: &indexes,
        values: &values,
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

#[cfg(any(feature = "pg14", feature = "pg15", feature = "pg16"))]
#[pgrx::pg_extern(sql = "\
CREATE FUNCTION _vectors_svecf32_subscript(internal) RETURNS internal
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '@FUNCTION_NAME@';")]
fn _vectors_svecf32_subscript(_fcinfo: pgrx::pg_sys::FunctionCallInfo) -> Datum {
    #[pgrx::pg_guard]
    unsafe extern "C" fn transform(
        subscript: *mut pgrx::pg_sys::SubscriptingRef,
        indirection: *mut pgrx::pg_sys::List,
        pstate: *mut pgrx::pg_sys::ParseState,
        is_slice: bool,
        is_assignment: bool,
    ) {
        unsafe {
            if (*indirection).length != 1 {
                pgrx::pg_sys::error!("type svector does only support one subscript");
            }
            if !is_slice {
                pgrx::pg_sys::error!("type svector does only support slice fetch");
            }
            if is_assignment {
                pgrx::pg_sys::error!("type svector does not support subscripted assignment");
            }
            let subscript = &mut *subscript;
            let ai = (*(*indirection).elements.add(0)).ptr_value as *mut pgrx::pg_sys::A_Indices;
            subscript.refupperindexpr = pgrx::pg_sys::lappend(
                std::ptr::null_mut(),
                if !(*ai).uidx.is_null() {
                    let subexpr =
                        pgrx::pg_sys::transformExpr(pstate, (*ai).uidx, (*pstate).p_expr_kind);
                    let subexpr = pgrx::pg_sys::coerce_to_target_type(
                        pstate,
                        subexpr,
                        pgrx::pg_sys::exprType(subexpr),
                        pgrx::pg_sys::INT4OID,
                        -1,
                        pgrx::pg_sys::CoercionContext_COERCION_ASSIGNMENT,
                        pgrx::pg_sys::CoercionForm_COERCE_IMPLICIT_CAST,
                        -1,
                    );
                    if subexpr.is_null() {
                        pgrx::error!("svector subscript must have type integer");
                    }
                    subexpr.cast()
                } else {
                    std::ptr::null_mut()
                },
            );
            subscript.reflowerindexpr = pgrx::pg_sys::lappend(
                std::ptr::null_mut(),
                if !(*ai).lidx.is_null() {
                    let subexpr =
                        pgrx::pg_sys::transformExpr(pstate, (*ai).lidx, (*pstate).p_expr_kind);
                    let subexpr = pgrx::pg_sys::coerce_to_target_type(
                        pstate,
                        subexpr,
                        pgrx::pg_sys::exprType(subexpr),
                        pgrx::pg_sys::INT4OID,
                        -1,
                        pgrx::pg_sys::CoercionContext_COERCION_ASSIGNMENT,
                        pgrx::pg_sys::CoercionForm_COERCE_IMPLICIT_CAST,
                        -1,
                    );
                    if subexpr.is_null() {
                        pgrx::error!("svector subscript must have type integer");
                    }
                    subexpr.cast()
                } else {
                    std::ptr::null_mut()
                },
            );
            subscript.refrestype = subscript.refcontainertype;
        }
    }
    #[pgrx::pg_guard]
    unsafe extern "C" fn exec_setup(
        _subscript: *const pgrx::pg_sys::SubscriptingRef,
        state: *mut pgrx::pg_sys::SubscriptingRefState,
        steps: *mut pgrx::pg_sys::SubscriptExecSteps,
    ) {
        #[derive(Default)]
        struct Workspace {
            range: Option<(Option<usize>, Option<usize>)>,
        }
        #[pgrx::pg_guard]
        unsafe extern "C" fn sbs_check_subscripts(
            _state: *mut pgrx::pg_sys::ExprState,
            op: *mut pgrx::pg_sys::ExprEvalStep,
            _econtext: *mut pgrx::pg_sys::ExprContext,
        ) -> bool {
            unsafe {
                let state = &mut *(*op).d.sbsref.state;
                let workspace = &mut *(state.workspace as *mut Workspace);
                workspace.range = None;
                let mut end = None;
                let mut start = None;
                if state.upperprovided.read() {
                    if !state.upperindexnull.read() {
                        let upper = state.upperindex.read().value() as i32;
                        end = Some(upper as usize);
                    } else {
                        (*op).resnull.write(true);
                        return false;
                    }
                }
                if state.lowerprovided.read() {
                    if !state.lowerindexnull.read() {
                        let lower = state.lowerindex.read().value() as i32;
                        start = Some((lower - 1) as usize);
                    } else {
                        (*op).resnull.write(true);
                        return false;
                    }
                }
                workspace.range = Some((start, end));
                true
            }
        }
        #[pgrx::pg_guard]
        unsafe extern "C" fn sbs_fetch(
            _state: *mut pgrx::pg_sys::ExprState,
            op: *mut pgrx::pg_sys::ExprEvalStep,
            _econtext: *mut pgrx::pg_sys::ExprContext,
        ) {
            unsafe {
                let state = &mut *(*op).d.sbsref.state;
                let workspace = &mut *(state.workspace as *mut Workspace);
                let input =
                    SVecf32Input::from_datum((*op).resvalue.read(), (*op).resnull.read()).unwrap();
                let Some((start, end)) = workspace.range else {
                    (*op).resnull.write(true);
                    return;
                };
                let start: u16 = match start.unwrap_or(0).try_into() {
                    Ok(x) => x,
                    Err(_) => {
                        (*op).resnull.write(true);
                        return;
                    }
                };
                let end: u16 = match end.unwrap_or(input.dims() as usize).try_into() {
                    Ok(x) => x,
                    Err(_) => {
                        (*op).resnull.write(true);
                        return;
                    }
                };
                if start >= end || end > input.dims() {
                    (*op).resnull.write(true);
                    return;
                }
                let data = input.data();
                let start_index = data.indexes.partition_point(|&x| x < start);
                let end_index = data.indexes.partition_point(|&x| x < end);
                let mut indexes = data.indexes[start_index..end_index].to_vec();
                indexes.iter_mut().for_each(|x| *x -= start);
                let output = SVecf32::new_in_postgres(SparseF32Ref {
                    dims: end - start,
                    indexes: &indexes,
                    values: &data.values[start_index..end_index],
                });
                (*op).resnull.write(false);
                (*op).resvalue.write(Datum::from(output.into_raw()));
            }
        }
        unsafe {
            let state = &mut *state;
            let steps = &mut *steps;
            assert!(state.numlower == 1);
            assert!(state.numupper == 1);
            state.workspace = pgrx::pg_sys::palloc(std::mem::size_of::<Workspace>());
            std::ptr::write::<Workspace>(state.workspace.cast(), Workspace::default());
            steps.sbs_check_subscripts = Some(sbs_check_subscripts);
            steps.sbs_fetch = Some(sbs_fetch);
            steps.sbs_assign = None;
            steps.sbs_fetch_old = None;
        }
    }
    static SBSROUTINES: pgrx::pg_sys::SubscriptRoutines = pgrx::pg_sys::SubscriptRoutines {
        transform: Some(transform),
        exec_setup: Some(exec_setup),
        fetch_strict: true,
        fetch_leakproof: false,
        store_leakproof: false,
    };
    std::ptr::addr_of!(SBSROUTINES).into()
}

#[cfg(not(any(feature = "pg14", feature = "pg15", feature = "pg16")))]
#[pgrx::pg_extern(sql = "\
CREATE FUNCTION _vectors_svecf32_subscript(internal) RETURNS internal
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '@FUNCTION_NAME@';")]
fn _vectors_svecf32_subscript(_fcinfo: pgrx::pg_sys::FunctionCallInfo) -> Datum {
    unreachable!()
}

#[pgrx::pg_extern(sql = "\
CREATE FUNCTION _vectors_svecf32_send(svector) RETURNS bytea
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '@FUNCTION_NAME@';")]
fn _vectors_svecf32_send(vector: SVecf32Input<'_>) -> Datum {
    use pgrx::pg_sys::StringInfoData;
    unsafe {
        let mut buf = StringInfoData::default();
        let dims = vector.dims;
        let len = vector.len;
        let data = vector.data();
        pgrx::pg_sys::pq_begintypsend(&mut buf);
        pgrx::pg_sys::pq_sendbytes(&mut buf, (&dims) as *const u16 as _, 2);
        pgrx::pg_sys::pq_sendbytes(&mut buf, (&len) as *const u16 as _, 2);
        pgrx::pg_sys::pq_sendbytes(
            &mut buf,
            data.indexes.as_ptr() as _,
            (std::mem::size_of::<u16>() * len as usize) as _,
        );
        pgrx::pg_sys::pq_sendbytes(
            &mut buf,
            data.values.as_ptr() as _,
            (std::mem::size_of::<F32>() * len as usize) as _,
        );
        Datum::from(pgrx::pg_sys::pq_endtypsend(&mut buf))
    }
}

#[pgrx::pg_extern(sql = "
CREATE FUNCTION _vectors_svecf32_recv(internal, oid, integer) RETURNS svector
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '@FUNCTION_NAME@';")]
fn _vectors_svecf32_recv(internal: pgrx::Internal, _oid: Oid, _typmod: i32) -> SVecf32Output {
    use pgrx::pg_sys::StringInfo;
    unsafe {
        let buf: StringInfo = internal.into_datum().unwrap().cast_mut_ptr();
        let dims = (pgrx::pg_sys::pq_getmsgbytes(buf, 2) as *const u16).read_unaligned();
        let len = (pgrx::pg_sys::pq_getmsgbytes(buf, 2) as *const u16).read_unaligned();
        if dims == 0 || len == 0 {
            pgrx::error!("data corruption is detected");
        }
        let indexes_bytes = std::mem::size_of::<u16>() * len as usize;
        let indexes_ptr = pgrx::pg_sys::pq_getmsgbytes(buf, indexes_bytes as _);
        let values_bytes = std::mem::size_of::<F32>() * len as usize;
        let values_ptr = pgrx::pg_sys::pq_getmsgbytes(buf, values_bytes as _);
        let mut output = SVecf32::new_zeroed_in_postgres(len as usize);
        output.dims = dims;
        std::ptr::copy(indexes_ptr, output.indexes_mut() as _, indexes_bytes);
        std::ptr::copy(values_ptr, output.values_mut() as _, values_bytes);
        output
    }
}

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn _vectors_svector_from_kv_string(dims: i32, input: &str) -> SVecf32Output {
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
                    vector.push(SparseF32Element { index: key, value });
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
    if !vector.is_empty() && vector[vector.len() - 1].index >= dims {
        SessionError::BadValueDimensions.friendly();
    }

    let mut indexes = Vec::<u16>::with_capacity(vector.len());
    let mut values = Vec::<F32>::with_capacity(vector.len());
    for x in vector {
        indexes.push(x.index);
        values.push(x.value);
    }
    SVecf32::new_in_postgres(SparseF32Ref {
        dims,
        indexes: &indexes,
        values: &values,
    })
}

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn _vectors_svector_from_split_array(
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
                index: index as u16,
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

    let mut indexes = Vec::<u16>::with_capacity(vector.len());
    let mut values = Vec::<F32>::with_capacity(vector.len());
    for x in vector {
        indexes.push(x.index);
        values.push(x.value);
    }
    SVecf32::new_in_postgres(SparseF32Ref {
        dims,
        indexes: &indexes,
        values: &values,
    })
}
