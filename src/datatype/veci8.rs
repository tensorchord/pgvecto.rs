use crate::datatype::typmod::Typmod;
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

pub const VECI8_KIND: u8 = 3;

/// Veci8 utilizes int8 for data storage, originally derived from Vecf32.
/// Given a vector of F32, [a_0, a_1, a_2, ..., a_n], we aim to find the maximum and minimum values. The maximum value, max, is the greatest among {a_0, a_1, a_2, ..., a_n}, and the minimum value, min, is the smallest.
/// We can transform F32 to I8 using the formula (a - (max + min) / 2) / (max - min) * 255, resulting in a vector of I8, [b_0, b_1, b_2, ..., b_n]. Here 255 is the range size that the int8 type can cover, which is the difference between -128 and 127.
/// Converting I8 back to F32 can be achieved by using the formula b * (max - min) / 255 + (max + min) / 2, which gives us a vector of F32, albeit with a slight loss of precision.
/// We use alpha to represent (max - min) / 255, and offset to represent (max + min) / 2 here.
#[repr(C, align(8))]
pub struct Veci8 {
    varlena: u32,
    len: u16,
    kind: u8,
    reserved: u8,
    alpha: F32,
    offset: F32,
    phantom: [I8; 0],
}

impl Veci8 {
    fn varlena(size: usize) -> u32 {
        // varattrib_4b type with compress is not setted
        (size << 2) as u32
    }

    fn layout(len: usize) -> Layout {
        u16::try_from(len).expect("Vector is too large.");
        // size of struct VecI8
        let layout_alpha = Layout::new::<Veci8>();
        // size of data in VecI8
        let layout_beta = Layout::array::<I8>(len).unwrap();
        let layout = layout_alpha.extend(layout_beta).unwrap().0;
        layout.pad_to_align()
    }

    pub fn new_in_postgres(slice: &[I8], alpha: F32, offset: F32) -> Veci8Output {
        unsafe {
            assert!(1 <= slice.len() && slice.len() <= 65535);
            let layout = Veci8::layout(slice.len());
            let ptr = pgrx::pg_sys::palloc(layout.size()) as *mut Veci8;
            ptr.cast::<u8>().add(layout.size() - 8).write_bytes(0, 8);
            std::ptr::addr_of_mut!((*ptr).varlena).write(Veci8::varlena(layout.size()));
            std::ptr::addr_of_mut!((*ptr).len).write(slice.len() as u16);
            // new kind for VecI8
            std::ptr::addr_of_mut!((*ptr).kind).write(VECI8_KIND);
            std::ptr::addr_of_mut!((*ptr).reserved).write(0);
            std::ptr::addr_of_mut!((*ptr).alpha).write(alpha);
            std::ptr::addr_of_mut!((*ptr).offset).write(offset);
            std::ptr::copy_nonoverlapping(slice.as_ptr(), (*ptr).phantom.as_mut_ptr(), slice.len());
            Veci8Output(NonNull::new(ptr).unwrap())
        }
    }

    pub fn new_zeroed_in_postgres(len: usize) -> Veci8Output {
        unsafe {
            assert!((1..=65535).contains(&len));
            let layout = Veci8::layout(len);
            let ptr = pgrx::pg_sys::palloc(layout.size()) as *mut Veci8;
            ptr.cast::<u8>().add(layout.size() - 8).write_bytes(0, 8);
            std::ptr::addr_of_mut!((*ptr).varlena).write(Veci8::varlena(layout.size()));
            std::ptr::addr_of_mut!((*ptr).len).write(len as u16);
            // new kind for VecI8
            std::ptr::addr_of_mut!((*ptr).kind).write(VECI8_KIND);
            std::ptr::addr_of_mut!((*ptr).reserved).write(0);
            Veci8Output(NonNull::new(ptr).unwrap())
        }
    }

    pub fn len(&self) -> usize {
        self.len as usize
    }

    pub fn alpha(&self) -> F32 {
        self.alpha
    }

    pub fn offset(&self) -> F32 {
        self.offset
    }

    /// return value after dequantization by index
    /// since index<usize> return &Output, we can't create a new Output and return it as a reference, so we need to use this function to return a new Output directly
    #[inline(always)]
    pub fn index(&self, index: usize) -> F32 {
        self.data()[index].to_f() * self.alpha() + self.offset()
    }

    pub fn to_ref(&self) -> VecI8Ref<'_> {
        VecI8Ref::new(self.len, self.data(), self.alpha(), self.offset())
    }

    pub fn data(&self) -> &[I8] {
        debug_assert_eq!(self.varlena & 3, 0);
        debug_assert_eq!(self.kind, VECI8_KIND);
        unsafe { std::slice::from_raw_parts(self.phantom.as_ptr(), self.len as usize) }
    }

    pub fn data_mut(&mut self) -> &mut [I8] {
        debug_assert_eq!(self.varlena & 3, 0);
        debug_assert_eq!(self.kind, VECI8_KIND);
        unsafe { std::slice::from_raw_parts_mut(self.phantom.as_mut_ptr(), self.len as usize) }
    }
}

impl Deref for Veci8 {
    type Target = [I8];

    fn deref(&self) -> &Self::Target {
        self.data()
    }
}

impl DerefMut for Veci8 {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.data_mut()
    }
}

impl AsRef<[I8]> for Veci8 {
    fn as_ref(&self) -> &[I8] {
        self.data()
    }
}

impl AsMut<[I8]> for Veci8 {
    fn as_mut(&mut self) -> &mut [I8] {
        self.data_mut()
    }
}

impl PartialEq for Veci8 {
    fn eq(&self, other: &Self) -> bool {
        if self.len() != other.len() {
            return false;
        }
        let n = self.len();
        for i in 0..n {
            if self.index(i) != other.index(i) {
                return false;
            }
        }
        true
    }
}

impl Eq for Veci8 {}

impl PartialOrd for Veci8 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Veci8 {
    fn cmp(&self, other: &Self) -> Ordering {
        use Ordering::*;
        if let x @ Less | x @ Greater = self.len().cmp(&other.len()) {
            return x;
        }
        let n = self.len();
        for i in 0..n {
            if let x @ Less | x @ Greater = self.index(i).cmp(&other.index(i)) {
                return x;
            }
        }
        Equal
    }
}

pub enum Veci8Input<'a> {
    Owned(Veci8Output),
    Borrowed(&'a Veci8),
}

impl<'a> Veci8Input<'a> {
    pub unsafe fn new(p: NonNull<Veci8>) -> Self {
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
    type Target = Veci8;

    fn deref(&self) -> &Self::Target {
        match self {
            Veci8Input::Owned(x) => x,
            Veci8Input::Borrowed(x) => x,
        }
    }
}

pub struct Veci8Output(NonNull<Veci8>);

impl Veci8Output {
    pub fn into_raw(self) -> *mut Veci8 {
        let result = self.0.as_ptr();
        std::mem::forget(self);
        result
    }
}

impl Deref for Veci8Output {
    type Target = Veci8;

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
            let ptr = NonNull::new(datum.cast_mut_ptr::<Veci8>()).unwrap();
            unsafe { Some(Veci8Input::new(ptr)) }
        }
    }
}

impl IntoDatum for Veci8Output {
    fn into_datum(self) -> Option<Datum> {
        Some(Datum::from(self.into_raw() as *mut ()))
    }

    fn type_oid() -> Oid {
        pgrx::wrappers::regtypein("vectors.veci8")
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

#[cfg(not(any(feature = "pg14", feature = "pg15", feature = "pg16")))]
#[pgrx::pg_extern(sql = "\
CREATE FUNCTION _vectors_veci8_subscript(internal) RETURNS internal
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '@FUNCTION_NAME@';")]
fn _vectors_veci8_subscript(_fcinfo: pgrx::pg_sys::FunctionCallInfo) -> Datum {
    unreachable!()
}

#[pgrx::pg_extern(sql = "\
CREATE FUNCTION _vectors_veci8_subscript(internal) RETURNS internal
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '@FUNCTION_NAME@';")]
fn _vectors_veci8_subscript(_fcinfo: pgrx::pg_sys::FunctionCallInfo) -> Datum {
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
                pgrx::pg_sys::error!("type veci8 does only support one subscript");
            }
            if !is_slice {
                pgrx::pg_sys::error!("type veci8 does only support slice fetch");
            }
            if is_assignment {
                pgrx::pg_sys::error!("type veci8 does not support subscripted assignment");
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
                        pgrx::error!("veci8 subscript must have type integer");
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
                        pgrx::error!("veci8 subscript must have type integer");
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
                        if upper >= 0 {
                            end = Some(upper as usize);
                        } else {
                            (*op).resnull.write(true);
                            return false;
                        }
                    } else {
                        (*op).resnull.write(true);
                        return false;
                    }
                }
                if state.lowerprovided.read() {
                    if !state.lowerindexnull.read() {
                        let lower = state.lowerindex.read().value() as i32;
                        if lower >= 0 {
                            start = Some(lower as usize);
                        } else {
                            (*op).resnull.write(true);
                            return false;
                        }
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
                    Veci8Input::from_datum((*op).resvalue.read(), (*op).resnull.read()).unwrap();
                let slice = match workspace.range {
                    Some((None, None)) => input.data().get(..),
                    Some((None, Some(y))) => input.data().get(..y),
                    Some((Some(x), None)) => input.data().get(x..),
                    Some((Some(x), Some(y))) => input.data().get(x..y),
                    None => None,
                };
                if let Some(slice) = slice {
                    if !slice.is_empty() {
                        let output = Veci8::new_in_postgres(slice, input.alpha(), input.offset());
                        (*op).resnull.write(false);
                        (*op).resvalue.write(Datum::from(output.into_raw()));
                    } else {
                        (*op).resnull.write(true);
                    }
                } else {
                    (*op).resnull.write(true);
                }
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

#[pgrx::pg_extern(sql = "\
CREATE FUNCTION _vectors_veci8_send(veci8) RETURNS bytea
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE C AS 'MODULE_PATHNAME', '@FUNCTION_NAME@';")]
fn _vectors_veci8_send(vector: Veci8Input<'_>) -> Datum {
    use pgrx::pg_sys::StringInfoData;
    unsafe {
        let mut buf = StringInfoData::default();
        let len = vector.len;
        let alpha = vector.alpha;
        let offset = vector.offset;
        let bytes = std::mem::size_of::<I8>() * len as usize;
        pgrx::pg_sys::pq_begintypsend(&mut buf);
        pgrx::pg_sys::pq_sendbytes(&mut buf, (&len) as *const u16 as _, 2);
        pgrx::pg_sys::pq_sendbytes(&mut buf, (&alpha) as *const F32 as _, 4);
        pgrx::pg_sys::pq_sendbytes(&mut buf, (&offset) as *const F32 as _, 4);
        pgrx::pg_sys::pq_sendbytes(&mut buf, vector.data().as_ptr() as _, bytes as _);
        Datum::from(pgrx::pg_sys::pq_endtypsend(&mut buf))
    }
}

#[pgrx::pg_extern(sql = "\
CREATE FUNCTION _vectors_veci8_recv(bytea) RETURNS veci8
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE C AS 'MODULE_PATHNAME', '@FUNCTION_NAME@';")]
fn _vectors_veci8_recv(internal: pgrx::Internal, _oid: Oid, _typmod: i32) -> Veci8Output {
    use pgrx::pg_sys::StringInfo;
    unsafe {
        let buf: StringInfo = internal.into_datum().unwrap().cast_mut_ptr();
        let len = (pgrx::pg_sys::pq_getmsgbytes(buf, 2) as *const u16).read_unaligned();
        if len == 0 {
            pgrx::error!("data corruption is detected");
        }
        let alpha = (pgrx::pg_sys::pq_getmsgbytes(buf, 4) as *const F32).read_unaligned();
        let offset = (pgrx::pg_sys::pq_getmsgbytes(buf, 4) as *const F32).read_unaligned();
        let bytes = std::mem::size_of::<I8>() * len as usize;
        let ptr = pgrx::pg_sys::pq_getmsgbytes(buf, bytes as _);
        let mut output = Veci8::new_zeroed_in_postgres(len as usize);
        output.alpha = alpha;
        output.offset = offset;
        std::ptr::copy(ptr, output.data_mut().as_mut_ptr() as _, bytes);
        output
    }
}

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn _vectors_veci8_in(input: &CStr, _oid: Oid, typmod: i32) -> Veci8Output {
    use crate::utils::parse::parse_vector;
    let reserve = Typmod::parse_from_i32(typmod)
        .unwrap()
        .dims()
        .map(|x| x.get())
        .unwrap_or(0);
    let v = parse_vector(input.to_bytes(), reserve as usize, |s| s.parse().ok());
    match v {
        Err(e) => {
            bad_literal(&e.to_string());
        }
        Ok(vector) => {
            check_value_dimensions(vector.len());
            let (vector, alpha, offset) = quantization(vector);
            Veci8::new_in_postgres(vector.as_slice(), alpha, offset)
        }
    }
}

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn _vectors_veci8_out(vector: Veci8Input<'_>) -> CString {
    let vector = dequantization(vector.data(), vector.alpha, vector.offset);
    let mut buffer = String::new();
    buffer.push('[');
    if let Some(&x) = vector.first() {
        buffer.push_str(format!("{}", x).as_str());
    }
    for &x in vector.iter().skip(1) {
        buffer.push_str(format!(", {}", x).as_str());
    }
    buffer.push(']');
    CString::new(buffer).unwrap()
}

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn _vectors_to_veci8(len: i32, alpha: f32, offset: f32, values: pgrx::Array<i32>) -> Veci8Output {
    check_value_dimensions(len as usize);
    if (len as usize) != values.len() {
        bad_literal("Lengths of values and len are not matched.");
    }
    if values.contains_nulls() {
        bad_literal("Index or value contains nulls.");
    }
    let values = values
        .iter()
        .map(|x| I8(x.unwrap() as i8))
        .collect::<Vec<_>>();
    Veci8::new_in_postgres(values.as_slice(), F32(alpha), F32(offset))
}
