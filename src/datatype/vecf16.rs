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
use std::ops::Index;
use std::ops::IndexMut;
use std::ptr::NonNull;

#[repr(C, align(8))]
pub struct Vecf16 {
    varlena: u32,
    len: u16,
    kind: u8,
    reserved: u8,
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
            assert!(1 <= slice.len() && slice.len() <= 65535);
            let layout = Vecf16::layout(slice.len());
            let ptr = pgrx::pg_sys::palloc(layout.size()) as *mut Vecf16;
            ptr.cast::<u8>().add(layout.size() - 8).write_bytes(0, 8);
            std::ptr::addr_of_mut!((*ptr).varlena).write(Vecf16::varlena(layout.size()));
            std::ptr::addr_of_mut!((*ptr).kind).write(1);
            std::ptr::addr_of_mut!((*ptr).reserved).write(0);
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
        // TODO: force checking it in the future
        // debug_assert_eq!(self.kind, 1);
        // debug_assert_eq!(self.reserved, 0);
        unsafe { std::slice::from_raw_parts(self.phantom.as_ptr(), self.len as usize) }
    }
    pub fn data_mut(&mut self) -> &mut [F16] {
        debug_assert_eq!(self.varlena & 3, 0);
        // TODO: force checking it in the future
        // debug_assert_eq!(self.kind, 1);
        // debug_assert_eq!(self.reserved, 0);
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
        pgrx::wrappers::regtypein("vectors.vecf16")
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
fn _vectors_vecf16_in(input: &CStr, _oid: Oid, typmod: i32) -> Vecf16Output {
    use crate::utils::parse::parse_vector;
    let reserve = Typmod::parse_from_i32(typmod).unwrap().dims().unwrap_or(0);
    let v = parse_vector(input.to_bytes(), reserve as usize, |s| s.parse().ok());
    match v {
        Err(e) => {
            SessionError::BadLiteral {
                hint: e.to_string(),
            }
            .friendly();
        }
        Ok(vector) => {
            if vector.is_empty() || vector.len() > 65535 {
                SessionError::BadValueDimensions.friendly();
            }
            Vecf16::new_in_postgres(&vector)
        }
    }
}

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn _vectors_vecf16_out(vector: Vecf16Input<'_>) -> CString {
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

#[cfg(any(feature = "pg14", feature = "pg15", feature = "pg16"))]
#[pgrx::pg_extern(sql = "\
CREATE FUNCTION _vectors_vecf16_subscript(internal) RETURNS internal
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '@FUNCTION_NAME@';")]
fn _vectors_vecf16_subscript(_fcinfo: pgrx::pg_sys::FunctionCallInfo) -> Datum {
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
                pgrx::pg_sys::error!("type vecf16 does only support one subscript");
            }
            if !is_slice {
                pgrx::pg_sys::error!("type vecf16 does only support slice fetch");
            }
            if is_assignment {
                pgrx::pg_sys::error!("type vecf16 does not support subscripted assignment");
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
                        pgrx::error!("vecf16 subscript must have type integer");
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
                        pgrx::error!("vecf16 subscript must have type integer");
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
                    Vecf16Input::from_datum((*op).resvalue.read(), (*op).resnull.read()).unwrap();
                let slice = match workspace.range {
                    Some((None, None)) => input.data().get(..),
                    Some((None, Some(y))) => input.data().get(..y),
                    Some((Some(x), None)) => input.data().get(x..),
                    Some((Some(x), Some(y))) => input.data().get(x..y),
                    None => None,
                };
                if let Some(slice) = slice {
                    if !slice.is_empty() {
                        let output = Vecf16::new_in_postgres(slice);
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

#[cfg(not(any(feature = "pg14", feature = "pg15", feature = "pg16")))]
#[pgrx::pg_extern(sql = "\
CREATE FUNCTION _vectors_vecf16_subscript(internal) RETURNS internal
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '@FUNCTION_NAME@';")]
fn _vectors_vecf16_subscript(_fcinfo: pgrx::pg_sys::FunctionCallInfo) -> Datum {
    unreachable!()
}
