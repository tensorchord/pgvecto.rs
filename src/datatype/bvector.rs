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
use std::fmt::Write;
use std::ops::Deref;
use std::ops::DerefMut;
use std::ptr::NonNull;

#[repr(C, align(8))]
pub struct BVector {
    varlena: u32,
    dims: u16,
    kind: u8,
    reserved: u8,
    phantom: [usize; 0],
}

impl BVector {
    fn varlena(size: usize) -> u32 {
        (size << 2) as u32
    }
    fn layout(len: usize) -> Layout {
        u16::try_from(len).expect("Vector is too large.");
        let layout_alpha = Layout::new::<BVector>();
        let layout_beta = Layout::array::<usize>(len.div_ceil(BVEC_WIDTH)).unwrap();
        let layout = layout_alpha.extend(layout_beta).unwrap().0;
        layout.pad_to_align()
    }
    pub fn new_in_postgres(vector: BinaryVecRef<'_>) -> BVectorOutput {
        unsafe {
            let dims = vector.dims as usize;
            let layout = BVector::layout(dims);
            let ptr = pgrx::pg_sys::palloc(layout.size()) as *mut BVector;
            ptr.cast::<u8>().add(layout.size() - 8).write_bytes(0, 8);
            std::ptr::addr_of_mut!((*ptr).varlena).write(BVector::varlena(layout.size()));
            std::ptr::addr_of_mut!((*ptr).kind).write(3);
            std::ptr::addr_of_mut!((*ptr).reserved).write(0);
            std::ptr::addr_of_mut!((*ptr).dims).write(dims as u16);
            std::ptr::copy_nonoverlapping(
                vector.data.as_ptr(),
                (*ptr).phantom.as_mut_ptr(),
                dims.div_ceil(BVEC_WIDTH),
            );
            BVectorOutput(NonNull::new(ptr).unwrap())
        }
    }
    pub fn new_zeroed_in_postgres(size: usize) -> BVectorOutput {
        unsafe {
            assert!(u16::try_from(size).is_ok());
            let layout = BVector::layout(size);
            let ptr = pgrx::pg_sys::palloc0(layout.size()) as *mut BVector;
            ptr.cast::<u8>().add(layout.size() - 8).write_bytes(0, 8);
            std::ptr::addr_of_mut!((*ptr).varlena).write(BVector::varlena(layout.size()));
            std::ptr::addr_of_mut!((*ptr).kind).write(3);
            std::ptr::addr_of_mut!((*ptr).reserved).write(0);
            std::ptr::addr_of_mut!((*ptr).dims).write(size as u16);
            BVectorOutput(NonNull::new(ptr).unwrap())
        }
    }
    pub fn dims(&self) -> u16 {
        self.dims
    }
    pub fn data(&self) -> BinaryVecRef<'_> {
        debug_assert_eq!(self.varlena & 3, 0);
        debug_assert_eq!(self.kind, 3);
        BinaryVecRef {
            dims: self.dims,
            data: unsafe {
                std::slice::from_raw_parts(
                    self.phantom.as_ptr(),
                    (self.dims as usize).div_ceil(BVEC_WIDTH),
                )
            },
        }
    }
}

impl PartialEq for BVector {
    fn eq(&self, other: &Self) -> bool {
        self.data() == other.data()
    }
}

impl Eq for BVector {}

impl PartialOrd for BVector {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BVector {
    fn cmp(&self, other: &Self) -> Ordering {
        self.data().cmp(&other.data())
    }
}

pub enum BVectorInput<'a> {
    Owned(BVectorOutput),
    Borrowed(&'a BVector),
}

impl<'a> BVectorInput<'a> {
    pub unsafe fn new(p: NonNull<BVector>) -> Self {
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
    type Target = BVector;

    fn deref(&self) -> &Self::Target {
        match self {
            BVectorInput::Owned(x) => x,
            BVectorInput::Borrowed(x) => x,
        }
    }
}

pub struct BVectorOutput(NonNull<BVector>);

impl BVectorOutput {
    pub fn into_raw(self) -> *mut BVector {
        let result = self.0.as_ptr();
        std::mem::forget(self);
        result
    }
}

impl Deref for BVectorOutput {
    type Target = BVector;

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
            let ptr = NonNull::new(datum.cast_mut_ptr::<BVector>()).unwrap();
            unsafe { Some(BVectorInput::new(ptr)) }
        }
    }
}

impl IntoDatum for BVectorOutput {
    fn into_datum(self) -> Option<Datum> {
        Some(Datum::from(self.into_raw() as *mut ()))
    }

    fn type_oid() -> Oid {
        pgrx::wrappers::regtypein("vectors.bvector")
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

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn _vectors_bvector_in(input: &CStr, _oid: Oid, typmod: i32) -> BVectorOutput {
    use crate::utils::parse::parse_vector;
    let reserve = Typmod::parse_from_i32(typmod)
        .unwrap()
        .dims()
        .map(|x| x.get())
        .unwrap_or(0);
    let v = parse_vector(input.to_bytes(), reserve as usize, |s| {
        s.parse::<u8>().ok().and_then(|x| match x {
            0 => Some(false),
            1 => Some(true),
            _ => None,
        })
    });
    match v {
        Err(e) => {
            bad_literal(&e.to_string());
        }
        Ok(vector) => {
            check_value_dimensions(vector.len());
            let mut values = BinaryVec::new(vector.len() as u16);
            for (i, &x) in vector.iter().enumerate() {
                if x {
                    values.set(i, true);
                }
            }
            BVector::new_in_postgres(BinaryVecRef::from(&values))
        }
    }
}

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn _vectors_bvector_out(vector: BVectorInput<'_>) -> CString {
    let mut buffer = String::new();
    buffer.push('[');
    let mut iter = vector.data().iter();
    if let Some(x) = iter.next() {
        write!(buffer, "{}", x as u32).unwrap();
    }
    for x in iter {
        write!(buffer, ", {}", x as u32).unwrap();
    }
    buffer.push(']');
    CString::new(buffer).unwrap()
}

#[pgrx::pg_extern(sql = "\
CREATE FUNCTION _vectors_bvector_subscript(internal) RETURNS internal
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '@FUNCTION_NAME@';")]
fn _vectors_bvector_subscript(_fcinfo: pgrx::pg_sys::FunctionCallInfo) -> Datum {
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
                pgrx::pg_sys::error!("type bvector does only support one subscript");
            }
            if !is_slice {
                pgrx::pg_sys::error!("type bvector does only support slice fetch");
            }
            if is_assignment {
                pgrx::pg_sys::error!("type bvector does not support subscripted assignment");
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
                        pgrx::error!("bvector subscript must have type integer");
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
                        pgrx::error!("bvector subscript must have type integer");
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
                    BVectorInput::from_datum((*op).resvalue.read(), (*op).resnull.read()).unwrap();
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
                let end: u16 = match end.unwrap_or(input.dims as usize).try_into() {
                    Ok(x) => x,
                    Err(_) => {
                        (*op).resnull.write(true);
                        return;
                    }
                };
                if start >= end || end > input.dims {
                    (*op).resnull.write(true);
                    return;
                }
                let dims = end - start;
                let mut values = BinaryVec::new(dims);
                if start % BVEC_WIDTH as u16 == 0 {
                    let start_idx = start as usize / BVEC_WIDTH;
                    let end_idx = (end as usize).div_ceil(BVEC_WIDTH);
                    values
                        .data
                        .copy_from_slice(&input.data().data[start_idx..end_idx]);
                } else {
                    let mut i = 0;
                    let mut j = start as usize;
                    while j < end as usize {
                        values.set(i, input.data().get(j));
                        i += 1;
                        j += 1;
                    }
                }
                let output = BVector::new_in_postgres(BinaryVecRef::from(&values));
                (*op).resvalue.write(output.into_datum().unwrap());
                (*op).resnull.write(false);
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
CREATE FUNCTION _vectors_bvector_send(bvector) RETURNS bytea
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '@FUNCTION_NAME@';")]
fn _vectors_bvector_send(vector: BVectorInput<'_>) -> Datum {
    use pgrx::pg_sys::StringInfoData;
    unsafe {
        let mut buf = StringInfoData::default();
        let len = vector.dims;
        let bytes = (len as usize).div_ceil(BVEC_WIDTH) * std::mem::size_of::<usize>();
        pgrx::pg_sys::pq_begintypsend(&mut buf);
        pgrx::pg_sys::pq_sendbytes(&mut buf, (&len) as *const u16 as _, 2);
        pgrx::pg_sys::pq_sendbytes(&mut buf, vector.phantom.as_ptr() as _, bytes as _);
        Datum::from(pgrx::pg_sys::pq_endtypsend(&mut buf))
    }
}

#[pgrx::pg_extern(sql = "
CREATE FUNCTION _vectors_bvector_recv(internal, oid, integer) RETURNS bvector
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '@FUNCTION_NAME@';")]
fn _vectors_bvector_recv(internal: pgrx::Internal, _oid: Oid, _typmod: i32) -> BVectorOutput {
    use pgrx::pg_sys::StringInfo;
    unsafe {
        let buf: StringInfo = internal.into_datum().unwrap().cast_mut_ptr();
        let len = (pgrx::pg_sys::pq_getmsgbytes(buf, 2) as *const u16).read_unaligned();
        if len == 0 {
            pgrx::error!("data corruption is detected");
        }
        let bytes = (len as usize).div_ceil(BVEC_WIDTH) * std::mem::size_of::<usize>();
        let ptr = pgrx::pg_sys::pq_getmsgbytes(buf, bytes as _);
        let mut output = BVector::new_zeroed_in_postgres(len as usize);
        std::ptr::copy(ptr, output.phantom.as_mut_ptr() as _, bytes);
        output
    }
}
