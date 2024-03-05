use crate::datatype::memory_svecf32::{SVecf32Input, SVecf32Output};
use base::vector::SVecf32Borrowed;
use pgrx::datum::FromDatum;
use pgrx::datum::Internal;
use pgrx::pg_sys::Datum;

#[pgrx::pg_extern(sql = "\
CREATE FUNCTION _vectors_svecf32_subscript(internal) RETURNS internal
IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', '@FUNCTION_NAME@';")]
fn _vectors_svecf32_subscript(_fcinfo: pgrx::pg_sys::FunctionCallInfo) -> Internal {
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
                    SVecf32Input::from_datum((*op).resvalue.read(), (*op).resnull.read()).unwrap();
                let dims = input.dims() as u32;
                let Some((start, end)) = workspace.range else {
                    (*op).resnull.write(true);
                    return;
                };
                let start: u32 = match start.unwrap_or(0).try_into() {
                    Ok(x) => x,
                    Err(_) => {
                        (*op).resnull.write(true);
                        return;
                    }
                };
                let end: u32 = match end.unwrap_or(dims as usize).try_into() {
                    Ok(x) => x,
                    Err(_) => {
                        (*op).resnull.write(true);
                        return;
                    }
                };
                if start >= end || end > dims {
                    (*op).resnull.write(true);
                    return;
                }
                let svecf32 = input.for_borrow();
                let start_index = svecf32.indexes().partition_point(|&x| x < start);
                let end_index = svecf32.indexes().partition_point(|&x| x < end);
                let mut indexes = svecf32.indexes()[start_index..end_index].to_vec();
                indexes.iter_mut().for_each(|x| *x -= start);
                let output = SVecf32Output::new(SVecf32Borrowed::new(
                    end - start,
                    &indexes,
                    &svecf32.values()[start_index..end_index],
                ));
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
    Internal::from(Some(Datum::from(std::ptr::addr_of!(SBSROUTINES))))
}
