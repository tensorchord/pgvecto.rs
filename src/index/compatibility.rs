use crate::gucs::planning::ENABLE_PGVECTOR_COMPATIBILITY;
use libc::c_void;
use pgrx::pg_sys::AsPgCStr;
use std::collections::HashMap;
use std::ffi::CStr;

pub unsafe fn on_process_utility(pstmt: *mut pgrx::pg_sys::PlannedStmt) {
    let enabled = ENABLE_PGVECTOR_COMPATIBILITY.get();
    if !enabled {
        return;
    }
    unsafe {
        let utility_statement = (*pstmt).utilityStmt;
        if utility_statement.is_null() {
            return;
        }
        let is_index = pgrx::is_a(utility_statement, pgrx::pg_sys::NodeTag::T_IndexStmt);
        let is_variable_set =
            pgrx::is_a(utility_statement, pgrx::pg_sys::NodeTag::T_VariableSetStmt);
        let is_variable_show =
            pgrx::is_a(utility_statement, pgrx::pg_sys::NodeTag::T_VariableShowStmt);

        if is_index {
            let istmt: *mut pgrx::pg_sys::IndexStmt = utility_statement.cast();
            let method = CStr::from_ptr((*istmt).accessMethod).to_str();
            if method == Ok("hnsw") || method == Ok("ivfflat") {
                rewrite_type_options(istmt, method.unwrap());
                rewrite_opclass(istmt);
                swap_destroy(&mut (*istmt).accessMethod, "vectors".as_pg_cstr());
            }
        } else if is_variable_set {
            let vstmt: *mut pgrx::pg_sys::VariableSetStmt = utility_statement.cast();
            let name = CStr::from_ptr((*vstmt).name).to_str();
            match name {
                Ok("ivfflat.probes") => {
                    swap_destroy(&mut (*vstmt).name, "vectors.ivf_nprobe".as_pg_cstr());
                }
                Ok("hnsw.ef_search") => {
                    swap_destroy(&mut (*vstmt).name, "vectors.hnsw_ef_search".as_pg_cstr());
                }
                Ok(_) => {}
                Err(_) => {}
            }
        } else if is_variable_show {
            let vstmt: *mut pgrx::pg_sys::VariableShowStmt = utility_statement.cast();
            let name = CStr::from_ptr((*vstmt).name).to_str();
            match name {
                Ok("ivfflat.probes") => {
                    swap_destroy(&mut (*vstmt).name, "vectors.ivf_nprobe".as_pg_cstr());
                }
                Ok("hnsw.ef_search") => {
                    swap_destroy(&mut (*vstmt).name, "vectors.hnsw_ef_search".as_pg_cstr());
                }
                Ok(_) => {}
                Err(_) => {}
            }
        }
    }
}

unsafe fn rewrite_type_options(istmt: *mut pgrx::pg_sys::IndexStmt, method: &str) {
    unsafe {
        let original = vec_from_list((*istmt).options);
        let opts = options_from_vec(original);
        match method {
            "hnsw" => {
                let mut m: Option<u32> = None;
                let mut ef_construction: Option<u32> = None;
                for (key, value) in opts {
                    match key.as_str() {
                        "m" => {
                            if m.is_some() {
                                pgrx::error!("option `m` is set twice");
                            }
                            m = Some(value.parse::<u32>().expect("bad option"));
                        }
                        "ef_construction" => {
                            if ef_construction.is_some() {
                                pgrx::error!("option `ef_construction` is set twice");
                            }
                            ef_construction = Some(value.parse::<u32>().expect("bad option"));
                        }
                        key => pgrx::error!("unknown option {key}"),
                    }
                }
                let m = m.unwrap_or(16);
                let ef_construction = ef_construction.unwrap_or(64);
                let arg = pgrx::pg_sys::makeString(
                    format!(
                        "[indexing.hnsw]\nm = {}\nef_construction = {}",
                        m, ef_construction
                    )
                    .as_pg_cstr(),
                );
                let elem = pgrx::pg_sys::makeDefElem("options".as_pg_cstr(), arg as _, -1);
                swap_destroy(&mut (*istmt).options, list_from_vec(vec![elem]));
            }
            "ivfflat" => {
                let mut lists: Option<u32> = None;
                for (key, value) in opts {
                    match key.as_str() {
                        "lists" => {
                            if lists.is_some() {
                                pgrx::error!("option `lists` is set twice");
                            }
                            lists = Some(value.parse::<u32>().expect("bad option"));
                        }
                        key => pgrx::error!("unknown option {key}"),
                    }
                }
                let lists = lists.unwrap_or(100);
                let arg = pgrx::pg_sys::makeString(
                    format!("[indexing.ivf]\nnlist = {}", lists).as_pg_cstr(),
                );
                let elem = pgrx::pg_sys::makeDefElem("options".as_pg_cstr(), arg as _, -1);
                swap_destroy(&mut (*istmt).options, list_from_vec(vec![elem]));
            }
            _ => {}
        }
    }
}

unsafe fn rewrite_opclass(istmt: *mut pgrx::pg_sys::IndexStmt) {
    unsafe {
        let elems = vec_from_list::<pgrx::pg_sys::IndexElem>((*istmt).indexParams);
        if elems.is_empty() {
            return;
        }
        for e in elems {
            let opclass_name = vec_from_list::<c_void>((*e).opclass)
                .into_iter()
                .next()
                .unwrap();
            if opclass_name.is_null() {
                continue;
            }
            #[cfg(feature = "pg14")]
            let opclass_ptr = (*(opclass_name as *mut pgrx::pg_sys::Value)).val.str_;
            #[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17"))]
            let opclass_ptr = (*(opclass_name as *mut pgrx::pg_sys::String)).sval;
            let opclass = match CStr::from_ptr(opclass_ptr).to_str() {
                Ok("vector_l2_ops") => "vector_l2_ops",
                Ok("vector_ip_ops") => "vector_dot_ops",
                Ok("vector_cosine_ops") => "vector_cos_ops",
                Ok(_) | Err(_) => {
                    return;
                }
            };
            let opclass = pgrx::pg_sys::makeString(opclass.as_pg_cstr());
            swap_destroy(&mut (*e).opclass, list_from_vec(vec![opclass]));
        }
    }
}

pub unsafe fn options_from_vec(vec: Vec<*mut pgrx::pg_sys::DefElem>) -> HashMap<String, String> {
    let mut options = HashMap::new();
    if vec.is_empty() {
        return options;
    }
    for e in vec {
        unsafe {
            let defname = CStr::from_ptr((*e).defname).to_str().unwrap().to_owned();
            let defvalue = CStr::from_ptr(pgrx::pg_sys::defGetString(e))
                .to_str()
                .unwrap()
                .to_owned();
            options.insert(defname, defvalue);
        }
    }
    options
}

pub unsafe fn vec_from_list<T>(l: *mut pgrx::pg_sys::List) -> Vec<*mut T> {
    let mut vec = Vec::new();
    if l.is_null() {
        return vec;
    }
    unsafe {
        let length = (*l).length as usize;
        for i in 0..length {
            let cell = (*l).elements.add(i);
            vec.push((*cell).ptr_value as *mut T)
        }
    }
    vec
}

pub unsafe fn list_from_vec<T>(vec: Vec<*mut T>) -> *mut pgrx::pg_sys::List {
    use std::ptr;
    if vec.is_empty() {
        return ptr::null_mut();
    }
    let mut newlist: *mut pgrx::pg_sys::List = ptr::null_mut();
    for elem in vec {
        unsafe {
            newlist = pgrx::pg_sys::list_append_unique(newlist, elem as _);
        }
    }
    newlist
}

unsafe fn swap_destroy<T>(target: &mut *mut T, value: *mut T) {
    let ptr = *target;
    *target = value;
    if !ptr.is_null() {
        unsafe {
            pgrx::pg_sys::pfree(ptr.cast());
        }
    }
}
