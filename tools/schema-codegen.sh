#!/usr/bin/env bash
set -e

# CONTROL_FILEPATH
# SO_FILEPATH

cat << EOF
extern "Rust" {
    fn _vectors_jemalloc_alloc(layout: std::alloc::Layout) -> *mut u8;
    fn _vectors_jemalloc_dealloc(ptr: *mut u8, layout: std::alloc::Layout);
}

struct Jemalloc;

unsafe impl std::alloc::GlobalAlloc for Jemalloc {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        unsafe { _vectors_jemalloc_alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: std::alloc::Layout) {
        unsafe { _vectors_jemalloc_dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;
EOF

while read -r sym; do
    if [ "$sym" = "__gmon_start__" ]; then
        continue
    fi
cat << EOF
#[no_mangle]
extern "C" fn $sym() {
    eprintln!("Calling undefined symbol: {}", "$sym");
    std::process::exit(1);
}
EOF
done <<< $(nm -u $SO_FILEPATH | grep -v "@" | awk '{print $2}')

printf "fn main() {\n"

cat << EOF
    // vectors::__pgrx_marker();

    let mut entities = Vec::new();
    let control_file_path = std::path::PathBuf::from("$CONTROL_FILEPATH");
    let control_file = ::pgrx::pgrx_sql_entity_graph::ControlFile::try_from(control_file_path).expect(".control file should properly formatted");
    let control_file_entity = ::pgrx::pgrx_sql_entity_graph::SqlGraphEntity::ExtensionRoot(control_file);

    entities.push(control_file_entity);
EOF

while read -r name_ident; do
cat << EOF
    extern "Rust" {
        fn $name_ident() -> ::pgrx::pgrx_sql_entity_graph::SqlGraphEntity;
    }
    let entity = unsafe { $name_ident() };
    entities.push(entity);
EOF
done <<< $(nm -D -g $SO_FILEPATH | grep "T __pgrx_internals_" | awk '{print $3}')

cat << EOF
    let pgrx_sql = ::pgrx::pgrx_sql_entity_graph::PgrxSql::build(
        entities.into_iter(),
        "vectors".to_string(),
        false,
    )
    .expect("SQL generation error");
    eprintln!("SQL entities to {}", "/dev/stdout",);
    pgrx_sql
        .write(&mut std::io::stdout())
        .expect("Could not write SQL to stdout");
EOF

printf "}\n"
