#!/usr/bin/env bash
set -e

# CONTROL_FILEPATH
# SO_FILEPATH

printf "fn main() {\n"

cat << EOF
    vectors::__pgrx_marker();

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
