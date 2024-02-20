fn main() {
    println!("cargo:rerun-if-changed=src/f16.h");
    println!("cargo:rerun-if-changed=src/f16.c");
    println!("cargo:rerun-if-changed=src/binary.h");
    println!("cargo:rerun-if-changed=src/binary.c");
    cc::Build::new()
        .compiler("clang-16")
        .file("./src/f16.c")
        .file("./src/binary.c")
        .opt_level(3)
        .debug(true)
        .compile("vectorsc");
}
