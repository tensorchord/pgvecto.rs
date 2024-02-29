fn main() {
    println!("cargo:rerun-if-changed=src/f16.h");
    println!("cargo:rerun-if-changed=src/f16.c");
    cc::Build::new()
        .compiler("clang-16")
        .file("./src/f16.c")
        .opt_level(3)
        .debug(true)
        .compile("vectorsc");
}
