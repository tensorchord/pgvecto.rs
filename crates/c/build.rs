fn main() {
    println!("cargo:rerun-if-changed=src/c.h");
    println!("cargo:rerun-if-changed=src/c.c");
    cc::Build::new()
        .compiler("clang-16")
        .file("./src/c.c")
        .opt_level(3)
        .debug(true)
        .compile("vectorsc");
}
