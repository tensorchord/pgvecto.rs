fn main() {
    println!("cargo:rerun-if-changed=src/f16.h");
    println!("cargo:rerun-if-changed=src/f16.c");
    cc::Build::new()
        .compiler("clang-16")
        .file("./src/f16.c")
        .opt_level(3)
        .flag("-fassociative-math")
        .flag("-ffp-contract=fast")
        .flag("-freciprocal-math")
        .flag("-fno-signed-zeros")
        .debug(true)
        .compile("vectorsc");
}
