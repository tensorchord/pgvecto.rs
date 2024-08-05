fn main() {
    if std::env::var("CARGO_CFG_TARGET_ARCH") == Ok("x86_64".to_string()) {
        println!("cargo:rerun-if-changed=src/x86_64.c");
        cc::Build::new()
            .compiler("clang")
            .file("./src/x86_64.c")
            .opt_level(3)
            .flag("-fassociative-math")
            .flag("-ffp-contract=fast")
            .flag("-freciprocal-math")
            .flag("-fno-signed-zeros")
            .compile("assets");
    }
}
