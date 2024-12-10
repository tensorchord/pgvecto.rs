fn main() {
    println!("cargo::rerun-if-changed=cshim.c");
    cc::Build::new()
        .compiler("clang")
        .file("cshim.c")
        .opt_level(3)
        .flag("-fassociative-math")
        .flag("-ffp-contract=fast")
        .flag("-freciprocal-math")
        .flag("-fno-signed-zeros")
        .compile("base_cshim");
}
