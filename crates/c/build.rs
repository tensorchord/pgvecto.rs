fn main() {
    cc::Build::new()
        .compiler("/usr/bin/clang-16")
        .file("./src/c.c")
        .opt_level(3)
        .compile("c");
}
