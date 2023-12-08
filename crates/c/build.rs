fn main() {
    cc::Build::new()
        .compiler("/usr/bin/clang")
        .file("./src/c.c")
        .compile("c");
}
