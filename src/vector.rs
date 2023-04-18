#[pgrx::pg_extern]
fn hello_vectors() -> &'static str {
    "Hello, vectors"
}

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use crate::vector;

    #[crate::pg_test]
    fn test_hello_vectors() {
        assert_eq!("Hello, vectors", vector::hello_vectors());
    }
}
