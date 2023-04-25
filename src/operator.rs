use pgrx::prelude::*;

/// Square Euclidean distance. Try this one if you don't have any special requirements.
fn square_euclidean_distance<T, O>(left: Vec<T>, right: Vec<T>) -> O
where
    T: std::ops::Sub<T, Output = T> + Copy,
    O: std::iter::Sum + std::ops::Mul<O, Output = O> + From<T> + Copy,
{
    if left.len() != right.len() {
        error!(
            "wrong dimension: left({}) != right({})",
            left.len(),
            right.len()
        );
    }
    left.iter()
        .zip(right.iter())
        .map(|(x, y)| {
            let z = *x - *y;
            let z: O = z.into();
            z * z
        })
        .sum()
}

#[pg_operator(immutable, parallel_safe)]
#[opname(<->)]
fn square_euclidean_distance_f32(left: Vec<f32>, right: Vec<f32>) -> f32 {
    square_euclidean_distance::<f32, f32>(left, right)
}

#[pg_operator(immutable, parallel_safe)]
#[opname(<->)]
fn square_euclidean_distance_f64(left: Vec<f64>, right: Vec<f64>) -> f64 {
    square_euclidean_distance::<f64, f64>(left, right)
}

#[pg_operator(immutable, parallel_safe)]
#[opname(<->)]
fn square_euclidean_distance_i32(left: Vec<i32>, right: Vec<i32>) -> f64 {
    square_euclidean_distance::<i32, f64>(left, right)
}

/// Dot product distance.
#[pg_operator(immutable, parallel_safe)]
#[opname(<#>)]
fn dot_product_distance(left: Vec<f32>, right: Vec<f32>) -> f32 {
    if left.len() != right.len() {
        error!(
            "wrong dimension: left({}) != right({})",
            left.len(),
            right.len()
        );
    }
    left.iter().zip(right.iter()).map(|(x, y)| x * y).sum()
}

/// Cosine distance. Similar to Euclidean distance but with a normalization.
/// Use this if your vectors are not normalized.
#[pg_operator(immutable, parallel_safe)]
#[opname(<=>)]
fn cosine_distance(left: Vec<f32>, right: Vec<f32>) -> f32 {
    if left.len() != right.len() {
        error!(
            "wrong dimension: left({}) != right({})",
            left.len(),
            right.len()
        );
    }
    let dot_product: f32 = left.iter().zip(right.iter()).map(|(x, y)| x * y).sum();
    let norm_left = left.iter().map(|x| x.powi(2)).sum::<f32>().sqrt();
    let norm_right = right.iter().map(|y| y.powi(2)).sum::<f32>().sqrt();
    dot_product / (norm_left * norm_right)
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_square_euclidean_distance() {
        let distance = Spi::get_one::<f64>("SELECT ARRAY[0, 1]::int[] <-> ARRAY[3, 2]::int[]");
        assert_eq!(distance, Ok(Some(10.0)));
        let distance = Spi::get_one::<f32>("SELECT ARRAY[0, 1]::real[] <-> ARRAY[3, 2]::real[]");
        assert_eq!(distance, Ok(Some(10.0)));
        let distance = Spi::get_one::<f64>(
            "SELECT ARRAY[0, 1]::double precision[] <-> ARRAY[3, 2]::double precision[]",
        );
        assert_eq!(distance, Ok(Some(10.0)));
        let distance = Spi::get_one::<f64>("SELECT ARRAY[0, 1] <-> '{3, 2}'");
        assert_eq!(distance, Ok(Some(10.0)));
        let distance = Spi::get_one::<f64>("SELECT ARRAY[0, 1] <-> ARRAY[3, 2]");
        assert_eq!(distance, Ok(Some(10.0)));
    }

    #[pg_test]
    fn test_dot_product_distance() {
        let distance = Spi::get_one::<f32>("SELECT ARRAY[5, 1] <#> ARRAY[1, 2]");
        assert_eq!(distance, Ok(Some(7.0)));
    }

    #[pg_test]
    fn test_cosine_distance() {
        let distance = Spi::get_one::<f32>("SELECT ARRAY[4, 4] <#> ARRAY[2, 2]");
        assert_eq!(distance, Ok(Some(16.0)));
    }
}
