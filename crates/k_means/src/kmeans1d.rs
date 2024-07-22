use base::scalar::ScalarLike;
use common::vec2::Vec2;

pub fn kmeans1d<S: ScalarLike>(c: usize, a: &[S]) -> Vec<S> {
    assert!(0 < c && c < a.len());
    let a = {
        let mut x = a.to_vec();
        x.sort();
        x
    };
    let n = a.len();
    // h(i, j), i <= j is cost of grouping [i, j] points into a cluster
    let h = {
        let mut sum_y = 0.0f64;
        let mut sum_y2 = 0.0f64;
        let mut prefix_y = vec![0.0f64];
        let mut prefix_y2 = vec![0.0f64];
        for y in a.iter().map(|y| y.to_f().to_f32() as f64) {
            sum_y += y;
            sum_y2 += y * y;
            prefix_y.push(sum_y);
            prefix_y2.push(sum_y2);
        }
        move |i, j| {
            let sum_y = prefix_y[j + 1] - prefix_y[i];
            let sum_y2 = prefix_y2[j + 1] - prefix_y2[i];
            let mu = sum_y / (j + 1 - i) as f64;
            let result = sum_y2 + (j + 1 - i) as f64 * mu * mu - 2.0 * mu * sum_y;
            S::from_f32(result as f32)
        }
    };
    // f_i(j) is cost of grouping points with IDs [0, j] into clusters with IDs [0, i].
    // f_i(j) = min { f_{i - 1}(k) + h(k + 1, j) | 0 <= k < j }
    let mut f = Vec2::<(S, usize)>::zeros((c, n));
    for j in 0..n {
        f[(0, j)] = (h(0, j), usize::MAX);
    }
    for i in 1..c {
        struct Question<F> {
            n: usize,
            f: F,
        }
        impl<S: ScalarLike, F: Fn(usize, usize) -> S> smawk::Matrix<S> for Question<F> {
            fn nrows(&self) -> usize {
                self.n
            }
            fn ncols(&self) -> usize {
                self.n
            }
            fn index(&self, i: usize, j: usize) -> S {
                if i < j {
                    (self.f)(std::cmp::min(i, j), std::cmp::max(i, j))
                } else {
                    S::nan()
                }
            }
        }
        let minima = smawk::column_minima(&Question {
            n,
            f: |k, j| f[(i - 1, k)].0 + h(k + 1, j),
        });
        f[(i, 0)] = (S::nan(), usize::MAX);
        for j in 1..n {
            let k = minima[j - 1];
            f[(i, j)] = (f[(i - 1, k)].0 + h(k + 1, j), k);
        }
    }
    let mut centroids = vec![S::nan(); c];
    let mut i = c - 1;
    let mut j = n - 1;
    loop {
        let k = f[(i, j)].1;
        let l = if k == usize::MAX { 0 } else { k + 1 };
        centroids[i] = a[l..=j].iter().copied().sum::<S>() / S::from_f32((j + 1 - l) as f32);
        if k == usize::MAX {
            break;
        }
        i -= 1;
        j = k;
    }
    centroids
}

#[cfg(test)]
mod test {
    use super::*;
    use base::scalar::F32;

    #[test]
    fn sample_0() {
        let clusters = kmeans1d(
            4,
            &[
                -50.0, 4.0, 4.1, 4.2, 200.2, 200.4, 200.9, 80.0, 100.0, 102.0,
            ]
            .map(F32),
        );
        assert_eq!(clusters, [-50.0, 4.1, 94.0, 200.5].map(F32));
    }
}
