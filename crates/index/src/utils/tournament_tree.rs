use std::cmp::Reverse;

pub struct LoserTree<I, T> {
    // 0..n
    iterators: Vec<I>,
    // 0..m
    x: Vec<Option<Reverse<T>>>,
    // 0..m, m = (winner: 1) + (losers: 2 ^ 0 + 2 ^ 1 + 2 ^ 2 + 2 ^ 3 + ... + 2 ^ (k - 1))
    losers: Vec<usize>,
}

impl<I> LoserTree<I, I::Item>
where
    I: Iterator,
    I::Item: Ord,
{
    pub fn new(mut iterators: Vec<I>) -> Self {
        let n = iterators.len();
        let m = n.next_power_of_two();
        let mut x = Vec::new();
        x.resize_with(m, || None);
        let mut losers = vec![usize::MAX; m];
        for i in 0..n {
            x[i] = iterators[i].next().map(Reverse);
        }
        let mut winners = vec![usize::MAX; 2 * m];
        for i in 0..m {
            winners[m + i] = i;
        }
        for i in (1..m).rev() {
            let (l, r) = (winners[i << 1], winners[i << 1 | 1]);
            (losers[i], winners[i]) = if x[l] < x[r] { (l, r) } else { (r, l) };
        }
        losers[0] = winners[1];
        Self {
            iterators,
            x,
            losers,
        }
    }
}

impl<I> Iterator for LoserTree<I, I::Item>
where
    I: Iterator,
    I::Item: Ord,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        let n = self.iterators.len();
        let m = n.next_power_of_two();
        let r = self.losers[0];
        let Reverse(result) = self.x[r].take()?;
        self.x[r] = self.iterators[r].next().map(Reverse);
        let mut v = r;
        let mut i = (m + r) >> 1;
        while i != 0 {
            if self.x[v] < self.x[self.losers[i]] {
                std::mem::swap(&mut v, &mut self.losers[i]);
            }
            i >>= 1;
        }
        self.losers[0] = v;
        Some(result)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use rand::Rng;

    fn check(seqs: &[Vec<u32>]) {
        let brute_force = {
            let mut result = Vec::new();
            let mut seqs = seqs
                .iter()
                .map(|x| x.clone().into_iter().peekable())
                .collect::<Vec<_>>();
            while !seqs.is_empty() {
                let mut index = 0usize;
                let mut value = u32::MAX;
                for (i, seq) in seqs.iter_mut().enumerate() {
                    if let Some(&x) = seq.peek() {
                        if x <= value {
                            index = i;
                            value = x;
                        }
                    }
                }
                let Some(_) = seqs[index].next() else { break };
                result.push(value);
            }
            result
        };
        let loser_tree = {
            let iterators = seqs.iter().map(|x| x.iter().copied()).collect();
            LoserTree::new(iterators).collect::<Vec<_>>()
        };
        assert_eq!(brute_force, loser_tree);
    }

    #[test]
    fn test_hardcode() {
        check(&[]);
        check(&[vec![0, 2, 4], vec![1, 3, 5], vec![], vec![], vec![]]);
        check(&[vec![], vec![], vec![], vec![], vec![]]);
        check(&[vec![1, 1, 1, 1, 1, 1]]);
        check(&[vec![1, 2, 3, 4, 5, 6], vec![1, 2, 3, 4, 5, 6]]);
        check(&[vec![2, 2, 3, 3, 4, 4, 5], vec![1, 1, 5, 6, 6]]);
    }

    #[test]
    fn test_random() {
        fn vec(n: usize) -> Vec<u32> {
            let mut vec = vec![0u32; n];
            vec.fill_with(|| rand::thread_rng().gen_range(0..100_000));
            vec.sort();
            vec
        }

        fn vecs() -> Vec<Vec<u32>> {
            use rand::Rng;
            let m = rand::thread_rng().gen_range(0..100);
            let mut vecs = Vec::new();
            for _ in 0..m {
                let n = rand::thread_rng().gen_range(0..10000);
                vecs.push(vec(n));
            }
            vecs
        }

        for _ in 0..10 {
            check(&vecs());
        }
    }
}
