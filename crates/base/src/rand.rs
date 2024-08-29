use rand::Rng;

pub fn sample_u32<R>(rng: &mut R, length: u32, amount: u32) -> Vec<u32>
where
    R: Rng + ?Sized,
{
    match rand::seq::index::sample(rng, length as usize, amount as usize) {
        rand::seq::index::IndexVec::U32(x) => x,
        _ => unreachable!(),
    }
}

pub fn sample_u32_sorted<R>(rng: &mut R, length: u32, amount: u32) -> Vec<u32>
where
    R: Rng + ?Sized,
{
    let mut x = sample_u32(rng, length, amount);
    x.sort();
    x
}
