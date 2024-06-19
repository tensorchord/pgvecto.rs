use base::scalar::F32;

pub fn prune(
    dist: impl Fn(u32, u32) -> F32,
    u: u32,
    edges: &mut Vec<(F32, u32)>,
    add: &[(F32, u32)],
    m: u32,
) {
    let mut trace = add.to_vec();
    trace.extend(edges.as_slice());
    trace.sort_by_key(|(_, v)| *v);
    trace.dedup_by_key(|(_, v)| *v);
    trace.retain(|(_, v)| *v != u);
    trace.sort();
    let mut res = Vec::new();
    for (dis_u, u) in trace {
        if res.len() == m as usize {
            break;
        }
        let check = res
            .iter()
            .map(|&(_, v)| dist(u, v))
            .all(|dist| dist > dis_u);
        if check {
            res.push((dis_u, u));
        }
    }
    *edges = res;
}

pub fn robust_prune(
    dist: impl Fn(u32, u32) -> F32,
    u: u32,
    edges: &mut Vec<(F32, u32)>,
    add: &[(F32, u32)],
    alpha: f32,
    m: u32,
) {
    let alpha = F32(alpha);
    // V ← (V ∪ Nout(p)) \ {p}
    let mut trace = add.to_vec();
    trace.extend(edges.as_slice());
    trace.sort_by_key(|(_, v)| *v);
    trace.dedup_by_key(|(_, v)| *v);
    trace.retain(|(_, v)| *v != u);
    trace.sort();
    // Nout(p) ← ∅
    let mut res = Vec::new();
    for (dis_u, u) in trace {
        if res.len() == m as usize {
            break;
        }
        let check = res
            .iter()
            .map(|&(_, v)| dist(u, v))
            .all(|dist| alpha * dist > dis_u);
        if check {
            res.push((dis_u, u));
        }
    }
    *edges = res;
}
