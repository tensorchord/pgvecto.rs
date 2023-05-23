use pgrx::pg_sys::Datum;
use pgrx::pg_sys::Oid;
use pgrx::pgrx_sql_entity_graph::metadata::ArgumentError;
use pgrx::pgrx_sql_entity_graph::metadata::Returns;
use pgrx::pgrx_sql_entity_graph::metadata::ReturnsError;
use pgrx::pgrx_sql_entity_graph::metadata::SqlMapping;
use pgrx::pgrx_sql_entity_graph::metadata::SqlTranslatable;
use pgrx::Array;
use pgrx::FromDatum;
use pgrx::IntoDatum;
use std::alloc::Allocator;
use std::alloc::Layout;
use std::cmp::Ordering;
use std::ffi::CStr;
use std::ffi::CString;
use std::ops::Deref;
use std::ops::DerefMut;
use std::ops::Index;
use std::ops::IndexMut;
use std::ptr::NonNull;

pgrx::extension_sql!(
    r#"
CREATE TYPE vector (
    INPUT     = vector_in,
    OUTPUT    = vector_out,
    TYPMOD_IN = vector_typmod_in,
    STORAGE   = plain,
    INTERNALLENGTH = VARIABLE,
    ALIGNMENT = double
);
"#,
    name = "vector",
    creates = [Type(Vector)],
    requires = [vector_in, vector_out, vector_typmod_in],
);
// todo: "extend" storage and TOAST

const MAGIC: u32 = 0xc9fc554b;

// todo: make it a generic type
pub type Scalar = f64;

#[repr(C, align(8))]
pub struct Vector {
    // forced by postgres
    header: [u8; 4],
    // debugging
    magic: u32,
    // data
    len: u16,
    phantom: [Scalar; 0],
}

static_assertions::const_assert_eq!(0x1u16, unsafe {
    std::mem::transmute::<[u8; 2], u16>([1, 0])
});

impl Vector {
    fn header(size: usize) -> [u8; 4] {
        ((size << 2) as u32).to_le_bytes()
    }
    fn layout(len: usize) -> Layout {
        u16::try_from(len).ok().expect("Vector is too large.");
        let layout_alpha = std::alloc::Layout::new::<Vector>();
        let layout_beta = std::alloc::Layout::array::<Scalar>(len).unwrap();
        let layout = layout_alpha.extend(layout_beta).unwrap().0;
        layout.pad_to_align()
    }
    pub fn new(slice: &[Scalar]) -> Box<Self> {
        unsafe {
            assert!(u16::try_from(slice.len()).is_ok());
            let layout = Vector::layout(slice.len());
            let ptr = std::alloc::Global.allocate(layout).unwrap().as_ptr() as *mut Vector;
            std::ptr::addr_of_mut!((*ptr).header).write(Vector::header(layout.size()));
            std::ptr::addr_of_mut!((*ptr).magic).write(MAGIC);
            std::ptr::addr_of_mut!((*ptr).len).write(slice.len() as u16);
            std::ptr::copy_nonoverlapping(slice.as_ptr(), (*ptr).phantom.as_mut_ptr(), slice.len());
            Box::from_raw(ptr)
        }
    }
    pub fn new_in_postgres(slice: &[Scalar]) -> PgVector {
        unsafe {
            assert!(u16::try_from(slice.len()).is_ok());
            let layout = Vector::layout(slice.len());
            let ptr = pgrx::pg_sys::palloc(layout.size()) as *mut Vector;
            std::ptr::addr_of_mut!((*ptr).header).write(Vector::header(layout.size()));
            std::ptr::addr_of_mut!((*ptr).magic).write(MAGIC);
            std::ptr::addr_of_mut!((*ptr).len).write(slice.len() as u16);
            std::ptr::copy_nonoverlapping(slice.as_ptr(), (*ptr).phantom.as_mut_ptr(), slice.len());
            PgVector(NonNull::new(ptr).unwrap())
        }
    }
    pub fn new_zeroed(len: usize) -> Box<Self> {
        unsafe {
            assert!(u16::try_from(len).is_ok());
            let layout = Vector::layout(len);
            let ptr = std::alloc::Global.allocate_zeroed(layout).unwrap().as_ptr() as *mut Vector;
            std::ptr::addr_of_mut!((*ptr).header).write(Vector::header(layout.size()));
            std::ptr::addr_of_mut!((*ptr).magic).write(MAGIC);
            std::ptr::addr_of_mut!((*ptr).len).write(len as u16);
            Box::from_raw(ptr)
        }
    }
    pub fn new_zeroed_in_postgres(len: usize) -> PgVector {
        unsafe {
            assert!(u64::try_from(len).is_ok());
            let layout = Vector::layout(len);
            let ptr = pgrx::pg_sys::palloc0(layout.size()) as *mut Vector;
            std::ptr::addr_of_mut!((*ptr).header).write(Vector::header(layout.size()));
            std::ptr::addr_of_mut!((*ptr).magic).write(MAGIC);
            std::ptr::addr_of_mut!((*ptr).len).write(len as u16);
            PgVector(NonNull::new(ptr).unwrap())
        }
    }
    pub fn len(&self) -> usize {
        self.len as usize
    }
    pub fn data(&self) -> &[Scalar] {
        debug_assert_eq!(self.header[0] & 3, 0);
        debug_assert_eq!(self.magic, MAGIC);
        unsafe { std::slice::from_raw_parts(self.phantom.as_ptr(), self.len as usize) }
    }
    pub fn data_mut(&mut self) -> &mut [Scalar] {
        debug_assert_eq!(self.header[0] & 3, 0);
        debug_assert_eq!(self.magic, MAGIC);
        unsafe { std::slice::from_raw_parts_mut(self.phantom.as_mut_ptr(), self.len as usize) }
    }
    pub fn copy_into_box(&self) -> Box<Vector> {
        Vector::new(self.data())
    }
    pub fn copy_into_postgres(&self) -> PgVector {
        Vector::new_in_postgres(self.data())
    }
}

impl Deref for Vector {
    type Target = [Scalar];

    fn deref(&self) -> &Self::Target {
        self.data()
    }
}

impl DerefMut for Vector {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.data_mut()
    }
}

impl AsRef<[Scalar]> for Vector {
    fn as_ref(&self) -> &[Scalar] {
        self.data()
    }
}

impl AsMut<[Scalar]> for Vector {
    fn as_mut(&mut self) -> &mut [Scalar] {
        self.data_mut()
    }
}

impl Index<usize> for Vector {
    type Output = Scalar;

    fn index(&self, index: usize) -> &Self::Output {
        self.data().index(index)
    }
}

impl IndexMut<usize> for Vector {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        self.data_mut().index_mut(index)
    }
}

impl PartialEq for Vector {
    fn eq(&self, other: &Self) -> bool {
        if self.len() != other.len() {
            return false;
        }
        let n = self.len();
        for i in 0..n {
            if !self[i].total_cmp(&other[i]).is_eq() {
                return false;
            }
        }
        true
    }
}

impl Eq for Vector {}

impl PartialOrd for Vector {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Vector {
    fn cmp(&self, other: &Self) -> Ordering {
        use Ordering::*;
        if let x @ Less | x @ Greater = self.len().cmp(&other.len()) {
            return x;
        }
        let n = self.len();
        for i in 0..n {
            if let x @ Less | x @ Greater = self[i].total_cmp(&other[i]) {
                return x;
            }
        }
        Equal
    }
}

#[repr(C)]
pub struct PgVector(NonNull<Vector>);

impl PgVector {
    pub fn into_raw(self) -> *mut Vector {
        let result = self.0.as_ptr();
        std::mem::forget(self);
        result
    }

    pub unsafe fn from_raw(raw: *mut Vector) -> Self {
        Self(NonNull::new(raw).unwrap())
    }
}

impl Deref for PgVector {
    type Target = Vector;

    fn deref(&self) -> &Self::Target {
        unsafe { self.0.as_ref() }
    }
}

impl DerefMut for PgVector {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { self.0.as_mut() }
    }
}

impl Drop for PgVector {
    fn drop(&mut self) {
        unsafe {
            pgrx::pg_sys::pfree(self.0.as_ptr() as _);
        }
    }
}

impl FromDatum for &Vector {
    unsafe fn from_polymorphic_datum(datum: Datum, is_null: bool, _typoid: Oid) -> Option<Self> {
        if is_null {
            None
        } else {
            Some(&*datum.cast_mut_ptr())
        }
    }
}

impl FromDatum for PgVector {
    unsafe fn from_polymorphic_datum(datum: Datum, is_null: bool, _typoid: Oid) -> Option<Self> {
        if is_null {
            None
        } else {
            Some(PgVector::from_raw(datum.cast_mut_ptr()))
        }
    }
}

impl IntoDatum for &Vector {
    fn into_datum(self) -> Option<Datum> {
        Some(Datum::from(self as *const _ as *const ()))
    }

    fn type_oid() -> Oid {
        pgrx::wrappers::regtypein("vector")
    }
}

impl IntoDatum for PgVector {
    fn into_datum(self) -> Option<Datum> {
        Some(Datum::from(self.into_raw() as *mut ()))
    }

    fn type_oid() -> Oid {
        pgrx::wrappers::regtypein("vector")
    }
}

unsafe impl SqlTranslatable for &Vector {
    fn argument_sql() -> Result<SqlMapping, ArgumentError> {
        Ok(SqlMapping::As(String::from("vector")))
    }
    fn return_sql() -> Result<Returns, ReturnsError> {
        Ok(Returns::One(SqlMapping::As(String::from("vector"))))
    }
}

unsafe impl SqlTranslatable for PgVector {
    fn argument_sql() -> Result<SqlMapping, ArgumentError> {
        Ok(SqlMapping::As(String::from("vector")))
    }
    fn return_sql() -> Result<Returns, ReturnsError> {
        Ok(Returns::One(SqlMapping::As(String::from("vector"))))
    }
}

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn vector_in(input: &CStr, _oid: Oid, _typmod: i32) -> PgVector {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum State {
        MatchingLeft,
        Reading,
        MatchedRight,
    }
    use State::*;
    let input = input.to_bytes();
    let mut vector = Vec::<Scalar>::new();
    let mut state = MatchingLeft;
    let mut token: Option<String> = None;
    for &c in input {
        match (state, c) {
            (MatchingLeft, b'[') => {
                state = Reading;
            }
            (Reading, b'0'..=b'9' | b'.' | b'e' | b'+' | b'-') => {
                let token = token.get_or_insert(String::new());
                token.push(char::from_u32(c as u32).unwrap());
            }
            (Reading, b',') => {
                let token = token.take().expect("Expect a number.");
                vector.push(token.parse::<Scalar>().expect("Bad number."));
            }
            (Reading, b']') => {
                if let Some(token) = token.take() {
                    vector.push(token.parse::<Scalar>().expect("Bad number."));
                }
                state = MatchedRight;
            }
            (_, b' ') => {}
            _ => panic!("Bad charactor with ascii {:#x}.", c),
        }
    }
    if state != MatchedRight {
        panic!("Bad sequence.");
    }
    Vector::new_in_postgres(&vector)
}

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn vector_out(vector: &Vector) -> CString {
    let mut buffer = String::new();
    buffer.push_str("[");
    if let Some(&x) = vector.data().get(0) {
        buffer.push_str(format!("{}", x).as_str());
    }
    for &x in &vector.data()[1..] {
        buffer.push_str(format!(", {}", x).as_str());
    }
    buffer.push_str("]");
    CString::new(buffer).unwrap()
}

#[pgrx::pg_extern(immutable, parallel_safe, strict)]
fn vector_typmod_in(list: Array<&CStr>) -> i32 {
    if list.len() == 0 {
        return -1;
    } else if list.len() == 1 {
        let s = list.get(0).unwrap().unwrap().to_str().unwrap();
        let dimension = s.parse::<u16>().unwrap();
        i32::from(dimension)
    } else {
        panic!("Invaild typmod");
    }
}

#[pgrx::pg_operator(immutable, parallel_safe, requires = ["vector"])]
#[pgrx::opname(+)]
#[pgrx::commutator(+)]
fn operator_add(lhs: &Vector, rhs: &Vector) -> PgVector {
    assert_eq!(lhs.len(), rhs.len(), "Invaild operation.");
    let n = lhs.len();
    let mut v = Vector::new_zeroed(n);
    for i in 0..n {
        v[i] = lhs[i] + rhs[i];
    }
    v.copy_into_postgres()
}

#[pgrx::pg_operator(immutable, parallel_safe, requires = ["vector"])]
#[pgrx::opname(-)]
fn operator_minus(lhs: &Vector, rhs: &Vector) -> PgVector {
    assert_eq!(lhs.len(), rhs.len(), "Invaild operation.");
    let n = lhs.len();
    let mut v = Vector::new_zeroed(n);
    for i in 0..n {
        v[i] = lhs[i] - rhs[i];
    }
    v.copy_into_postgres()
}

#[pgrx::pg_operator(immutable, parallel_safe, requires = ["vector"])]
#[pgrx::opname(<)]
#[pgrx::negator(>=)]
#[pgrx::commutator(>)]
#[pgrx::restrict(scalarltsel)]
#[pgrx::join(scalarltjoinsel)]
fn operator_lt(lhs: &Vector, rhs: &Vector) -> bool {
    assert_eq!(lhs.len(), rhs.len(), "Invaild operation.");
    lhs < rhs
}

#[pgrx::pg_operator(immutable, parallel_safe, requires = ["vector"])]
#[pgrx::opname(<=)]
#[pgrx::negator(>)]
#[pgrx::commutator(>=)]
#[pgrx::restrict(scalarltsel)]
#[pgrx::join(scalarltjoinsel)]
fn operator_lte(lhs: &Vector, rhs: &Vector) -> bool {
    assert_eq!(lhs.len(), rhs.len(), "Invaild operation.");
    lhs <= rhs
}

#[pgrx::pg_operator(immutable, parallel_safe, requires = ["vector"])]
#[pgrx::opname(>)]
#[pgrx::negator(<=)]
#[pgrx::commutator(<)]
#[pgrx::restrict(scalargtsel)]
#[pgrx::join(scalargtjoinsel)]
fn operator_gt(lhs: &Vector, rhs: &Vector) -> bool {
    assert_eq!(lhs.len(), rhs.len(), "Invaild operation.");
    lhs > rhs
}

#[pgrx::pg_operator(immutable, parallel_safe, requires = ["vector"])]
#[pgrx::opname(>=)]
#[pgrx::negator(<)]
#[pgrx::commutator(<=)]
#[pgrx::restrict(scalargtsel)]
#[pgrx::join(scalargtjoinsel)]
fn operator_gte(lhs: &Vector, rhs: &Vector) -> bool {
    assert_eq!(lhs.len(), rhs.len(), "Invaild operation.");
    lhs >= rhs
}

#[pgrx::pg_operator(immutable, parallel_safe, requires = ["vector"])]
#[pgrx::opname(=)]
#[pgrx::negator(<>)]
#[pgrx::commutator(=)]
#[pgrx::restrict(eqsel)]
#[pgrx::join(eqjoinsel)]
fn operator_eq(lhs: &Vector, rhs: &Vector) -> bool {
    assert_eq!(lhs.len(), rhs.len(), "Invaild operation.");
    lhs == rhs
}

#[pgrx::pg_operator(immutable, parallel_safe, requires = ["vector"])]
#[pgrx::opname(<>)]
#[pgrx::negator(=)]
#[pgrx::commutator(<>)]
#[pgrx::restrict(eqsel)]
#[pgrx::join(eqjoinsel)]
fn operator_neq(lhs: &Vector, rhs: &Vector) -> bool {
    assert_eq!(lhs.len(), rhs.len(), "Invaild operation.");
    lhs != rhs
}

#[pgrx::pg_operator(immutable, parallel_safe, requires = ["vector"])]
#[pgrx::opname(<=>)]
#[pgrx::commutator(<=>)]
fn operator_cosine(lhs: &Vector, rhs: &Vector) -> Scalar {
    if lhs.len() != rhs.len() {
        return Scalar::NAN;
    }
    let n = lhs.len();
    if n == 0 {
        return Scalar::NAN;
    }
    let mut alpha = 0.0 as Scalar;
    let mut beta = 0.0 as Scalar;
    let mut gamma = 0.0 as Scalar;
    for i in 0..n {
        alpha += lhs[i] * rhs[i];
        beta += lhs[i] * lhs[i];
        gamma += rhs[i] * rhs[i];
    }
    alpha / (beta * gamma).sqrt()
}

#[pgrx::pg_operator(immutable, parallel_safe, requires = ["vector"])]
#[pgrx::opname(<#>)]
#[pgrx::commutator(<#>)]
fn operator_inner_product(lhs: &Vector, rhs: &Vector) -> Scalar {
    if lhs.len() != rhs.len() {
        return Scalar::NAN;
    }
    let n = lhs.len();
    let mut alpha = 0.0 as Scalar;
    for i in 0..n {
        alpha += lhs[i] * rhs[i];
    }
    alpha
}

#[pgrx::pg_operator(immutable, parallel_safe, requires = ["vector"])]
#[pgrx::opname(<->)]
#[pgrx::commutator(<->)]
fn operator_l2_distance(lhs: &Vector, rhs: &Vector) -> Scalar {
    if lhs.len() != rhs.len() {
        return Scalar::NAN;
    }
    let n = lhs.len();
    let mut alpha = 0.0 as Scalar;
    for i in 0..n {
        alpha += (lhs[i] - rhs[i]) * (lhs[i] - rhs[i]);
    }
    alpha
}
