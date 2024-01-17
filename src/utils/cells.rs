use std::cell::{Cell, RefCell};

pub struct PgCell<T>(Cell<T>);

unsafe impl<T: Send> Send for PgCell<T> {}
unsafe impl<T: Sync> Sync for PgCell<T> {}

impl<T> PgCell<T> {
    pub const unsafe fn new(x: T) -> Self {
        Self(Cell::new(x))
    }
}

impl<T: Copy> PgCell<T> {
    pub fn get(&self) -> T {
        self.0.get()
    }
    pub fn set(&self, value: T) {
        self.0.set(value);
    }
}

pub struct PgRefCell<T>(RefCell<T>);

unsafe impl<T: Send> Send for PgRefCell<T> {}
unsafe impl<T: Sync> Sync for PgRefCell<T> {}

impl<T> PgRefCell<T> {
    pub const unsafe fn new(x: T) -> Self {
        Self(RefCell::new(x))
    }
    pub fn borrow_mut(&self) -> std::cell::RefMut<'_, T> {
        self.0.borrow_mut()
    }
    #[allow(unused)]
    pub fn borrow(&self) -> std::cell::Ref<'_, T> {
        self.0.borrow()
    }
}
