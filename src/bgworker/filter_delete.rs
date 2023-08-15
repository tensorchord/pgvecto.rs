use crate::prelude::*;
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;

pub struct FilterDelete {
    data: DashMap<Pointer, (u16, bool)>,
}

impl FilterDelete {
    pub fn new() -> Self {
        Self {
            data: DashMap::new(),
        }
    }
    pub fn filter(&self, x: u64) -> Option<Pointer> {
        let p = Pointer::from_u48(x >> 16);
        let version = x as u16;
        if let Some(cell) = self.data.get(&p) {
            let (current_version, current_existence) = cell.value();
            if version < *current_version {
                None
            } else {
                debug_assert!(version == *current_version);
                debug_assert!(*current_existence);
                Some(p)
            }
        } else {
            debug_assert!(version == 0);
            Some(p)
        }
    }
    pub fn on_deleting(&self, p: Pointer) -> bool {
        match self.data.entry(p) {
            Entry::Occupied(mut entry) => {
                let (current_version, current_existence) = entry.get_mut();
                if *current_existence {
                    *current_version = *current_version + 1;
                    *current_existence = false;
                    true
                } else {
                    false
                }
            }
            Entry::Vacant(entry) => {
                let current_version = 1u16;
                let current_existence = false;
                entry.insert((current_version, current_existence));
                true
            }
        }
    }
    pub fn on_inserting(&self, p: Pointer) -> u64 {
        match self.data.entry(p) {
            Entry::Occupied(mut entry) => {
                let (current_version, current_existence) = entry.get_mut();
                debug_assert!(*current_existence == false);
                *current_existence = true;
                p.as_u48() << 16 | *current_version as u64
            }
            Entry::Vacant(entry) => {
                let current_version = 0u16;
                let current_existence = true;
                entry.insert((current_version, current_existence));
                p.as_u48() << 16 | current_version as u64
            }
        }
    }
}
