use {
	crate::config::FilesystemProjectIds,
	std::{
		collections::BTreeSet,
		sync::{Arc, Mutex},
	},
};

#[derive(Clone)]
pub struct Pool(Arc<Mutex<State>>);

pub struct Id {
	pool: Pool,
	value: u32,
}

struct State {
	available: BTreeSet<u32>,
	next: u32,
	range: FilesystemProjectIds,
}

impl Pool {
	#[must_use]
	pub fn new(range: FilesystemProjectIds) -> Self {
		Self(Arc::new(Mutex::new(State {
			available: BTreeSet::new(),
			next: 0,
			range,
		})))
	}

	#[must_use]
	pub fn acquire(&self) -> Option<Id> {
		let mut state = self.0.lock().unwrap();
		let value = if let Some(value) = state.available.pop_first() {
			value
		} else {
			if state.next >= state.range.count {
				return None;
			}
			let value = state.range.start.checked_add(state.next)?;
			state.next += 1;
			value
		};
		let pool = self.clone();

		Some(Id { pool, value })
	}
}

impl Id {
	pub fn release(self) {
		let inserted = self.pool.0.lock().unwrap().available.insert(self.value);
		assert!(inserted, "the filesystem project ID was released twice");
	}

	#[must_use]
	pub fn value(&self) -> u32 {
		self.value
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn reuses_released_ids() {
		let pool = Pool::new(FilesystemProjectIds {
			count: 2,
			start: 101,
		});
		let first = pool.acquire().unwrap();
		let second = pool.acquire().unwrap();

		assert_eq!(first.value(), 101);
		assert_eq!(second.value(), 102);
		assert!(pool.acquire().is_none());

		first.release();
		let first = pool.acquire().unwrap();
		assert_eq!(first.value(), 101);
	}
}
