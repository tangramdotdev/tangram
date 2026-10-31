use {
	std::{
		collections::BTreeSet,
		sync::{Arc, Mutex},
	},
	tangram_server_config::FilesystemProjectIds,
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

	/// Acquire a specific ID, such as one that a stopped sandbox's directory already has.
	#[must_use]
	pub fn acquire_value(&self, value: u32) -> Option<Id> {
		let mut state = self.0.lock().unwrap();
		let offset = value.checked_sub(state.range.start)?;
		if offset >= state.range.count {
			return None;
		}
		if offset < state.next {
			if !state.available.remove(&value) {
				return None;
			}
		} else {
			let start = state.range.start;
			for skipped in state.next..offset {
				state.available.insert(start + skipped);
			}
			state.next = offset + 1;
		}
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

	#[test]
	fn acquires_specific_ids() {
		let pool = Pool::new(FilesystemProjectIds {
			count: 4,
			start: 101,
		});
		let third = pool.acquire_value(103).unwrap();
		assert!(pool.acquire_value(103).is_none());
		assert_eq!(pool.acquire().unwrap().value(), 101);
		assert_eq!(pool.acquire().unwrap().value(), 102);
		assert_eq!(pool.acquire().unwrap().value(), 104);
		assert!(pool.acquire_value(105).is_none());
		third.release();
		assert_eq!(pool.acquire_value(103).unwrap().value(), 103);
	}
}
