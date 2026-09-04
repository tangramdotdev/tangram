use {super::Store, crate::object};

impl Store {
	pub fn delete_object_archive_queue_entry(&self, arg: object::archive::queue::delete::Arg) {
		self.state()
			.object_archive_queue
			.remove(&(arg.indexer, arg.sequence));
	}

	pub fn delete_object_index_queue_fragment(&self, arg: object::index::queue::delete::Arg) {
		self.state()
			.object_index_queue
			.remove(&(arg.indexer, arg.sequence));
	}

	pub fn put_object_archive_queue_entry(&self, arg: object::archive::queue::put::Arg) {
		let entry = arg.entry;
		let key = (entry.indexer.clone(), entry.sequence);
		self.state().object_archive_queue.insert(key, entry);
	}

	pub fn put_object_index_queue_fragment(&self, arg: object::index::queue::put::Arg) {
		let fragment = arg.fragment;
		let key = (fragment.indexer.clone(), fragment.sequence);
		self.state().object_index_queue.insert(key, fragment);
	}

	#[must_use]
	pub fn try_get_object_archive_queue_entry(
		&self,
		arg: object::archive::queue::get::Arg,
	) -> Option<object::archive::queue::Entry> {
		self.state()
			.object_archive_queue
			.get(&(arg.indexer, arg.sequence))
			.cloned()
	}

	#[must_use]
	pub fn try_get_object_index_queue_fragment(
		&self,
		arg: object::index::queue::get::Arg,
	) -> Option<object::index::queue::Fragment> {
		self.state()
			.object_index_queue
			.get(&(arg.indexer, arg.sequence))
			.cloned()
	}
}

#[cfg(test)]
mod tests {
	use {super::Store, crate::object, bytes::Bytes, tangram_client::prelude::*};

	#[test]
	fn archive() {
		let store = Store::new();
		let indexer = tg::indexer::Id::new();
		let object = tg::object::Id::new(tg::object::Kind::Blob, &Bytes::from_static(b"object"));
		let entry = object::archive::queue::Entry {
			indexer: indexer.clone(),
			object,
			put: [1; 16],
			sequence: 42,
		};
		let arg = object::archive::queue::put::Arg {
			entry: entry.clone(),
		};
		store.put_object_archive_queue_entry(arg);
		let arg = object::archive::queue::get::Arg {
			indexer: indexer.clone(),
			sequence: 42,
		};
		assert_eq!(store.try_get_object_archive_queue_entry(arg), Some(entry));
		let arg = object::archive::queue::delete::Arg {
			indexer: indexer.clone(),
			sequence: 42,
		};
		store.delete_object_archive_queue_entry(arg);
		let arg = object::archive::queue::get::Arg {
			indexer,
			sequence: 42,
		};
		assert!(store.try_get_object_archive_queue_entry(arg).is_none());
	}

	#[test]
	fn index() {
		let store = Store::new();
		let indexer = tg::indexer::Id::new();
		let fragment = object::index::queue::Fragment {
			batch: object::index::queue::batch::Id::new([1; 16]),
			fragment: 1,
			fragments: 2,
			indexer: indexer.clone(),
			payload: Bytes::from_static(b"payload"),
			sequence: 42,
		};
		let arg = object::index::queue::put::Arg {
			fragment: fragment.clone(),
		};
		store.put_object_index_queue_fragment(arg);
		let arg = object::index::queue::get::Arg {
			indexer: indexer.clone(),
			sequence: 42,
		};
		assert_eq!(
			store.try_get_object_index_queue_fragment(arg),
			Some(fragment)
		);
		let arg = object::index::queue::delete::Arg {
			indexer: indexer.clone(),
			sequence: 42,
		};
		store.delete_object_index_queue_fragment(arg);
		let arg = object::index::queue::get::Arg {
			indexer,
			sequence: 42,
		};
		assert!(store.try_get_object_index_queue_fragment(arg).is_none());
	}
}
