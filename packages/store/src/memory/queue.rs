use {
	super::Store,
	crate::{archive, index},
};

impl Store {
	#[must_use]
	pub fn get_archive_queue_entries(
		&self,
		arg: archive::queue::get::batch::Arg,
	) -> Vec<archive::queue::Entry> {
		self.state()
			.archive_queue
			.range((arg.indexer.clone(), arg.sequence_start)..(arg.indexer, arg.sequence_end))
			.map(|(_, entry)| entry.clone())
			.collect()
	}

	#[must_use]
	pub fn get_index_queue_fragments(
		&self,
		arg: index::queue::get::batch::Arg,
	) -> Vec<index::queue::Fragment> {
		self.state()
			.index_queue
			.range((arg.indexer.clone(), arg.sequence_start)..(arg.indexer, arg.sequence_end))
			.map(|(_, fragment)| fragment.clone())
			.collect()
	}

	pub fn delete_archive_queue_entry(&self, arg: archive::queue::delete::Arg) {
		self.state()
			.archive_queue
			.remove(&(arg.indexer, arg.sequence));
	}

	pub fn delete_index_queue_fragment(&self, arg: index::queue::delete::Arg) {
		self.state()
			.index_queue
			.remove(&(arg.indexer, arg.sequence));
	}

	pub fn put_archive_queue_entry(&self, arg: archive::queue::put::Arg) {
		let entry = arg.entry;
		let key = (entry.indexer.clone(), entry.sequence);
		self.state().archive_queue.insert(key, entry);
	}

	pub fn put_index_queue_fragment(&self, arg: index::queue::put::Arg) {
		let fragment = arg.fragment;
		let key = (fragment.indexer.clone(), fragment.sequence);
		self.state().index_queue.insert(key, fragment);
	}

	#[must_use]
	pub fn try_get_archive_queue_entry(
		&self,
		arg: archive::queue::get::Arg,
	) -> Option<archive::queue::Entry> {
		self.state()
			.archive_queue
			.get(&(arg.indexer, arg.sequence))
			.cloned()
	}

	#[must_use]
	pub fn try_get_index_queue_fragment(
		&self,
		arg: index::queue::get::Arg,
	) -> Option<index::queue::Fragment> {
		self.state()
			.index_queue
			.get(&(arg.indexer, arg.sequence))
			.cloned()
	}
}

#[cfg(test)]
mod tests {
	use {
		super::Store,
		crate::{archive, index},
		bytes::Bytes,
		tangram_client::prelude::*,
	};

	#[test]
	fn archive() {
		let store = Store::new();
		let indexer = tg::indexer::Id::new();
		let object = tg::object::Id::new(tg::object::Kind::Blob, &Bytes::from_static(b"object"));
		let entry = archive::queue::Entry {
			indexer: indexer.clone(),
			object,
			put: [1; 16],
			sequence: 42,
		};
		let arg = archive::queue::put::Arg {
			entry: entry.clone(),
		};
		store.put_archive_queue_entry(arg);
		let arg = archive::queue::get::Arg {
			indexer: indexer.clone(),
			sequence: 42,
		};
		assert_eq!(store.try_get_archive_queue_entry(arg), Some(entry.clone()));
		let arg = archive::queue::get::batch::Arg {
			indexer: indexer.clone(),
			sequence_end: 43,
			sequence_start: 42,
		};
		assert_eq!(store.get_archive_queue_entries(arg), vec![entry]);
		let arg = archive::queue::delete::Arg {
			indexer: indexer.clone(),
			sequence: 42,
		};
		store.delete_archive_queue_entry(arg);
		let arg = archive::queue::get::Arg {
			indexer,
			sequence: 42,
		};
		assert!(store.try_get_archive_queue_entry(arg).is_none());
	}

	#[test]
	fn index() {
		let store = Store::new();
		let indexer = tg::indexer::Id::new();
		let fragment = index::queue::Fragment {
			batch: index::queue::batch::Id::new([1; 16]),
			fragment: 1,
			fragments: 2,
			indexer: indexer.clone(),
			payload: Bytes::from_static(b"payload"),
			sequence: 42,
		};
		let arg = index::queue::put::Arg {
			fragment: fragment.clone(),
		};
		store.put_index_queue_fragment(arg);
		let arg = index::queue::get::Arg {
			indexer: indexer.clone(),
			sequence: 42,
		};
		assert_eq!(
			store.try_get_index_queue_fragment(arg),
			Some(fragment.clone())
		);
		let arg = index::queue::get::batch::Arg {
			indexer: indexer.clone(),
			sequence_end: 43,
			sequence_start: 42,
		};
		assert_eq!(store.get_index_queue_fragments(arg), vec![fragment]);
		let arg = index::queue::delete::Arg {
			indexer: indexer.clone(),
			sequence: 42,
		};
		store.delete_index_queue_fragment(arg);
		let arg = index::queue::get::Arg {
			indexer,
			sequence: 42,
		};
		assert!(store.try_get_index_queue_fragment(arg).is_none());
	}
}
