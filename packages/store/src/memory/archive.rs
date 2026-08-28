use {super::Store, crate::object::archive::outbox, tangram_client::prelude::*};

impl Store {
	pub fn delete_object_archive_outbox_entries(&self, arg: outbox::delete::Arg) {
		let mut state = self.state();
		for entry in arg.entries {
			let key = (entry.partition, entry.stored_at, entry.id);
			state.object_archive_outbox.remove(&key);
		}
	}

	pub fn dequeue_object_archive_outbox_entries(
		&self,
		arg: outbox::dequeue::Arg,
	) -> tg::Result<Vec<outbox::Entry>> {
		let state = self.state();
		let entries = state
			.object_archive_outbox
			.iter()
			.filter(|(partition, _, _)| {
				(arg.partition_start..arg.partition_end).contains(partition)
			})
			.take(arg.batch_size)
			.map(|(partition, stored_at, id)| outbox::Entry {
				id: id.clone(),
				partition: *partition,
				stored_at: *stored_at,
			})
			.collect();

		Ok(entries)
	}

	pub fn put_object_archive_outbox_entries(&self, arg: outbox::put::Arg) {
		let mut state = self.state();
		for entry in arg.entries {
			let key = (entry.partition, entry.stored_at, entry.id);
			state.object_archive_outbox.insert(key);
		}
	}
}

#[cfg(test)]
mod tests {
	use {super::*, bytes::Bytes};

	#[test]
	fn operations() {
		let store = Store::new();
		let first = tg::object::Id::new(tg::object::Kind::Blob, &Bytes::from_static(b"first"));
		let second = tg::object::Id::new(tg::object::Kind::Blob, &Bytes::from_static(b"second"));
		let first = outbox::Entry {
			id: first,
			partition: 0,
			stored_at: 1,
		};
		let second = outbox::Entry {
			id: second,
			partition: 1,
			stored_at: 2,
		};
		let first_newer = outbox::Entry {
			stored_at: 3,
			..first.clone()
		};
		store.put_object_archive_outbox_entries(outbox::put::Arg {
			entries: vec![first.clone(), second.clone(), first_newer.clone()],
		});

		let entries = store
			.dequeue_object_archive_outbox_entries(outbox::dequeue::Arg {
				batch_size: usize::MAX,
				partition_end: 1,
				partition_start: 0,
			})
			.unwrap();
		assert_eq!(entries, vec![first.clone(), first_newer.clone()]);

		store.delete_object_archive_outbox_entries(outbox::delete::Arg {
			entries: vec![first],
		});
		let entries = store
			.dequeue_object_archive_outbox_entries(outbox::dequeue::Arg {
				batch_size: usize::MAX,
				partition_end: 2,
				partition_start: 0,
			})
			.unwrap();
		assert_eq!(entries, vec![first_newer.clone(), second.clone()]);

		store.delete_object_archive_outbox_entries(outbox::delete::Arg {
			entries: vec![first_newer],
		});
		let entries = store
			.dequeue_object_archive_outbox_entries(outbox::dequeue::Arg {
				batch_size: usize::MAX,
				partition_end: 2,
				partition_start: 0,
			})
			.unwrap();
		assert_eq!(entries, vec![second]);
	}
}
