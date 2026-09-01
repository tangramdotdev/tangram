use {
	super::Store,
	crate::object::index::outbox::{batch, fragment},
	tangram_client::prelude::*,
};

impl Store {
	pub fn delete_object_index_outbox_batch(&self, arg: batch::delete::Arg) {
		let mut state = self.state();
		state
			.object_index_outbox
			.retain(|(partition, batch, _), _| {
				*partition != arg.partition || *batch != arg.id.value()
			});
	}

	pub fn delete_object_index_outbox_fragments(&self, arg: fragment::delete::Arg) {
		let mut state = self.state();
		for fragment in arg.fragments {
			state.object_index_outbox.remove(&(
				fragment.partition,
				fragment.batch.value(),
				fragment.index.value(),
			));
		}
	}

	pub fn dequeue_object_index_outbox_fragments(
		&self,
		arg: fragment::dequeue::Arg,
	) -> tg::Result<Vec<fragment::Fragment>> {
		let state = self.state();
		let fragments = state
			.object_index_outbox
			.iter()
			.filter(|((partition, batch, index), _)| {
				(arg.partition_start..arg.partition_end).contains(partition)
					&& arg.cursor.is_none_or(|cursor| (*batch, *index) > cursor)
					&& arg.bound.is_none_or(|bound| *batch <= bound)
			})
			.take(arg.batch_size)
			.map(|((partition, batch, index), payload)| fragment::Fragment {
				batch: batch::Id::new(*batch),
				index: fragment::Index::new(*index),
				partition: *partition,
				payload: payload.clone(),
			})
			.collect();

		Ok(fragments)
	}

	pub fn enqueue_object_index_outbox_batch(&self, arg: batch::enqueue::Arg) -> tg::Result<()> {
		let mut state = self.state();
		for (index, payload) in arg.fragments.into_iter().enumerate() {
			let index = u64::try_from(index)
				.map_err(|_| tg::error!("the object index outbox fragment index exceeded a u64"))?;
			state
				.object_index_outbox
				.insert((arg.partition, arg.id.value(), index), payload);
		}

		Ok(())
	}

	pub fn try_get_object_index_outbox_batch_at_or_before(
		&self,
		arg: batch::get::Arg,
	) -> tg::Result<Option<batch::Id>> {
		let state = self.state();
		let batch = state
			.object_index_outbox
			.keys()
			.filter(|(partition, batch, _)| {
				(arg.partition_start..arg.partition_end).contains(partition)
					&& arg.batch.is_none_or(|target| *batch <= target.value())
			})
			.map(|(_, batch, _)| *batch)
			.max()
			.map(batch::Id::new);

		Ok(batch)
	}
}

#[cfg(test)]
mod tests {
	use {super::*, bytes::Bytes};

	#[test]
	fn operations() {
		let store = Store::new();
		let first = batch::Id::new(1_u128.to_be_bytes());
		let second = batch::Id::new(2_u128.to_be_bytes());
		store
			.enqueue_object_index_outbox_batch(batch::enqueue::Arg {
				fragments: vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")],
				id: first,
				partition: 0,
			})
			.unwrap();
		store
			.enqueue_object_index_outbox_batch(batch::enqueue::Arg {
				fragments: vec![Bytes::from_static(b"c")],
				id: second,
				partition: 1,
			})
			.unwrap();

		let fragments = store
			.dequeue_object_index_outbox_fragments(fragment::dequeue::Arg {
				bound: None,
				cursor: None,
				batch_size: 1,
				partition_end: 1,
				partition_start: 0,
			})
			.unwrap();
		assert_eq!(fragments.len(), 1);
		assert_eq!(fragments[0].index, fragment::Index::new(0));
		assert_eq!(fragments[0].payload, Bytes::from_static(b"a"));

		let target = store
			.try_get_object_index_outbox_batch_at_or_before(batch::get::Arg {
				batch: None,
				partition_end: 2,
				partition_start: 0,
			})
			.unwrap()
			.unwrap();
		assert_eq!(target, second);
		let fragments = store
			.dequeue_object_index_outbox_fragments(fragment::dequeue::Arg {
				bound: None,
				cursor: None,
				batch_size: usize::MAX,
				partition_end: 2,
				partition_start: 0,
			})
			.unwrap();
		let fragments = fragments
			.into_iter()
			.filter(|fragment| fragment.batch <= first)
			.map(|fragment| fragment::Key {
				batch: fragment.batch,
				index: fragment.index,
				partition: fragment.partition,
			})
			.collect();
		store.delete_object_index_outbox_fragments(fragment::delete::Arg { fragments });
		assert!(
			store
				.try_get_object_index_outbox_batch_at_or_before(batch::get::Arg {
					batch: Some(first),
					partition_end: 2,
					partition_start: 0,
				})
				.unwrap()
				.is_none()
		);
		assert_eq!(
			store
				.try_get_object_index_outbox_batch_at_or_before(batch::get::Arg {
					batch: None,
					partition_end: 2,
					partition_start: 0,
				})
				.unwrap(),
			Some(second)
		);
		store.delete_object_index_outbox_batch(batch::delete::Arg {
			id: second,
			partition: 1,
		});
		assert!(
			store
				.try_get_object_index_outbox_batch_at_or_before(batch::get::Arg {
					batch: None,
					partition_end: 2,
					partition_start: 0,
				})
				.unwrap()
				.is_none()
		);
	}
}
