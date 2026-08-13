use {
	super::Store,
	crate::outbox::{
		Batch, BatchId, DeleteArg, DequeueArg, Fragment, FragmentIndex, TryGetBatchArg,
	},
	tangram_client::prelude::*,
};

impl Store {
	pub fn delete_outbox_fragments(&self, arg: DeleteArg) {
		let mut state = self.state();
		for fragment in arg.fragments {
			state.outbox.remove(&(
				fragment.partition,
				fragment.batch.value(),
				fragment.index.value(),
			));
		}
	}

	pub fn dequeue_outbox_fragments(&self, arg: DequeueArg) -> tg::Result<Vec<Fragment>> {
		let state = self.state();
		let fragments = state
			.outbox
			.iter()
			.filter(|((partition, _, _), _)| {
				(arg.partition_start..arg.partition_end).contains(partition)
			})
			.take(arg.batch_size)
			.map(|((partition, batch, index), payload)| Fragment {
				batch: BatchId::new(*batch),
				index: FragmentIndex::new(*index),
				partition: *partition,
				payload: payload.clone(),
			})
			.collect();

		Ok(fragments)
	}

	pub fn enqueue_outbox_batch(&self, batch: Batch) -> tg::Result<()> {
		let mut state = self.state();
		for (index, payload) in batch.fragments.into_iter().enumerate() {
			let index = u64::try_from(index)
				.map_err(|_| tg::error!("the outbox fragment index exceeded a u64"))?;
			state
				.outbox
				.insert((batch.partition, batch.id.value(), index), payload);
		}

		Ok(())
	}

	pub fn try_get_outbox_batch_at_or_before(
		&self,
		arg: TryGetBatchArg,
	) -> tg::Result<Option<BatchId>> {
		let state = self.state();
		let batch = state
			.outbox
			.keys()
			.filter(|(partition, batch, _)| {
				(arg.partition_start..arg.partition_end).contains(partition)
					&& arg.batch.is_none_or(|target| *batch <= target.value())
			})
			.map(|(_, batch, _)| *batch)
			.max()
			.map(BatchId::new);

		Ok(batch)
	}
}

#[cfg(test)]
mod tests {
	use {
		super::*,
		crate::outbox::{FragmentKey, TryGetBatchArg},
		bytes::Bytes,
	};

	#[test]
	fn operations() {
		let store = Store::new();
		let first = BatchId::new(1_u128.to_be_bytes());
		let second = BatchId::new(2_u128.to_be_bytes());
		store
			.enqueue_outbox_batch(Batch {
				fragments: vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")],
				id: first,
				partition: 0,
			})
			.unwrap();
		store
			.enqueue_outbox_batch(Batch {
				fragments: vec![Bytes::from_static(b"c")],
				id: second,
				partition: 1,
			})
			.unwrap();

		let fragments = store
			.dequeue_outbox_fragments(DequeueArg {
				batch_size: 1,
				partition_end: 1,
				partition_start: 0,
			})
			.unwrap();
		assert_eq!(fragments.len(), 1);
		assert_eq!(fragments[0].index, FragmentIndex::new(0));
		assert_eq!(fragments[0].payload, Bytes::from_static(b"a"));

		let target = store
			.try_get_outbox_batch_at_or_before(TryGetBatchArg {
				batch: None,
				partition_end: 2,
				partition_start: 0,
			})
			.unwrap()
			.unwrap();
		assert_eq!(target, second);
		let fragments = store
			.dequeue_outbox_fragments(DequeueArg {
				batch_size: usize::MAX,
				partition_end: 2,
				partition_start: 0,
			})
			.unwrap();
		let fragments = fragments
			.into_iter()
			.filter(|fragment| fragment.batch <= first)
			.map(|fragment| FragmentKey {
				batch: fragment.batch,
				index: fragment.index,
				partition: fragment.partition,
			})
			.collect();
		store.delete_outbox_fragments(DeleteArg { fragments });
		assert!(
			store
				.try_get_outbox_batch_at_or_before(TryGetBatchArg {
					batch: Some(first),
					partition_end: 2,
					partition_start: 0,
				})
				.unwrap()
				.is_none()
		);
		assert_eq!(
			store
				.try_get_outbox_batch_at_or_before(TryGetBatchArg {
					batch: None,
					partition_end: 2,
					partition_start: 0,
				})
				.unwrap(),
			Some(second)
		);
	}
}
