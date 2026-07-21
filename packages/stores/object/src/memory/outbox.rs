use {
	super::Store,
	crate::outbox::{DeleteArg, DequeueArg, EnqueueArg, Id, Item, TryGetIdArg},
	tangram_client::prelude::*,
};

impl Store {
	pub fn delete_outbox(&self, arg: DeleteArg) {
		let mut state = self.state();
		for key in arg.keys {
			state.outbox.remove(&(key.partition, key.id.value()));
		}
	}

	pub fn dequeue_outbox(&self, arg: DequeueArg) -> tg::Result<Vec<Item>> {
		let partition_end = arg
			.partition_start
			.checked_add(arg.partition_count)
			.ok_or_else(|| tg::error!("the outbox partition range overflowed"))?;
		let state = self.state();
		let items = state
			.outbox
			.iter()
			.filter(|((partition, _), _)| (arg.partition_start..partition_end).contains(partition))
			.take(arg.batch_size)
			.map(|((partition, id), payload)| Item {
				id: Id::new(*id),
				partition: *partition,
				payload: payload.clone(),
			})
			.collect();

		Ok(items)
	}

	pub fn enqueue_outbox(&self, arg: EnqueueArg) -> tg::Result<()> {
		let mut state = self.state();
		state.outbox_id = state
			.outbox_id
			.checked_add(1)
			.ok_or_else(|| tg::error!("the outbox id overflowed"))?;
		let id = state.outbox_id;
		state.outbox.insert((arg.partition, id), arg.payload);

		Ok(())
	}

	pub fn try_get_outbox_id_at_or_before(&self, arg: TryGetIdArg) -> tg::Result<Option<Id>> {
		let partition_end = arg
			.partition_start
			.checked_add(arg.partition_count)
			.ok_or_else(|| tg::error!("the outbox partition range overflowed"))?;
		let state = self.state();
		let id = state
			.outbox
			.keys()
			.filter(|(partition, id)| {
				(arg.partition_start..partition_end).contains(partition)
					&& arg.id.is_none_or(|target| *id <= target.value())
			})
			.map(|(_, id)| *id)
			.max()
			.map(Id::new);

		Ok(id)
	}
}

#[cfg(test)]
mod tests {
	use {
		super::*,
		crate::outbox::{Key, TryGetIdArg},
		bytes::Bytes,
	};

	#[test]
	fn operations() {
		let store = Store::new();
		store
			.enqueue_outbox(EnqueueArg {
				partition: 0,
				payload: Bytes::from_static(b"a"),
			})
			.unwrap();
		store
			.enqueue_outbox(EnqueueArg {
				partition: 1,
				payload: Bytes::from_static(b"b"),
			})
			.unwrap();
		store
			.enqueue_outbox(EnqueueArg {
				partition: 0,
				payload: Bytes::from_static(b"c"),
			})
			.unwrap();

		let items = store
			.dequeue_outbox(DequeueArg {
				batch_size: 1,
				partition_count: 1,
				partition_start: 0,
			})
			.unwrap();
		assert_eq!(items.len(), 1);
		assert_eq!(items[0].payload, Bytes::from_static(b"a"));

		let target = store
			.try_get_outbox_id_at_or_before(TryGetIdArg {
				id: None,
				partition_count: 2,
				partition_start: 0,
			})
			.unwrap()
			.unwrap();
		store
			.enqueue_outbox(EnqueueArg {
				partition: 1,
				payload: Bytes::from_static(b"d"),
			})
			.unwrap();
		let newest = store
			.try_get_outbox_id_at_or_before(TryGetIdArg {
				id: None,
				partition_count: 2,
				partition_start: 0,
			})
			.unwrap()
			.unwrap();
		assert_ne!(newest, target);
		let items = store
			.dequeue_outbox(DequeueArg {
				batch_size: usize::MAX,
				partition_count: 2,
				partition_start: 0,
			})
			.unwrap();
		let keys = items
			.into_iter()
			.filter(|item| item.id.value() <= target.value())
			.map(|item| Key {
				id: item.id,
				partition: item.partition,
			})
			.collect();
		store.delete_outbox(DeleteArg { keys });
		assert!(
			store
				.try_get_outbox_id_at_or_before(TryGetIdArg {
					id: Some(target),
					partition_count: 2,
					partition_start: 0,
				})
				.unwrap()
				.is_none()
		);
		assert_eq!(
			store
				.try_get_outbox_id_at_or_before(TryGetIdArg {
					id: None,
					partition_count: 2,
					partition_start: 0,
				})
				.unwrap(),
			Some(newest)
		);
	}
}
