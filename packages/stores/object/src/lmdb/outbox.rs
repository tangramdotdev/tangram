use {
	super::{Db, Key, KeyKind, Store},
	crate::outbox::{DeleteArg, DequeueArg, EnqueueArg, Id, Item, TryGetIdArg},
	foundationdb_tuple::{self as fdbt, TuplePack as _},
	heed as lmdb,
	num::ToPrimitive as _,
	tangram_client::prelude::*,
};

pub(super) struct EnqueueRequest {
	pub partition: u64,
	pub payload: bytes::Bytes,
}

impl Store {
	pub async fn delete_outbox(&self, arg: DeleteArg) -> tg::Result<()> {
		if arg.keys.is_empty() {
			return Ok(());
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = super::Request::DeleteOutbox(arg);
		self.sender
			.as_ref()
			.unwrap()
			.send((request, sender))
			.await
			.map_err(|error| tg::error!(!error, "failed to send the request"))?;
		receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))?
	}

	pub async fn dequeue_outbox(&self, arg: DequeueArg) -> tg::Result<Vec<Item>> {
		let partition_end = arg
			.partition_start
			.checked_add(arg.partition_count)
			.ok_or_else(|| tg::error!("the outbox partition range overflowed"))?;
		let db = self.db;
		let env = self.env.clone();
		tokio::task::spawn_blocking(move || {
			let transaction = env
				.read_txn()
				.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
			let mut items = Vec::new();
			for partition in arg.partition_start..partition_end {
				let prefix = fdbt::pack(&(KeyKind::Outbox.to_i32().unwrap(), partition));
				let entries = db
					.prefix_iter(&transaction, &prefix)
					.map_err(|error| tg::error!(!error, "failed to iterate the outbox"))?;
				for entry in entries {
					if items.len() >= arg.batch_size {
						return Ok(items);
					}
					let (key, payload) = entry
						.map_err(|error| tg::error!(!error, "failed to get an outbox item"))?;
					let (partition, id) = unpack_key(key)?;
					items.push(Item {
						id: Id::new(id),
						partition,
						payload: bytes::Bytes::copy_from_slice(payload),
					});
				}
			}

			Ok(items)
		})
		.await
		.map_err(|error| tg::error!(!error, "failed to join the task"))?
	}

	pub async fn enqueue_outbox(&self, arg: EnqueueArg) -> tg::Result<()> {
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = super::Request::EnqueueOutbox(EnqueueRequest {
			partition: arg.partition,
			payload: arg.payload,
		});
		self.sender
			.as_ref()
			.unwrap()
			.send((request, sender))
			.await
			.map_err(|error| tg::error!(!error, "failed to send the request"))?;
		receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))?
	}

	pub async fn try_get_outbox_id_at_or_before(&self, arg: TryGetIdArg) -> tg::Result<Option<Id>> {
		let partition_end = arg
			.partition_start
			.checked_add(arg.partition_count)
			.ok_or_else(|| tg::error!("the outbox partition range overflowed"))?;
		let db = self.db;
		let env = self.env.clone();
		tokio::task::spawn_blocking(move || {
			let transaction = env
				.read_txn()
				.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
			let mut output = None;
			for partition in arg.partition_start..partition_end {
				let prefix = fdbt::pack(&(KeyKind::Outbox.to_i32().unwrap(), partition));
				let entries = db
					.rev_prefix_iter(&transaction, &prefix)
					.map_err(|error| tg::error!(!error, "failed to iterate the outbox"))?;
				for entry in entries {
					let (key, _) = entry
						.map_err(|error| tg::error!(!error, "failed to get an outbox item"))?;
					let (_, id) = unpack_key(key)?;
					if arg.id.is_some_and(|target| id > target.value()) {
						continue;
					}
					output = Some(output.map_or(id, |output: u128| output.max(id)));
					break;
				}
			}

			Ok(output.map(Id::new))
		})
		.await
		.map_err(|error| tg::error!(!error, "failed to join the task"))?
	}

	pub(super) fn task_delete_outbox(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: DeleteArg,
	) -> tg::Result<()> {
		for key in arg.keys {
			let key = Key::Outbox {
				id: key.id.value(),
				partition: key.partition,
			};
			db.delete(transaction, &key.pack_to_vec())
				.map_err(|error| tg::error!(!error, "failed to delete the outbox item"))?;
		}

		Ok(())
	}

	pub(super) fn task_enqueue_outbox(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		request: EnqueueRequest,
	) -> tg::Result<()> {
		let EnqueueRequest { partition, payload } = request;
		let counter_key = Key::OutboxId.pack_to_vec();
		let id = db
			.get(transaction, &counter_key)
			.map_err(|error| tg::error!(!error, "failed to get the outbox id"))?
			.map_or(Ok(0), decode_id)?
			.checked_add(1)
			.ok_or_else(|| tg::error!("the outbox id overflowed"))?;
		db.put(transaction, &counter_key, &id.to_be_bytes())
			.map_err(|error| tg::error!(!error, "failed to put the outbox id"))?;
		let key = Key::Outbox { id, partition };
		db.put(transaction, &key.pack_to_vec(), &payload)
			.map_err(|error| tg::error!(!error, "failed to put the outbox item"))?;

		Ok(())
	}
}

fn decode_id(bytes: &[u8]) -> tg::Result<u128> {
	let bytes = bytes
		.try_into()
		.map_err(|_| tg::error!("invalid outbox id length"))?;
	Ok(u128::from_be_bytes(bytes))
}

fn unpack_key(bytes: &[u8]) -> tg::Result<(u64, u128)> {
	let (_, partition, id): (i32, u64, Vec<u8>) = fdbt::unpack(bytes)
		.map_err(|error| tg::error!(!error, "failed to unpack the outbox key"))?;
	let id = decode_id(&id)?;

	Ok((partition, id))
}

#[cfg(test)]
mod tests {
	use {super::*, crate::outbox::Key, std::path::Path};

	fn store(path: &Path) -> Store {
		let config = super::super::Config {
			map_size: 1024 * 1024 * 10,
			path: path.join("test.lmdb"),
			posix_sem_prefix: None,
		};
		Store::new(&config).unwrap()
	}

	#[tokio::test]
	async fn operations_and_persistence() {
		let temp = tangram_util::fs::Temp::new().unwrap();
		std::fs::create_dir(temp.path()).unwrap();
		let target = {
			let store = store(temp.path());
			store
				.enqueue_outbox(EnqueueArg {
					partition: 0,
					payload: bytes::Bytes::from_static(b"a"),
				})
				.await
				.unwrap();
			store
				.enqueue_outbox(EnqueueArg {
					partition: 1,
					payload: bytes::Bytes::from_static(b"b"),
				})
				.await
				.unwrap();
			store
				.try_get_outbox_id_at_or_before(TryGetIdArg {
					id: None,
					partition_count: 2,
					partition_start: 0,
				})
				.await
				.unwrap()
				.unwrap()
		};

		let store = store(temp.path());
		assert_eq!(
			store
				.try_get_outbox_id_at_or_before(TryGetIdArg {
					id: None,
					partition_count: 2,
					partition_start: 0,
				})
				.await
				.unwrap(),
			Some(target)
		);
		store
			.enqueue_outbox(EnqueueArg {
				partition: 0,
				payload: bytes::Bytes::from_static(b"c"),
			})
			.await
			.unwrap();
		let newest = store
			.try_get_outbox_id_at_or_before(TryGetIdArg {
				id: None,
				partition_count: 2,
				partition_start: 0,
			})
			.await
			.unwrap()
			.unwrap();
		assert!(newest.value() > target.value());
		let items = store
			.dequeue_outbox(DequeueArg {
				batch_size: usize::MAX,
				partition_count: 2,
				partition_start: 0,
			})
			.await
			.unwrap();
		assert_eq!(items.len(), 3);
		assert_eq!(items[0].payload, bytes::Bytes::from_static(b"a"));
		assert_eq!(items[1].payload, bytes::Bytes::from_static(b"c"));
		assert_eq!(items[2].payload, bytes::Bytes::from_static(b"b"));
		let keys = items
			.into_iter()
			.filter(|item| item.id.value() <= target.value())
			.map(|item| Key {
				id: item.id,
				partition: item.partition,
			})
			.collect();
		store.delete_outbox(DeleteArg { keys }).await.unwrap();
		assert!(
			store
				.try_get_outbox_id_at_or_before(TryGetIdArg {
					id: Some(target),
					partition_count: 2,
					partition_start: 0,
				})
				.await
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
				.await
				.unwrap(),
			Some(newest)
		);
	}
}
