use {
	super::{Db, Key as StoreKey, Kind, Store},
	crate::object::index::outbox::{batch, fragment},
	foundationdb_tuple::{self as fdbt, TuplePack as _},
	heed as lmdb,
	num::ToPrimitive as _,
	tangram_client::prelude::*,
};

mod key;

pub(super) use key::Key;

impl Store {
	pub async fn delete_object_index_outbox_batch(
		&self,
		arg: batch::delete::Arg,
	) -> tg::Result<()> {
		let request = super::request::Request::DeleteObjectIndexOutboxBatch(arg);

		self.send_write_request(request).await
	}

	pub async fn delete_object_index_outbox_fragments(
		&self,
		arg: fragment::delete::Arg,
	) -> tg::Result<()> {
		if arg.fragments.is_empty() {
			return Ok(());
		}
		let request = super::request::Request::DeleteObjectIndexOutboxFragments(arg);

		self.send_write_request(request).await
	}

	pub async fn dequeue_object_index_outbox_fragments(
		&self,
		arg: fragment::dequeue::Arg,
	) -> tg::Result<Vec<fragment::Fragment>> {
		let request = crate::read::Request::DequeueObjectIndexOutboxFragments(arg);
		let response = self.send_read_request(request).await?;
		let crate::read::Response::DequeueObjectIndexOutboxFragments(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub async fn enqueue_object_index_outbox_batch(
		&self,
		arg: batch::enqueue::Arg,
	) -> tg::Result<()> {
		let request = super::request::Request::EnqueueObjectIndexOutboxBatch(arg);

		self.send_write_request(request).await
	}

	pub async fn try_get_object_index_outbox_batch_at_or_before(
		&self,
		arg: batch::get::Arg,
	) -> tg::Result<Option<batch::Id>> {
		let request = crate::read::Request::TryGetObjectIndexOutboxBatchAtOrBefore(arg);
		let response = self.send_read_request(request).await?;
		let crate::read::Response::TryGetObjectIndexOutboxBatchAtOrBefore(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub(super) fn dequeue_object_index_outbox_fragments_with_transaction(
		db: &Db,
		transaction: &lmdb::RoTxn<'_>,
		arg: &fragment::dequeue::Arg,
	) -> tg::Result<Vec<fragment::Fragment>> {
		let mut fragments = Vec::new();
		for partition in arg.partition_start..arg.partition_end {
			let prefix =
				fdbt::pack(&(Kind::ObjectIndexOutboxFragment.to_i32().unwrap(), partition));
			let entries = db
				.prefix_iter(transaction, &prefix)
				.map_err(|error| tg::error!(!error, "failed to iterate the object index outbox"))?;
			for entry in entries {
				if fragments.len() >= arg.batch_size {
					return Ok(fragments);
				}
				let (key, payload) = entry.map_err(|error| {
					tg::error!(!error, "failed to get an object index outbox fragment")
				})?;
				let (partition, batch, index) = unpack_key(key)?;
				fragments.push(fragment::Fragment {
					batch: batch::Id::new(batch),
					index: fragment::Index::new(index),
					partition,
					payload: bytes::Bytes::copy_from_slice(payload),
				});
			}
		}

		Ok(fragments)
	}

	pub(super) fn try_get_object_index_outbox_batch_at_or_before_with_transaction(
		db: &Db,
		transaction: &lmdb::RoTxn<'_>,
		arg: &batch::get::Arg,
	) -> tg::Result<Option<batch::Id>> {
		let mut output = None;
		for partition in arg.partition_start..arg.partition_end {
			let prefix =
				fdbt::pack(&(Kind::ObjectIndexOutboxFragment.to_i32().unwrap(), partition));
			let entries = db
				.rev_prefix_iter(transaction, &prefix)
				.map_err(|error| tg::error!(!error, "failed to iterate the object index outbox"))?;
			for entry in entries {
				let (key, _) = entry.map_err(|error| {
					tg::error!(!error, "failed to get an object index outbox fragment")
				})?;
				let (_, batch, _) = unpack_key(key)?;
				if arg.batch.is_some_and(|target| batch > target.value()) {
					continue;
				}
				output = Some(output.map_or(batch, |output: [u8; 16]| output.max(batch)));
				break;
			}
		}

		Ok(output.map(batch::Id::new))
	}

	pub(super) fn delete_object_index_outbox_fragments_with_transaction(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: fragment::delete::Arg,
	) -> tg::Result<()> {
		for fragment in arg.fragments {
			let key = StoreKey::ObjectIndexOutbox(Key::Fragment {
				batch: fragment.batch.value(),
				index: fragment.index.value(),
				partition: fragment.partition,
			});
			db.delete(transaction, &key.pack_to_vec())
				.map_err(|error| {
					tg::error!(!error, "failed to delete the object index outbox fragment")
				})?;
		}

		Ok(())
	}

	pub(super) fn delete_object_index_outbox_batch_with_transaction(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: batch::delete::Arg,
	) -> tg::Result<()> {
		let prefix = fdbt::pack(&(
			Kind::ObjectIndexOutboxFragment.to_i32().unwrap(),
			arg.partition,
			arg.id.value().as_slice(),
		));
		let entries = db.prefix_iter(transaction, &prefix).map_err(|error| {
			tg::error!(!error, "failed to iterate the object index outbox batch")
		})?;
		let keys = entries
			.map(|entry| {
				let (key, _) = entry.map_err(|error| {
					tg::error!(!error, "failed to get an object index outbox batch entry")
				})?;

				Ok::<_, tg::Error>(key.to_vec())
			})
			.collect::<tg::Result<Vec<_>>>()?;
		for key in keys {
			db.delete(transaction, &key).map_err(|error| {
				tg::error!(
					!error,
					"failed to delete an object index outbox batch entry"
				)
			})?;
		}

		Ok(())
	}

	pub(super) fn enqueue_object_index_outbox_batch_with_transaction(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: batch::enqueue::Arg,
	) -> tg::Result<()> {
		for (index, payload) in arg.fragments.into_iter().enumerate() {
			let index = u64::try_from(index)
				.map_err(|_| tg::error!("the object index outbox fragment index exceeded a u64"))?;
			let key = StoreKey::ObjectIndexOutbox(Key::Fragment {
				batch: arg.id.value(),
				index,
				partition: arg.partition,
			});
			db.put(transaction, &key.pack_to_vec(), &payload)
				.map_err(|error| {
					tg::error!(!error, "failed to put the object index outbox fragment")
				})?;
		}

		Ok(())
	}
}

fn decode_batch(bytes: &[u8]) -> tg::Result<[u8; 16]> {
	bytes
		.try_into()
		.map_err(|_| tg::error!("invalid object index outbox batch id length"))
}

fn unpack_key(bytes: &[u8]) -> tg::Result<(u64, [u8; 16], u64)> {
	let (_, partition, batch, index): (i32, u64, Vec<u8>, u64) = fdbt::unpack(bytes)
		.map_err(|error| tg::error!(!error, "failed to unpack the object index outbox key"))?;
	let batch = decode_batch(&batch)?;

	Ok((partition, batch, index))
}

#[cfg(test)]
mod tests {
	use {super::*, std::path::Path};

	fn store(path: &Path) -> Store {
		let config = super::super::Config {
			map_size: 1024 * 1024 * 10,
			path: path.join("test.lmdb"),
			posix_sem_prefix: None,
			read_batch_size: 64,
			read_concurrency: 4,
			write_batch_size: 8_000,
		};
		Store::new(&config).unwrap()
	}

	#[tokio::test]
	async fn operations_and_persistence() {
		let temp = tangram_util::fs::Temp::new().unwrap();
		std::fs::create_dir(temp.path()).unwrap();
		let first = batch::Id::new(1_u128.to_be_bytes());
		let second = batch::Id::new(2_u128.to_be_bytes());
		{
			let store = store(temp.path());
			store
				.enqueue_object_index_outbox_batch(batch::enqueue::Arg {
					fragments: vec![
						bytes::Bytes::from_static(b"a"),
						bytes::Bytes::from_static(b"b"),
					],
					id: first,
					partition: 0,
				})
				.await
				.unwrap();
			store
				.enqueue_object_index_outbox_batch(batch::enqueue::Arg {
					fragments: vec![bytes::Bytes::from_static(b"c")],
					id: second,
					partition: 1,
				})
				.await
				.unwrap();
		}

		let store = store(temp.path());
		assert_eq!(
			store
				.try_get_object_index_outbox_batch_at_or_before(batch::get::Arg {
					batch: None,
					partition_end: 2,
					partition_start: 0,
				})
				.await
				.unwrap(),
			Some(second)
		);
		let fragments = store
			.dequeue_object_index_outbox_fragments(fragment::dequeue::Arg {
				batch_size: usize::MAX,
				partition_end: 2,
				partition_start: 0,
			})
			.await
			.unwrap();
		assert_eq!(fragments.len(), 3);
		assert_eq!(fragments[0].payload, bytes::Bytes::from_static(b"a"));
		assert_eq!(fragments[1].payload, bytes::Bytes::from_static(b"b"));
		assert_eq!(fragments[2].payload, bytes::Bytes::from_static(b"c"));
		let fragments = fragments
			.into_iter()
			.filter(|fragment| fragment.batch <= first)
			.map(|fragment| fragment::Key {
				batch: fragment.batch,
				index: fragment.index,
				partition: fragment.partition,
			})
			.collect();
		store
			.delete_object_index_outbox_fragments(fragment::delete::Arg { fragments })
			.await
			.unwrap();
		assert!(
			store
				.try_get_object_index_outbox_batch_at_or_before(batch::get::Arg {
					batch: Some(first),
					partition_end: 2,
					partition_start: 0,
				})
				.await
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
				.await
				.unwrap(),
			Some(second)
		);
		store
			.delete_object_index_outbox_batch(batch::delete::Arg {
				id: second,
				partition: 1,
			})
			.await
			.unwrap();
		assert!(
			store
				.try_get_object_index_outbox_batch_at_or_before(batch::get::Arg {
					batch: None,
					partition_end: 2,
					partition_start: 0,
				})
				.await
				.unwrap()
				.is_none()
		);
	}
}
