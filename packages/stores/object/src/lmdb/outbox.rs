use {
	super::{Db, Key, KeyKind, Store},
	crate::outbox::{
		Batch, BatchId, DeleteArg, DequeueArg, Fragment, FragmentIndex, TryGetBatchArg,
	},
	foundationdb_tuple::{self as fdbt, TuplePack as _},
	heed as lmdb,
	num::ToPrimitive as _,
	tangram_client::prelude::*,
};

impl Store {
	pub async fn delete_outbox_fragments(&self, arg: DeleteArg) -> tg::Result<()> {
		if arg.fragments.is_empty() {
			return Ok(());
		}
		let request = super::request::Request::DeleteOutboxFragments(arg);

		self.send_write_request(request).await
	}

	pub async fn dequeue_outbox_fragments(&self, arg: DequeueArg) -> tg::Result<Vec<Fragment>> {
		let request = crate::read::Request::DequeueOutboxFragments(arg);
		let response = self.send_read_request(request).await?;
		let crate::read::Response::DequeueOutboxFragments(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub async fn enqueue_outbox_batch(&self, batch: Batch) -> tg::Result<()> {
		let request = super::request::Request::EnqueueOutboxBatch(batch);

		self.send_write_request(request).await
	}

	pub async fn try_get_outbox_batch_at_or_before(
		&self,
		arg: TryGetBatchArg,
	) -> tg::Result<Option<BatchId>> {
		let request = crate::read::Request::TryGetOutboxBatchAtOrBefore(arg);
		let response = self.send_read_request(request).await?;
		let crate::read::Response::TryGetOutboxBatchAtOrBefore(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub(super) fn dequeue_outbox_fragments_with_transaction(
		db: &Db,
		transaction: &lmdb::RoTxn<'_>,
		arg: &DequeueArg,
	) -> tg::Result<Vec<Fragment>> {
		let mut fragments = Vec::new();
		for partition in arg.partition_start..arg.partition_end {
			let prefix = fdbt::pack(&(KeyKind::Outbox.to_i32().unwrap(), partition));
			let entries = db
				.prefix_iter(transaction, &prefix)
				.map_err(|error| tg::error!(!error, "failed to iterate the outbox"))?;
			for entry in entries {
				if fragments.len() >= arg.batch_size {
					return Ok(fragments);
				}
				let (key, payload) = entry
					.map_err(|error| tg::error!(!error, "failed to get an outbox fragment"))?;
				let (partition, batch, index) = unpack_key(key)?;
				fragments.push(Fragment {
					batch: BatchId::new(batch),
					index: FragmentIndex::new(index),
					partition,
					payload: bytes::Bytes::copy_from_slice(payload),
				});
			}
		}

		Ok(fragments)
	}

	pub(super) fn try_get_outbox_batch_at_or_before_with_transaction(
		db: &Db,
		transaction: &lmdb::RoTxn<'_>,
		arg: &TryGetBatchArg,
	) -> tg::Result<Option<BatchId>> {
		let mut output = None;
		for partition in arg.partition_start..arg.partition_end {
			let prefix = fdbt::pack(&(KeyKind::Outbox.to_i32().unwrap(), partition));
			let entries = db
				.rev_prefix_iter(transaction, &prefix)
				.map_err(|error| tg::error!(!error, "failed to iterate the outbox"))?;
			for entry in entries {
				let (key, _) = entry
					.map_err(|error| tg::error!(!error, "failed to get an outbox fragment"))?;
				let (_, batch, _) = unpack_key(key)?;
				if arg.batch.is_some_and(|target| batch > target.value()) {
					continue;
				}
				output = Some(output.map_or(batch, |output: [u8; 16]| output.max(batch)));
				break;
			}
		}

		Ok(output.map(BatchId::new))
	}

	pub(super) fn delete_outbox_fragments_with_transaction(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: DeleteArg,
	) -> tg::Result<()> {
		for fragment in arg.fragments {
			let key = Key::Outbox {
				batch: fragment.batch.value(),
				index: fragment.index.value(),
				partition: fragment.partition,
			};
			db.delete(transaction, &key.pack_to_vec())
				.map_err(|error| tg::error!(!error, "failed to delete the outbox fragment"))?;
		}

		Ok(())
	}

	pub(super) fn enqueue_outbox_batch_with_transaction(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		batch: Batch,
	) -> tg::Result<()> {
		for (index, payload) in batch.fragments.into_iter().enumerate() {
			let index = u64::try_from(index)
				.map_err(|_| tg::error!("the outbox fragment index exceeded a u64"))?;
			let key = Key::Outbox {
				batch: batch.id.value(),
				index,
				partition: batch.partition,
			};
			db.put(transaction, &key.pack_to_vec(), &payload)
				.map_err(|error| tg::error!(!error, "failed to put the outbox fragment"))?;
		}

		Ok(())
	}
}

fn decode_batch(bytes: &[u8]) -> tg::Result<[u8; 16]> {
	bytes
		.try_into()
		.map_err(|_| tg::error!("invalid outbox batch id length"))
}

fn unpack_key(bytes: &[u8]) -> tg::Result<(u64, [u8; 16], u64)> {
	let (_, partition, batch, index): (i32, u64, Vec<u8>, u64) = fdbt::unpack(bytes)
		.map_err(|error| tg::error!(!error, "failed to unpack the outbox key"))?;
	let batch = decode_batch(&batch)?;

	Ok((partition, batch, index))
}

#[cfg(test)]
mod tests {
	use {super::*, crate::outbox::FragmentKey, std::path::Path};

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
		let first = BatchId::new(1_u128.to_be_bytes());
		let second = BatchId::new(2_u128.to_be_bytes());
		{
			let store = store(temp.path());
			store
				.enqueue_outbox_batch(Batch {
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
				.enqueue_outbox_batch(Batch {
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
				.try_get_outbox_batch_at_or_before(TryGetBatchArg {
					batch: None,
					partition_end: 2,
					partition_start: 0,
				})
				.await
				.unwrap(),
			Some(second)
		);
		let fragments = store
			.dequeue_outbox_fragments(DequeueArg {
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
			.map(|fragment| FragmentKey {
				batch: fragment.batch,
				index: fragment.index,
				partition: fragment.partition,
			})
			.collect();
		store
			.delete_outbox_fragments(DeleteArg { fragments })
			.await
			.unwrap();
		assert!(
			store
				.try_get_outbox_batch_at_or_before(TryGetBatchArg {
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
				.try_get_outbox_batch_at_or_before(TryGetBatchArg {
					batch: None,
					partition_end: 2,
					partition_start: 0,
				})
				.await
				.unwrap(),
			Some(second)
		);
	}
}
