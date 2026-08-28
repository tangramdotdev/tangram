use {
	super::{Db, Key, Kind, Store},
	crate::object::archive::outbox,
	foundationdb_tuple::{self as fdbt, TuplePack as _},
	heed as lmdb,
	num_traits::ToPrimitive as _,
	tangram_client::prelude::*,
};

impl Store {
	pub async fn delete_object_archive_outbox_entries(
		&self,
		arg: outbox::delete::Arg,
	) -> tg::Result<()> {
		if arg.entries.is_empty() {
			return Ok(());
		}
		let request = super::request::Request::DeleteObjectArchiveOutboxEntries(arg);

		self.send_write_request(request).await
	}

	pub async fn dequeue_object_archive_outbox_entries(
		&self,
		arg: outbox::dequeue::Arg,
	) -> tg::Result<Vec<outbox::Entry>> {
		let request = crate::read::Request::DequeueObjectArchiveOutboxEntries(arg);
		let response = self.send_read_request(request).await?;
		let crate::read::Response::DequeueObjectArchiveOutboxEntries(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub async fn put_object_archive_outbox_entries(&self, arg: outbox::put::Arg) -> tg::Result<()> {
		if arg.entries.is_empty() {
			return Ok(());
		}
		let request = super::request::Request::PutObjectArchiveOutboxEntries(arg);

		self.send_write_request(request).await
	}

	pub(super) fn delete_object_archive_outbox_entries_with_transaction(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: outbox::delete::Arg,
	) -> tg::Result<()> {
		for entry in arg.entries {
			let key = Key::ObjectArchiveOutbox(entry);
			let key = key.pack_to_vec();
			db.delete(transaction, &key).map_err(|error| {
				tg::error!(!error, "failed to delete an object archive outbox entry")
			})?;
		}

		Ok(())
	}

	pub(super) fn dequeue_object_archive_outbox_entries_with_transaction(
		db: &Db,
		transaction: &lmdb::RoTxn<'_>,
		arg: &outbox::dequeue::Arg,
	) -> tg::Result<Vec<outbox::Entry>> {
		let mut entries = Vec::new();
		for partition in arg.partition_start..arg.partition_end {
			let prefix = fdbt::pack(&(Kind::ObjectArchiveOutbox.to_i32().unwrap(), partition));
			let iterator = db.prefix_iter(transaction, &prefix).map_err(|error| {
				tg::error!(!error, "failed to iterate the object archive outbox")
			})?;
			for entry in iterator {
				if entries.len() >= arg.batch_size {
					return Ok(entries);
				}
				let (key, _) = entry.map_err(|error| {
					tg::error!(!error, "failed to get an object archive outbox entry")
				})?;
				let (_, partition, stored_at, id): (i32, u64, i64, Vec<u8>) = fdbt::unpack(key)
					.map_err(|error| {
						tg::error!(!error, "failed to unpack an object archive outbox key")
					})?;
				let id = tg::object::Id::from_slice(&id)?;
				entries.push(outbox::Entry {
					id,
					partition,
					stored_at,
				});
			}
		}

		Ok(entries)
	}

	pub(super) fn put_object_archive_outbox_entries_with_transaction(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: outbox::put::Arg,
	) -> tg::Result<()> {
		for entry in arg.entries {
			let key = Key::ObjectArchiveOutbox(entry);
			let key = key.pack_to_vec();
			db.put(transaction, &key, &[]).map_err(|error| {
				tg::error!(
					!error,
					"failed to write an entry to the object archive outbox"
				)
			})?;
		}

		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use {super::*, bytes::Bytes, std::path::Path};

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
		let id = tg::object::Id::new(tg::object::Kind::Blob, &Bytes::from_static(b"object"));
		let entry = outbox::Entry {
			id,
			partition: 1,
			stored_at: 1,
		};
		let newer = outbox::Entry {
			stored_at: 2,
			..entry.clone()
		};
		{
			let store = store(temp.path());
			store
				.put_object_archive_outbox_entries(outbox::put::Arg {
					entries: vec![entry.clone(), newer.clone()],
				})
				.await
				.unwrap();
		}

		let store = store(temp.path());
		let entries = store
			.dequeue_object_archive_outbox_entries(outbox::dequeue::Arg {
				batch_size: usize::MAX,
				partition_end: 2,
				partition_start: 0,
			})
			.await
			.unwrap();
		assert_eq!(entries, vec![entry.clone(), newer.clone()]);
		store
			.delete_object_archive_outbox_entries(outbox::delete::Arg {
				entries: vec![entry],
			})
			.await
			.unwrap();
		let entries = store
			.dequeue_object_archive_outbox_entries(outbox::dequeue::Arg {
				batch_size: usize::MAX,
				partition_end: 2,
				partition_start: 0,
			})
			.await
			.unwrap();
		assert_eq!(entries, vec![newer.clone()]);
		store
			.delete_object_archive_outbox_entries(outbox::delete::Arg {
				entries: vec![newer],
			})
			.await
			.unwrap();
		let entries = store
			.dequeue_object_archive_outbox_entries(outbox::dequeue::Arg {
				batch_size: usize::MAX,
				partition_end: 2,
				partition_start: 0,
			})
			.await
			.unwrap();
		assert!(entries.is_empty());
	}
}
