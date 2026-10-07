use {
	crate::{Cache, Db, Key},
	foundationdb_tuple::{self as fdbt, TuplePack as _},
	heed as lmdb,
	std::ops::Bound,
	tangram_cache::archive,
	tangram_client::prelude::*,
};

impl Cache {
	pub async fn delete_archive_queue_entry(
		&self,
		arg: archive::queue::delete::Arg,
	) -> tg::Result<()> {
		let request = crate::request::Request::DeleteArchiveQueueEntry(arg);

		self.send_write_request(request).await
	}

	pub async fn get_archive_queue_entries(
		&self,
		arg: archive::queue::get::batch::Arg,
	) -> tg::Result<Vec<archive::queue::Entry>> {
		let request = crate::read::Request::GetArchiveQueueEntries(arg);
		let response = self.send_read_request(request).await?;
		let crate::read::Response::GetArchiveQueueEntries(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub async fn put_archive_queue_entry(&self, arg: archive::queue::put::Arg) -> tg::Result<()> {
		let request = crate::request::Request::PutArchiveQueueEntry(arg);

		self.send_write_request(request).await
	}

	pub async fn try_get_archive_queue_entry(
		&self,
		arg: archive::queue::get::Arg,
	) -> tg::Result<Option<archive::queue::Entry>> {
		let request = crate::read::Request::TryGetArchiveQueueEntry(arg);
		let response = self.send_read_request(request).await?;
		let crate::read::Response::TryGetArchiveQueueEntry(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub(super) fn delete_archive_queue_entry_with_transaction(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &archive::queue::delete::Arg,
	) -> tg::Result<()> {
		let key = Key::ArchiveQueue {
			indexer: &arg.indexer,
			sequence: arg.sequence,
		}
		.pack_to_vec();
		db.delete(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to delete an archive queue entry"))?;

		Ok(())
	}

	pub(super) fn get_archive_queue_entries_with_transaction(
		db: &Db,
		transaction: &lmdb::RoTxn<'_>,
		arg: &archive::queue::get::batch::Arg,
	) -> tg::Result<Vec<archive::queue::Entry>> {
		let sequence_start = Key::ArchiveQueue {
			indexer: &arg.indexer,
			sequence: arg.sequence_start,
		}
		.pack_to_vec();
		let sequence_end = Key::ArchiveQueue {
			indexer: &arg.indexer,
			sequence: arg.sequence_end,
		}
		.pack_to_vec();
		let range = (
			Bound::Included(sequence_start.as_slice()),
			Bound::Excluded(sequence_end.as_slice()),
		);
		let entries = db
			.range(transaction, &range)
			.map_err(|error| tg::error!(!error, "failed to iterate the archive queue"))?;
		entries
			.map(|entry| {
				let (key, value) = entry
					.map_err(|error| tg::error!(!error, "failed to get an archive queue entry"))?;
				let (_, _, sequence): (i32, Vec<u8>, u64) = fdbt::unpack(key)
					.map_err(|error| tg::error!(!error, "failed to unpack an archive queue key"))?;

				decode_archive_entry(arg.indexer.clone(), sequence, value)
			})
			.collect()
	}

	pub(super) fn put_archive_queue_entry_with_transaction(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: archive::queue::put::Arg,
	) -> tg::Result<()> {
		let entry = arg.entry;
		let key = Key::ArchiveQueue {
			indexer: &entry.indexer,
			sequence: entry.sequence,
		}
		.pack_to_vec();
		let object = entry.object.to_bytes();
		let value = fdbt::pack(&(object.as_ref(), entry.put.as_slice()));
		db.put(transaction, &key, &value)
			.map_err(|error| tg::error!(!error, "failed to put an archive queue entry"))?;

		Ok(())
	}

	pub(super) fn try_get_archive_queue_entry_with_transaction(
		db: &Db,
		transaction: &lmdb::RoTxn<'_>,
		arg: &archive::queue::get::Arg,
	) -> tg::Result<Option<archive::queue::Entry>> {
		let key = Key::ArchiveQueue {
			indexer: &arg.indexer,
			sequence: arg.sequence,
		}
		.pack_to_vec();
		let Some(value) = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to get an archive queue entry"))?
		else {
			return Ok(None);
		};
		let entry = decode_archive_entry(arg.indexer.clone(), arg.sequence, value)?;

		Ok(Some(entry))
	}
}

impl tangram_cache::archive::Cache for Cache {
	async fn delete_archive_queue_entry(
		&self,
		arg: tangram_cache::archive::queue::delete::Arg,
	) -> tg::Result<()> {
		self.delete_archive_queue_entry(arg).await
	}

	async fn get_archive_queue_entries(
		&self,
		arg: tangram_cache::archive::queue::get::batch::Arg,
	) -> tg::Result<Vec<tangram_cache::archive::queue::Entry>> {
		self.get_archive_queue_entries(arg).await
	}

	async fn put_archive_queue_entry(
		&self,
		arg: tangram_cache::archive::queue::put::Arg,
	) -> tg::Result<()> {
		self.put_archive_queue_entry(arg).await
	}

	async fn try_get_archive_queue_entry(
		&self,
		arg: tangram_cache::archive::queue::get::Arg,
	) -> tg::Result<Option<tangram_cache::archive::queue::Entry>> {
		self.try_get_archive_queue_entry(arg).await
	}
}

fn decode_archive_entry(
	indexer: tg::indexer::Id,
	sequence: u64,
	value: &[u8],
) -> tg::Result<archive::queue::Entry> {
	let (object, put): (Vec<u8>, Vec<u8>) = fdbt::unpack(value)
		.map_err(|error| tg::error!(!error, "failed to unpack an archive queue entry"))?;
	let object = tg::object::Id::from_slice(&object)?;
	let put = put
		.try_into()
		.map_err(|_| tg::error!("invalid archive queue put"))?;
	let entry = archive::queue::Entry {
		indexer,
		object,
		put,
		sequence,
	};

	Ok(entry)
}
