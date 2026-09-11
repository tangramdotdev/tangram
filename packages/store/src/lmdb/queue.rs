use {
	super::{Db, Key, Store},
	crate::{archive, index},
	foundationdb_tuple::{self as fdbt, TuplePack as _},
	heed as lmdb,
	std::ops::Bound,
	tangram_client::prelude::*,
};

impl Store {
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

	pub async fn get_index_queue_fragments(
		&self,
		arg: index::queue::get::batch::Arg,
	) -> tg::Result<Vec<index::queue::Fragment>> {
		let request = crate::read::Request::GetIndexQueueFragments(arg);
		let response = self.send_read_request(request).await?;
		let crate::read::Response::GetIndexQueueFragments(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub async fn delete_archive_queue_entry(
		&self,
		arg: archive::queue::delete::Arg,
	) -> tg::Result<()> {
		let request = super::request::Request::DeleteArchiveQueueEntry(arg);

		self.send_write_request(request).await
	}

	pub async fn delete_index_queue_fragment(
		&self,
		arg: index::queue::delete::Arg,
	) -> tg::Result<()> {
		let request = super::request::Request::DeleteIndexQueueFragment(arg);

		self.send_write_request(request).await
	}

	pub async fn put_archive_queue_entry(&self, arg: archive::queue::put::Arg) -> tg::Result<()> {
		let request = super::request::Request::PutArchiveQueueEntry(arg);

		self.send_write_request(request).await
	}

	pub async fn put_index_queue_fragment(&self, arg: index::queue::put::Arg) -> tg::Result<()> {
		let request = super::request::Request::PutIndexQueueFragment(arg);

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

	pub async fn try_get_index_queue_fragment(
		&self,
		arg: index::queue::get::Arg,
	) -> tg::Result<Option<index::queue::Fragment>> {
		let request = crate::read::Request::TryGetIndexQueueFragment(arg);
		let response = self.send_read_request(request).await?;
		let crate::read::Response::TryGetIndexQueueFragment(output) = response else {
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

	pub(super) fn get_index_queue_fragments_with_transaction(
		db: &Db,
		transaction: &lmdb::RoTxn<'_>,
		arg: &index::queue::get::batch::Arg,
	) -> tg::Result<Vec<index::queue::Fragment>> {
		let sequence_start = Key::IndexQueue {
			indexer: &arg.indexer,
			sequence: arg.sequence_start,
		}
		.pack_to_vec();
		let sequence_end = Key::IndexQueue {
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
			.map_err(|error| tg::error!(!error, "failed to iterate the index queue"))?;
		entries
			.map(|entry| {
				let (key, value) = entry
					.map_err(|error| tg::error!(!error, "failed to get an index queue fragment"))?;
				let (_, _, sequence): (i32, Vec<u8>, u64) = fdbt::unpack(key)
					.map_err(|error| tg::error!(!error, "failed to unpack an index queue key"))?;

				decode_index_fragment(arg.indexer.clone(), sequence, value)
			})
			.collect()
	}

	pub(super) fn delete_index_queue_fragment_with_transaction(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &index::queue::delete::Arg,
	) -> tg::Result<()> {
		let key = Key::IndexQueue {
			indexer: &arg.indexer,
			sequence: arg.sequence,
		}
		.pack_to_vec();
		db.delete(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to delete an index queue fragment"))?;

		Ok(())
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

	pub(super) fn put_index_queue_fragment_with_transaction(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: index::queue::put::Arg,
	) -> tg::Result<()> {
		let fragment = arg.fragment;
		let key = Key::IndexQueue {
			indexer: &fragment.indexer,
			sequence: fragment.sequence,
		}
		.pack_to_vec();
		let batch = fragment.batch.value();
		let value = fdbt::pack(&(
			batch.as_slice(),
			fragment.fragment,
			fragment.fragments,
			fragment.payload.as_ref(),
		));
		db.put(transaction, &key, &value)
			.map_err(|error| tg::error!(!error, "failed to put an index queue fragment"))?;

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

	pub(super) fn try_get_index_queue_fragment_with_transaction(
		db: &Db,
		transaction: &lmdb::RoTxn<'_>,
		arg: &index::queue::get::Arg,
	) -> tg::Result<Option<index::queue::Fragment>> {
		let key = Key::IndexQueue {
			indexer: &arg.indexer,
			sequence: arg.sequence,
		}
		.pack_to_vec();
		let Some(value) = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to get an index queue fragment"))?
		else {
			return Ok(None);
		};
		let fragment = decode_index_fragment(arg.indexer.clone(), arg.sequence, value)?;

		Ok(Some(fragment))
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

fn decode_index_fragment(
	indexer: tg::indexer::Id,
	sequence: u64,
	value: &[u8],
) -> tg::Result<index::queue::Fragment> {
	let (batch, fragment, fragments, payload): (Vec<u8>, u64, u64, Vec<u8>) =
		fdbt::unpack(value)
			.map_err(|error| tg::error!(!error, "failed to unpack an index queue fragment"))?;
	let batch = batch
		.try_into()
		.map(index::queue::batch::Id::new)
		.map_err(|_| tg::error!("invalid index queue batch id"))?;
	let fragment = index::queue::Fragment {
		batch,
		fragment,
		fragments,
		indexer,
		payload: payload.into(),
		sequence,
	};

	Ok(fragment)
}
