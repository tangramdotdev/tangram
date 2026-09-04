use {
	super::{Db, Key, Store},
	crate::object,
	foundationdb_tuple::{self as fdbt, TuplePack as _},
	heed as lmdb,
	tangram_client::prelude::*,
};

impl Store {
	pub async fn delete_object_archive_queue_entry(
		&self,
		arg: object::archive::queue::delete::Arg,
	) -> tg::Result<()> {
		let request = super::request::Request::DeleteObjectArchiveQueueEntry(arg);

		self.send_write_request(request).await
	}

	pub async fn delete_object_index_queue_fragment(
		&self,
		arg: object::index::queue::delete::Arg,
	) -> tg::Result<()> {
		let request = super::request::Request::DeleteObjectIndexQueueFragment(arg);

		self.send_write_request(request).await
	}

	pub async fn put_object_archive_queue_entry(
		&self,
		arg: object::archive::queue::put::Arg,
	) -> tg::Result<()> {
		let request = super::request::Request::PutObjectArchiveQueueEntry(arg);

		self.send_write_request(request).await
	}

	pub async fn put_object_index_queue_fragment(
		&self,
		arg: object::index::queue::put::Arg,
	) -> tg::Result<()> {
		let request = super::request::Request::PutObjectIndexQueueFragment(arg);

		self.send_write_request(request).await
	}

	pub async fn try_get_object_archive_queue_entry(
		&self,
		arg: object::archive::queue::get::Arg,
	) -> tg::Result<Option<object::archive::queue::Entry>> {
		let request = crate::read::Request::TryGetObjectArchiveQueueEntry(arg);
		let response = self.send_read_request(request).await?;
		let crate::read::Response::TryGetObjectArchiveQueueEntry(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub async fn try_get_object_index_queue_fragment(
		&self,
		arg: object::index::queue::get::Arg,
	) -> tg::Result<Option<object::index::queue::Fragment>> {
		let request = crate::read::Request::TryGetObjectIndexQueueFragment(arg);
		let response = self.send_read_request(request).await?;
		let crate::read::Response::TryGetObjectIndexQueueFragment(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub(super) fn delete_object_archive_queue_entry_with_transaction(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &object::archive::queue::delete::Arg,
	) -> tg::Result<()> {
		let key = Key::ObjectArchiveQueue {
			indexer: &arg.indexer,
			sequence: arg.sequence,
		}
		.pack_to_vec();
		db.delete(transaction, &key).map_err(|error| {
			tg::error!(!error, "failed to delete an object archive queue entry")
		})?;

		Ok(())
	}

	pub(super) fn delete_object_index_queue_fragment_with_transaction(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &object::index::queue::delete::Arg,
	) -> tg::Result<()> {
		let key = Key::ObjectIndexQueue {
			indexer: &arg.indexer,
			sequence: arg.sequence,
		}
		.pack_to_vec();
		db.delete(transaction, &key).map_err(|error| {
			tg::error!(!error, "failed to delete an object index queue fragment")
		})?;

		Ok(())
	}

	pub(super) fn put_object_archive_queue_entry_with_transaction(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: object::archive::queue::put::Arg,
	) -> tg::Result<()> {
		let entry = arg.entry;
		let key = Key::ObjectArchiveQueue {
			indexer: &entry.indexer,
			sequence: entry.sequence,
		}
		.pack_to_vec();
		let object = entry.object.to_bytes();
		let value = fdbt::pack(&(object.as_ref(), entry.put.as_slice()));
		db.put(transaction, &key, &value)
			.map_err(|error| tg::error!(!error, "failed to put an object archive queue entry"))?;

		Ok(())
	}

	pub(super) fn put_object_index_queue_fragment_with_transaction(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: object::index::queue::put::Arg,
	) -> tg::Result<()> {
		let fragment = arg.fragment;
		let key = Key::ObjectIndexQueue {
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
			.map_err(|error| tg::error!(!error, "failed to put an object index queue fragment"))?;

		Ok(())
	}

	pub(super) fn try_get_object_archive_queue_entry_with_transaction(
		db: &Db,
		transaction: &lmdb::RoTxn<'_>,
		arg: &object::archive::queue::get::Arg,
	) -> tg::Result<Option<object::archive::queue::Entry>> {
		let key = Key::ObjectArchiveQueue {
			indexer: &arg.indexer,
			sequence: arg.sequence,
		}
		.pack_to_vec();
		let Some(value) = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to get an object archive queue entry"))?
		else {
			return Ok(None);
		};
		let (object, put): (Vec<u8>, Vec<u8>) = fdbt::unpack(value).map_err(|error| {
			tg::error!(!error, "failed to unpack an object archive queue entry")
		})?;
		let object = tg::object::Id::from_slice(&object)?;
		let put = put
			.try_into()
			.map_err(|_| tg::error!("invalid object archive queue put"))?;
		let entry = object::archive::queue::Entry {
			indexer: arg.indexer.clone(),
			object,
			put,
			sequence: arg.sequence,
		};

		Ok(Some(entry))
	}

	pub(super) fn try_get_object_index_queue_fragment_with_transaction(
		db: &Db,
		transaction: &lmdb::RoTxn<'_>,
		arg: &object::index::queue::get::Arg,
	) -> tg::Result<Option<object::index::queue::Fragment>> {
		let key = Key::ObjectIndexQueue {
			indexer: &arg.indexer,
			sequence: arg.sequence,
		}
		.pack_to_vec();
		let Some(value) = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to get an object index queue fragment"))?
		else {
			return Ok(None);
		};
		let (batch, fragment, fragments, payload): (Vec<u8>, u64, u64, Vec<u8>) =
			fdbt::unpack(value).map_err(|error| {
				tg::error!(!error, "failed to unpack an object index queue fragment")
			})?;
		let batch = batch
			.try_into()
			.map(object::index::queue::batch::Id::new)
			.map_err(|_| tg::error!("invalid object index queue batch id"))?;
		let fragment = object::index::queue::Fragment {
			batch,
			fragment,
			fragments,
			indexer: arg.indexer.clone(),
			payload: payload.into(),
			sequence: arg.sequence,
		};

		Ok(Some(fragment))
	}
}
