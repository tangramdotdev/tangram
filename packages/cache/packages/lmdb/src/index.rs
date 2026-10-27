use {
	crate::{Cache, Db, Key},
	foundationdb_tuple::{self as fdbt, TuplePack as _},
	heed as lmdb,
	std::ops::Bound,
	tangram_cache::index,
	tangram_client::prelude::*,
};

impl Cache {
	pub async fn delete_index_queue_item(&self, arg: index::queue::delete::Arg) -> tg::Result<()> {
		let request = crate::request::Request::DeleteIndexQueueItem(arg);

		self.send_write_request(request).await
	}

	pub async fn delete_index_queue_item_batch(
		&self,
		arg: index::queue::delete::batch::Arg,
	) -> tg::Result<()> {
		let args = arg
			.sequences
			.into_iter()
			.map(|sequence| index::queue::delete::Arg {
				indexer: arg.indexer.clone(),
				sequence,
			})
			.collect();
		let request = crate::request::Request::DeleteIndexQueueItemBatch(args);
		self.send_write_request(request).await?;
		Ok(())
	}

	pub async fn get_index_queue_items(
		&self,
		arg: index::queue::get::batch::Arg,
	) -> tg::Result<Vec<index::queue::Item>> {
		let request = crate::read::Request::GetIndexQueueItems(arg);
		let response = self.send_read_request(request).await?;
		let crate::read::Response::GetIndexQueueItems(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub async fn put_index_queue_item(&self, arg: index::queue::put::Arg) -> tg::Result<()> {
		let request = crate::request::Request::PutIndexQueueItem(arg);

		self.send_write_request(request).await
	}

	pub async fn try_get_index_queue_item(
		&self,
		arg: index::queue::get::Arg,
	) -> tg::Result<Option<index::queue::Item>> {
		let request = crate::read::Request::TryGetIndexQueueItem(arg);
		let response = self.send_read_request(request).await?;
		let crate::read::Response::TryGetIndexQueueItem(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub(super) fn get_index_queue_items_with_transaction(
		db: &Db,
		transaction: &lmdb::RoTxn<'_>,
		arg: &index::queue::get::batch::Arg,
	) -> tg::Result<Vec<index::queue::Item>> {
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
					.map_err(|error| tg::error!(!error, "failed to get an index queue item"))?;
				let (_, _, sequence): (i32, Vec<u8>, u64) = fdbt::unpack(key)
					.map_err(|error| tg::error!(!error, "failed to unpack an index queue key"))?;

				decode_index_queue_item(arg.indexer.clone(), sequence, value)
			})
			.collect()
	}

	pub(super) fn delete_index_queue_item_with_transaction(
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
			.map_err(|error| tg::error!(!error, "failed to delete an index queue item"))?;

		Ok(())
	}

	pub(super) fn put_index_queue_item_with_transaction(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: index::queue::put::Arg,
	) -> tg::Result<()> {
		let item = arg.item;
		let key = Key::IndexQueue {
			indexer: &item.indexer,
			sequence: item.sequence,
		}
		.pack_to_vec();
		let batch = item.batch.value();
		let value = fdbt::pack(&(
			batch.as_slice(),
			item.fragment,
			item.fragments,
			item.payload.as_ref(),
		));
		db.put(transaction, &key, &value)
			.map_err(|error| tg::error!(!error, "failed to put an index queue item"))?;

		Ok(())
	}

	pub(super) fn try_get_index_queue_item_with_transaction(
		db: &Db,
		transaction: &lmdb::RoTxn<'_>,
		arg: &index::queue::get::Arg,
	) -> tg::Result<Option<index::queue::Item>> {
		let key = Key::IndexQueue {
			indexer: &arg.indexer,
			sequence: arg.sequence,
		}
		.pack_to_vec();
		let Some(value) = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to get an index queue item"))?
		else {
			return Ok(None);
		};
		let item = decode_index_queue_item(arg.indexer.clone(), arg.sequence, value)?;

		Ok(Some(item))
	}
}

impl tangram_cache::index::Cache for Cache {
	async fn delete_index_queue_item(
		&self,
		arg: tangram_cache::index::queue::delete::Arg,
	) -> tg::Result<()> {
		self.delete_index_queue_item(arg).await
	}

	async fn delete_index_queue_item_batch(
		&self,
		arg: tangram_cache::index::queue::delete::batch::Arg,
	) -> tg::Result<()> {
		self.delete_index_queue_item_batch(arg).await
	}

	async fn get_index_queue_items(
		&self,
		arg: tangram_cache::index::queue::get::batch::Arg,
	) -> tg::Result<Vec<tangram_cache::index::queue::Item>> {
		self.get_index_queue_items(arg).await
	}

	async fn put_index_queue_item(
		&self,
		arg: tangram_cache::index::queue::put::Arg,
	) -> tg::Result<()> {
		self.put_index_queue_item(arg).await
	}

	async fn try_get_index_queue_item(
		&self,
		arg: tangram_cache::index::queue::get::Arg,
	) -> tg::Result<Option<tangram_cache::index::queue::Item>> {
		self.try_get_index_queue_item(arg).await
	}
}

fn decode_index_queue_item(
	indexer: tg::indexer::Id,
	sequence: u64,
	value: &[u8],
) -> tg::Result<index::queue::Item> {
	let (batch, fragment, fragments, payload): (Vec<u8>, u64, u64, Vec<u8>) =
		fdbt::unpack(value)
			.map_err(|error| tg::error!(!error, "failed to unpack an index queue item"))?;
	let batch = batch
		.try_into()
		.map(index::queue::batch::Id::new)
		.map_err(|_| tg::error!("invalid index queue batch id"))?;
	let item = index::queue::Item {
		batch,
		fragment,
		fragments,
		indexer,
		payload: payload.into(),
		sequence,
	};

	Ok(item)
}
