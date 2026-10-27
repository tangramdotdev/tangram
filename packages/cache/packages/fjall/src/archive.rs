use {
	crate::{Cache, Key},
	foundationdb_tuple::{self as fdbt, TuplePack as _},
	std::ops::Bound,
	tangram_cache::archive,
	tangram_client::prelude::*,
};

impl Cache {
	pub async fn delete_archive_queue_item(
		&self,
		arg: archive::queue::delete::Arg,
	) -> tg::Result<()> {
		let request = crate::request::Request::DeleteArchiveQueueItem(arg);
		self.send_write_request(request).await?;
		Ok(())
	}

	pub async fn delete_archive_queue_item_batch(
		&self,
		arg: archive::queue::delete::batch::Arg,
	) -> tg::Result<()> {
		let args = arg
			.sequences
			.into_iter()
			.map(|sequence| archive::queue::delete::Arg {
				indexer: arg.indexer.clone(),
				sequence,
			})
			.collect();
		let request = crate::request::Request::DeleteArchiveQueueItemBatch(args);
		self.send_write_request(request).await?;
		Ok(())
	}

	pub async fn get_archive_queue_items(
		&self,
		arg: archive::queue::get::batch::Arg,
	) -> tg::Result<Vec<archive::queue::Item>> {
		let request = crate::read::Request::GetArchiveQueueItems(arg);
		let response = self.send_read_request(request).await?;
		let crate::read::Response::GetArchiveQueueItems(output) = response else {
			return Err(tg::error!("received an unexpected read response"));
		};
		Ok(output)
	}

	pub async fn put_archive_queue_item(&self, arg: archive::queue::put::Arg) -> tg::Result<()> {
		let request = crate::request::Request::PutArchiveQueueItem(arg);
		self.send_write_request(request).await?;
		Ok(())
	}

	pub async fn try_get_archive_queue_item(
		&self,
		arg: archive::queue::get::Arg,
	) -> tg::Result<Option<archive::queue::Item>> {
		let request = crate::read::Request::TryGetArchiveQueueItem(arg);
		let response = self.send_read_request(request).await?;
		let crate::read::Response::TryGetArchiveQueueItem(output) = response else {
			return Err(tg::error!("received an unexpected read response"));
		};
		Ok(output)
	}

	pub(super) fn delete_archive_queue_item_with_transaction(
		transaction: &mut crate::transaction::Transaction<'_>,
		arg: &archive::queue::delete::Arg,
	) -> tg::Result<()> {
		let key = Key::ArchiveQueue {
			indexer: &arg.indexer,
			sequence: arg.sequence,
		}
		.pack_to_vec();
		transaction
			.delete(&key)
			.map_err(|error| tg::error!(!error, "failed to delete an archive queue item"))?;

		Ok(())
	}

	pub(super) fn get_archive_queue_items_with_transaction(
		transaction: &crate::transaction::Transaction<'_>,
		arg: &archive::queue::get::batch::Arg,
	) -> tg::Result<Vec<archive::queue::Item>> {
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
		let entries = transaction.range(&range);
		entries
			.map(|entry| {
				let (key, value) = entry
					.map_err(|error| tg::error!(!error, "failed to get an archive queue item"))?;
				let (_, _, sequence): (i32, Vec<u8>, u64) = fdbt::unpack(&key)
					.map_err(|error| tg::error!(!error, "failed to unpack an archive queue key"))?;

				decode_archive_queue_item(arg.indexer.clone(), sequence, &value)
			})
			.collect()
	}

	pub(super) fn put_archive_queue_item_with_transaction(
		transaction: &mut crate::transaction::Transaction<'_>,
		arg: archive::queue::put::Arg,
	) -> tg::Result<()> {
		let item = arg.item;
		let key = Key::ArchiveQueue {
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
		transaction
			.put(&key, &value)
			.map_err(|error| tg::error!(!error, "failed to put an archive queue item"))?;

		Ok(())
	}

	pub(super) fn try_get_archive_queue_item_with_transaction(
		transaction: &crate::transaction::Transaction<'_>,
		arg: &archive::queue::get::Arg,
	) -> tg::Result<Option<archive::queue::Item>> {
		let key = Key::ArchiveQueue {
			indexer: &arg.indexer,
			sequence: arg.sequence,
		}
		.pack_to_vec();
		let Some(value) = transaction
			.get(&key)
			.map_err(|error| tg::error!(!error, "failed to get an archive queue item"))?
		else {
			return Ok(None);
		};
		let item = decode_archive_queue_item(arg.indexer.clone(), arg.sequence, &value)?;

		Ok(Some(item))
	}
}

impl tangram_cache::archive::Cache for Cache {
	async fn delete_archive_queue_item(
		&self,
		arg: tangram_cache::archive::queue::delete::Arg,
	) -> tg::Result<()> {
		self.delete_archive_queue_item(arg).await?;
		Ok(())
	}

	async fn delete_archive_queue_item_batch(
		&self,
		arg: tangram_cache::archive::queue::delete::batch::Arg,
	) -> tg::Result<()> {
		self.delete_archive_queue_item_batch(arg).await
	}

	async fn get_archive_queue_items(
		&self,
		arg: tangram_cache::archive::queue::get::batch::Arg,
	) -> tg::Result<Vec<tangram_cache::archive::queue::Item>> {
		self.get_archive_queue_items(arg).await
	}

	async fn put_archive_queue_item(
		&self,
		arg: tangram_cache::archive::queue::put::Arg,
	) -> tg::Result<()> {
		self.put_archive_queue_item(arg).await?;
		Ok(())
	}

	async fn try_get_archive_queue_item(
		&self,
		arg: tangram_cache::archive::queue::get::Arg,
	) -> tg::Result<Option<tangram_cache::archive::queue::Item>> {
		self.try_get_archive_queue_item(arg).await
	}
}

fn decode_archive_queue_item(
	indexer: tg::indexer::Id,
	sequence: u64,
	value: &[u8],
) -> tg::Result<archive::queue::Item> {
	let (batch, fragment, fragments, payload): (Vec<u8>, u64, u64, Vec<u8>) =
		fdbt::unpack(value)
			.map_err(|error| tg::error!(!error, "failed to unpack an archive queue item"))?;
	let batch = batch
		.try_into()
		.map(archive::queue::batch::Id::new)
		.map_err(|_| tg::error!("the archive queue batch id is invalid"))?;
	let item = archive::queue::Item {
		batch,
		fragment,
		fragments,
		indexer,
		payload: payload.into(),
		sequence,
	};

	Ok(item)
}
