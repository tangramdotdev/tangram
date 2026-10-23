use {
	crate::{Cache, Key},
	foundationdb_tuple::{self as fdbt, TuplePack as _},
	std::ops::Bound,
	tangram_cache::index,
	tangram_client::prelude::*,
};

impl Cache {
	pub async fn delete_index_queue_fragment(
		&self,
		arg: index::queue::delete::Arg,
	) -> tg::Result<()> {
		let request = crate::request::Request::DeleteIndexQueueFragment(arg);
		self.send_write_request(request).await?;
		Ok(())
	}

	pub async fn get_index_queue_fragments(
		&self,
		arg: index::queue::get::batch::Arg,
	) -> tg::Result<Vec<index::queue::Fragment>> {
		let request = crate::read::Request::GetIndexQueueFragments(arg);
		let response = self.send_read_request(request).await?;
		let crate::read::Response::GetIndexQueueFragments(output) = response else {
			return Err(tg::error!("received an unexpected read response"));
		};
		Ok(output)
	}

	pub async fn put_index_queue_fragment(&self, arg: index::queue::put::Arg) -> tg::Result<()> {
		let request = crate::request::Request::PutIndexQueueFragment(arg);
		self.send_write_request(request).await?;
		Ok(())
	}

	pub async fn try_get_index_queue_fragment(
		&self,
		arg: index::queue::get::Arg,
	) -> tg::Result<Option<index::queue::Fragment>> {
		let request = crate::read::Request::TryGetIndexQueueFragment(arg);
		let response = self.send_read_request(request).await?;
		let crate::read::Response::TryGetIndexQueueFragment(output) = response else {
			return Err(tg::error!("received an unexpected read response"));
		};
		Ok(output)
	}

	pub(super) fn delete_index_queue_fragment_with_transaction(
		transaction: &mut crate::transaction::Transaction<'_>,
		arg: &index::queue::delete::Arg,
	) -> tg::Result<()> {
		let key = Key::IndexQueue {
			indexer: &arg.indexer,
			sequence: arg.sequence,
		}
		.pack_to_vec();
		transaction
			.delete(&key)
			.map_err(|error| tg::error!(!error, "failed to delete an index queue fragment"))?;

		Ok(())
	}

	pub(super) fn get_index_queue_fragments_with_transaction(
		transaction: &crate::transaction::Transaction<'_>,
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
		let entries = transaction.range(&range);
		entries
			.map(|entry| {
				let (key, value) = entry
					.map_err(|error| tg::error!(!error, "failed to get an index queue fragment"))?;
				let (_, _, sequence): (i32, Vec<u8>, u64) = fdbt::unpack(&key)
					.map_err(|error| tg::error!(!error, "failed to unpack an index queue key"))?;

				decode_index_fragment(arg.indexer.clone(), sequence, &value)
			})
			.collect()
	}

	pub(super) fn put_index_queue_fragment_with_transaction(
		transaction: &mut crate::transaction::Transaction<'_>,
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
		transaction
			.put(&key, &value)
			.map_err(|error| tg::error!(!error, "failed to put an index queue fragment"))?;

		Ok(())
	}

	pub(super) fn try_get_index_queue_fragment_with_transaction(
		transaction: &crate::transaction::Transaction<'_>,
		arg: &index::queue::get::Arg,
	) -> tg::Result<Option<index::queue::Fragment>> {
		let key = Key::IndexQueue {
			indexer: &arg.indexer,
			sequence: arg.sequence,
		}
		.pack_to_vec();
		let Some(value) = transaction
			.get(&key)
			.map_err(|error| tg::error!(!error, "failed to get an index queue fragment"))?
		else {
			return Ok(None);
		};
		let fragment = decode_index_fragment(arg.indexer.clone(), arg.sequence, &value)?;

		Ok(Some(fragment))
	}
}

impl tangram_cache::index::Cache for Cache {
	async fn delete_index_queue_fragment(
		&self,
		arg: tangram_cache::index::queue::delete::Arg,
	) -> tg::Result<()> {
		self.delete_index_queue_fragment(arg).await?;
		Ok(())
	}

	async fn get_index_queue_fragments(
		&self,
		arg: tangram_cache::index::queue::get::batch::Arg,
	) -> tg::Result<Vec<tangram_cache::index::queue::Fragment>> {
		self.get_index_queue_fragments(arg).await
	}

	async fn put_index_queue_fragment(
		&self,
		arg: tangram_cache::index::queue::put::Arg,
	) -> tg::Result<()> {
		self.put_index_queue_fragment(arg).await?;
		Ok(())
	}

	async fn try_get_index_queue_fragment(
		&self,
		arg: tangram_cache::index::queue::get::Arg,
	) -> tg::Result<Option<tangram_cache::index::queue::Fragment>> {
		self.try_get_index_queue_fragment(arg).await
	}
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
		.map_err(|_| tg::error!("the index queue batch id is invalid"))?;
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
