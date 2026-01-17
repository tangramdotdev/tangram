use {
	super::{Index, Key, Kind},
	foundationdb::options::MutationType,
	foundationdb_tuple::TuplePack as _,
	futures::TryFutureExt as _,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn get_transaction_id(&self) -> tg::Result<u64> {
		let txn = self
			.database
			.create_trx()
			.map_err(|source| tg::error!(!source, "failed to create transaction"))?;
		let version = txn
			.get_read_version()
			.await
			.map_err(|source| tg::error!(!source, "failed to get read version"))?;
		Ok(version.cast_unsigned())
	}

	pub async fn get_queue_size(&self, _transaction_id: u64) -> tg::Result<u64> {
		let txn = self
			.database
			.create_trx()
			.map_err(|source| tg::error!(!source, "failed to create transaction"))?;

		let cache_entry_key = Key::QueueCount {
			queue: Kind::CacheEntryQueue,
		}
		.pack_to_vec();
		let object_key = Key::QueueCount {
			queue: Kind::ObjectQueue,
		}
		.pack_to_vec();
		let process_key = Key::QueueCount {
			queue: Kind::ProcessQueue,
		}
		.pack_to_vec();

		let (cache_entry_count, object_count, process_count) = futures::future::try_join3(
			txn.get(&cache_entry_key, false)
				.map_err(|source| tg::error!(!source, "failed to get queue count")),
			txn.get(&object_key, false)
				.map_err(|source| tg::error!(!source, "failed to get queue count")),
			txn.get(&process_key, false)
				.map_err(|source| tg::error!(!source, "failed to get queue count")),
		)
		.await?;

		let cache_entry_count = cache_entry_count
			.map(|v| {
				Ok::<_, tg::Error>(u64::from_le_bytes(
					v.as_ref()
						.try_into()
						.map_err(|source| tg::error!(!source, "invalid queue count"))?,
				))
			})
			.transpose()?
			.unwrap_or(0);
		let object_count = object_count
			.map(|v| {
				Ok::<_, tg::Error>(u64::from_le_bytes(
					v.as_ref()
						.try_into()
						.map_err(|source| tg::error!(!source, "invalid queue count"))?,
				))
			})
			.transpose()?
			.unwrap_or(0);
		let process_count = process_count
			.map(|v| {
				Ok::<_, tg::Error>(u64::from_le_bytes(
					v.as_ref()
						.try_into()
						.map_err(|source| tg::error!(!source, "invalid queue count"))?,
				))
			})
			.transpose()?
			.unwrap_or(0);

		let count = cache_entry_count + object_count + process_count;

		Ok(count)
	}

	pub fn increment_queue_count(txn: &foundationdb::Transaction, queue: Kind, delta: i64) {
		let key = Key::QueueCount { queue }.pack_to_vec();
		let value = delta.to_le_bytes();
		txn.atomic_op(&key, &value, MutationType::Add);
	}
}
