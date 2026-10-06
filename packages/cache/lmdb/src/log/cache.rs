use {
	crate::{Cache, Db, Key as CacheKey, Kind},
	foundationdb_tuple::{self as fdbt, TuplePack as _},
	heed as lmdb,
	num_traits::ToPrimitive as _,
	tangram_cache::log,
	tangram_client::prelude::*,
};

impl Cache {
	pub async fn delete_log_cache_entry(&self, arg: log::cache::delete::Arg) -> tg::Result<()> {
		let request = crate::request::Request::DeleteLogCacheEntry(arg);
		self.send_write_request(request).await?;
		Ok(())
	}

	pub(crate) fn delete_log_cache_entry_with_transaction(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: log::cache::delete::Arg,
	) -> tg::Result<()> {
		let entry = arg.entry;
		let arg = log::delete::Arg {
			process: entry.process.clone(),
		};
		Self::delete_log_with_transaction(db, transaction, &arg)?;
		let key = CacheKey::LogCache(entry).pack_to_vec();
		db.delete(transaction, &key)
			.map_err(|error| tg::error!(!error, "failed to delete a log cache entry"))?;
		Ok(())
	}

	pub async fn get_log_cache_entries(
		&self,
		arg: log::cache::get::Arg,
	) -> tg::Result<Vec<log::cache::Entry>> {
		let request = crate::read::Request::GetLogCacheEntries(arg);
		let response = self.send_read_request(request).await?;
		let crate::read::Response::GetLogCacheEntries(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};
		Ok(output)
	}

	pub(crate) fn get_log_cache_entries_with_transaction(
		db: &Db,
		transaction: &lmdb::RoTxn<'_>,
		arg: &log::cache::get::Arg,
	) -> tg::Result<Vec<log::cache::Entry>> {
		let prefix = fdbt::pack(&(Kind::LogCache.to_i32().unwrap(), arg.partition));
		let entries = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to iterate the log cache"))?;
		let mut output = Vec::new();
		for entry in entries.take(arg.batch_size) {
			let (key, _) =
				entry.map_err(|error| tg::error!(!error, "failed to get a log cache entry"))?;
			let (_, partition, expires_at, process): (i32, u64, i64, Vec<u8>) =
				fdbt::unpack(key)
					.map_err(|error| tg::error!(!error, "failed to unpack a log cache key"))?;
			if expires_at > arg.now {
				break;
			}
			let process = tg::process::Id::from_slice(&process)?;
			let entry = log::cache::Entry {
				expires_at,
				partition,
				process,
			};
			output.push(entry);
		}

		Ok(output)
	}

	pub async fn put_log_cache_entry(&self, arg: log::cache::put::Arg) -> tg::Result<()> {
		let request = crate::request::Request::PutLogCacheEntry(arg);
		self.send_write_request(request).await?;
		Ok(())
	}

	pub(crate) fn put_log_cache_entry_with_transaction(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: log::cache::put::Arg,
	) -> tg::Result<()> {
		let key = CacheKey::LogCache(arg.entry).pack_to_vec();
		db.put(transaction, &key, &[])
			.map_err(|error| tg::error!(!error, "failed to put a log cache entry"))?;
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use {super::*, bytes::Bytes, std::collections::BTreeSet};

	#[tokio::test]
	async fn expiration_deletes_the_complete_log_and_preserves_other_entries() {
		let temp = tangram_util::fs::Temp::new().unwrap();
		std::fs::create_dir(temp.path()).unwrap();
		let config = crate::Config {
			map_size: 10 * 1024 * 1024,
			posix_sem_prefix: None,
			path: temp.path().join("cache"),
			read_batch_size: 64,
			read_concurrency: 1,
			write_batch_size: 64,
		};
		let cache = Cache::new(&config).unwrap();
		let process = tg::process::Id::new();
		let other = tg::process::Id::new();
		let stream = tg::process::stdio::Stream::Stdout;
		let arg = log::put::Arg {
			bytes: Bytes::from_static(b"log"),
			position: 0,
			process: process.clone(),
			stream,
			stream_position: 0,
			timestamp: 0,
		};
		cache.put_log(arg).await.unwrap();
		let arg = log::end::Arg {
			end: tg::process::log::End {
				position: 3,
				stderr_position: 0,
				stdout_position: 3,
			},
			process: process.clone(),
		};
		cache.put_log_end(arg).await.unwrap();

		for (expires_at, partition, process) in [
			(10, 0, process.clone()),
			(20, 0, other.clone()),
			(5, 1, other.clone()),
		] {
			let entry = log::cache::Entry {
				expires_at,
				partition,
				process,
			};
			let arg = log::cache::put::Arg { entry };
			cache.put_log_cache_entry(arg.clone()).await.unwrap();
			cache.put_log_cache_entry(arg).await.unwrap();
		}

		let arg = log::cache::get::Arg {
			batch_size: 10,
			now: 9,
			partition: 0,
		};
		assert_eq!(cache.get_log_cache_entries(arg).await.unwrap(), []);
		let arg = log::cache::get::Arg { now: 10, ..arg };
		let entries = cache.get_log_cache_entries(arg).await.unwrap();
		assert_eq!(entries.len(), 1);

		let arg = log::cache::delete::Arg {
			entry: entries[0].clone(),
		};
		cache.delete_log_cache_entry(arg).await.unwrap();
		assert!(cache.try_get_log_end(&process).await.unwrap().is_none());

		let arg = log::read::Arg {
			length: 10,
			position: 0,
			process,
			streams: BTreeSet::from([stream]),
		};
		assert!(cache.try_read_log(arg).await.unwrap().is_empty());

		let arg = log::cache::get::Arg {
			batch_size: 1,
			now: 20,
			partition: 0,
		};
		assert_eq!(
			cache.get_log_cache_entries(arg).await.unwrap()[0].process,
			other
		);
		let arg = log::cache::get::Arg {
			partition: 1,
			..arg
		};
		assert_eq!(cache.get_log_cache_entries(arg).await.unwrap().len(), 1);
	}
}
