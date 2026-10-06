use {crate::Cache, tangram_cache::log, tangram_client::prelude::*};

impl Cache {
	pub async fn delete_log_cache_entry(&self, arg: log::cache::delete::Arg) -> tg::Result<()> {
		let mut state = self.state();
		let entry = arg.entry;
		state.logs.remove(&entry.process);
		state
			.log_cache
			.remove(&(entry.partition, entry.expires_at, entry.process));
		Ok(())
	}

	pub async fn get_log_cache_entries(
		&self,
		arg: log::cache::get::Arg,
	) -> tg::Result<Vec<log::cache::Entry>> {
		let state = self.state();
		let output = state
			.log_cache
			.iter()
			.filter(|(partition, expires_at, _)| {
				*partition == arg.partition && *expires_at <= arg.now
			})
			.take(arg.batch_size)
			.map(|(partition, expires_at, process)| log::cache::Entry {
				expires_at: *expires_at,
				partition: *partition,
				process: process.clone(),
			})
			.collect();

		Ok(output)
	}

	pub async fn put_log_cache_entry(&self, arg: log::cache::put::Arg) -> tg::Result<()> {
		let entry = arg.entry;
		self.state()
			.log_cache
			.insert((entry.partition, entry.expires_at, entry.process));
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use {super::*, bytes::Bytes, std::collections::BTreeSet};

	#[tokio::test]
	async fn expiration_deletes_the_complete_log_and_preserves_other_entries() {
		let cache = Cache::new();
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
		cache.put_log(arg);
		let arg = log::end::Arg {
			end: tg::process::log::End {
				position: 3,
				stderr_position: 0,
				stdout_position: 3,
			},
			process: process.clone(),
		};
		cache.put_log_end(arg);

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
		assert!(cache.try_get_log_end(&process).is_none());

		let arg = log::read::Arg {
			length: 10,
			position: 0,
			process,
			streams: BTreeSet::from([stream]),
		};
		assert!(cache.try_read_log(arg).is_empty());

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
