use {crate::Cache, tangram_cache::log, tangram_client::prelude::*};

impl Cache {
	pub async fn delete_log_cache_entry(&self, arg: log::cache::delete::Arg) -> tg::Result<()> {
		let entry = arg.entry;
		let arg = log::delete::Arg {
			process: entry.process.clone(),
		};
		self.delete_log_inner(arg).await?;
		let partition = crate::physical_partition(entry.partition, self.partition_offset)?;
		let process = entry.process.to_bytes();
		let params = (partition, entry.expires_at, process.as_ref());
		self.session
			.execute_unpaged(&self.statements.log.delete_cache_entry, params)
			.await
			.map_err(|error| tg::error!(!error, "failed to delete a log cache entry"))?;
		Ok(())
	}

	pub async fn get_log_cache_entries(
		&self,
		arg: log::cache::get::Arg,
	) -> tg::Result<Vec<log::cache::Entry>> {
		if arg.batch_size == 0 {
			return Ok(Vec::new());
		}
		let partition = crate::physical_partition(arg.partition, self.partition_offset)?;
		let limit = i32::try_from(arg.batch_size)
			.map_err(|error| tg::error!(!error, "the log cache batch size exceeded an i32"))?;
		let params = (partition, arg.now, limit);
		let result = self
			.session
			.execute_unpaged(&self.statements.log.get_cache_entries, params)
			.await
			.map_err(|error| tg::error!(!error, "failed to get log cache entries"))?
			.into_rows_result()
			.map_err(|error| tg::error!(!error, "failed to get log cache rows"))?;
		#[derive(scylla::DeserializeRow)]
		struct Row<'a> {
			expires_at: i64,
			process: &'a [u8],
		}
		let output = result
			.rows::<Row>()
			.map_err(|error| tg::error!(!error, "failed to iterate log cache rows"))?
			.map(|row| {
				let row =
					row.map_err(|error| tg::error!(!error, "failed to read a log cache row"))?;
				let process = tg::process::Id::from_slice(row.process)?;
				Ok(log::cache::Entry {
					expires_at: row.expires_at,
					partition: arg.partition,
					process,
				})
			})
			.collect::<tg::Result<_>>()?;
		Ok(output)
	}

	pub async fn put_log_cache_entry(&self, arg: log::cache::put::Arg) -> tg::Result<()> {
		let entry = arg.entry;
		let partition = crate::physical_partition(entry.partition, self.partition_offset)?;
		let process = entry.process.to_bytes();
		let params = (partition, entry.expires_at, process.as_ref());
		self.session
			.execute_unpaged(&self.statements.log.put_cache_entry, params)
			.await
			.map_err(|error| tg::error!(!error, "failed to put a log cache entry"))?;
		Ok(())
	}
}
