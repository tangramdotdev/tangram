use {super::Store, crate::object, num::ToPrimitive as _, tangram_client::prelude::*};

impl Store {
	pub async fn delete_object_cache_entry(
		&self,
		arg: object::cache::delete::Arg,
	) -> tg::Result<()> {
		let entry = arg.entry;
		let id = entry.id.to_bytes();
		let params = (id.as_ref(), entry.put.as_slice());
		self.session
			.execute_unpaged(&self.statements.delete_object, params)
			.await
			.map_err(|error| tg::error!(!error, id = %entry.id, "failed to delete the object"))?;

		let partition = super::physical_partition(entry.partition, self.partition_offset)?;
		let params = (partition, entry.cache.as_slice());
		self.session
			.execute_unpaged(&self.statements.delete_object_cache_entry, params)
			.await
			.map_err(|error| tg::error!(!error, "failed to delete an object cache entry"))?;

		Ok(())
	}

	pub async fn get_object_cache_entries(
		&self,
		arg: object::cache::get::Arg,
	) -> tg::Result<Vec<object::cache::Entry>> {
		if arg.batch_size == 0 {
			return Ok(Vec::new());
		}
		let partition = super::physical_partition(arg.partition, self.partition_offset)?;
		let limit = arg
			.batch_size
			.to_i32()
			.ok_or_else(|| tg::error!("the object cache batch size exceeded an i32"))?;
		let params = (partition, limit);
		let result = self
			.session
			.execute_unpaged(&self.statements.get_object_cache_entries, params)
			.await
			.map_err(|error| tg::error!(!error, "failed to get object cache entries"))?
			.into_rows_result()
			.map_err(|error| tg::error!(!error, "failed to get object cache rows"))?;

		#[derive(scylla::DeserializeRow)]
		struct Row<'a> {
			cache: &'a [u8],
			id: &'a [u8],
			partition: i64,
			put: &'a [u8],
		}

		result
			.rows::<Row>()
			.map_err(|error| tg::error!(!error, "failed to iterate object cache rows"))?
			.map(|result| {
				let row = result
					.map_err(|error| tg::error!(!error, "failed to get an object cache row"))?;
				let cache = row
					.cache
					.try_into()
					.map_err(|_| tg::error!("invalid object cache id"))?;
				let id = tg::object::Id::from_slice(row.id)?;
				let partition = super::logical_partition(row.partition, self.partition_offset)?;
				let put = row
					.put
					.try_into()
					.map_err(|_| tg::error!("invalid object cache put"))?;
				let entry = object::cache::Entry {
					cache,
					id,
					partition,
					put,
				};

				Ok(entry)
			})
			.collect()
	}

	pub async fn put_object_cache_entry(&self, arg: object::cache::put::Arg) -> tg::Result<()> {
		self.put_object_cache_entry_inner(arg.cache, arg.id, arg.partition, arg.put)
			.await
	}

	pub async fn put_object_cache_entry_with_object(
		&self,
		arg: object::cache::put::object::Arg,
	) -> tg::Result<()> {
		let object = arg.object;
		let id = object.id.clone();
		let put = object.put;
		self.put_object(object).await?;
		let result = self
			.put_object_cache_entry_inner(arg.cache, id.clone(), arg.partition, put)
			.await;
		if let Err(error) = result {
			let arg = object::delete::Arg {
				id: id.clone(),
				put,
			};
			if let Err(cleanup_error) = self.delete_object(arg).await {
				return Err(tg::error!(
					!error,
					cleanup_error = %cleanup_error.trace(),
					%id,
					"failed to put an object cache entry and delete the untracked object"
				));
			}

			return Err(error);
		}

		Ok(())
	}

	async fn put_object_cache_entry_inner(
		&self,
		cache: [u8; 16],
		id: tg::object::Id,
		partition: u64,
		put: [u8; 16],
	) -> tg::Result<()> {
		let partition = super::physical_partition(partition, self.partition_offset)?;
		let id = id.to_bytes();
		let params = (cache.as_slice(), id.as_ref(), partition, put.as_slice());
		self.session
			.execute_unpaged(&self.statements.put_object_cache_entry, params)
			.await
			.map_err(|error| tg::error!(!error, "failed to put an object cache entry"))?;

		Ok(())
	}
}
