use {super::Store, crate::object, num::ToPrimitive as _, tangram_client::prelude::*};

impl Store {
	pub async fn delete_object_cache_entry(
		&self,
		arg: object::cache::delete::Arg,
	) -> tg::Result<()> {
		let entry = arg.entry;
		let id = entry.id.to_bytes();
		let params = (entry.cached_at, id.as_ref());
		self.session
			.execute_unpaged(&self.statements.delete_object, params)
			.await
			.map_err(|error| tg::error!(!error, id = %entry.id, "failed to delete the object"))?;

		let partition = super::physical_partition(entry.partition, self.partition_offset)?;
		let params = (entry.cached_at, partition, entry.cached_at, id.as_ref());
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
			cached_at: i64,
			id: &'a [u8],
			partition: i64,
		}

		result
			.rows::<Row>()
			.map_err(|error| tg::error!(!error, "failed to iterate object cache rows"))?
			.map(|result| {
				let row = result
					.map_err(|error| tg::error!(!error, "failed to get an object cache row"))?;
				let id = tg::object::Id::from_slice(row.id)?;
				let partition = super::logical_partition(row.partition, self.partition_offset)?;
				let entry = object::cache::Entry {
					cached_at: row.cached_at,
					id,
					partition,
				};

				Ok(entry)
			})
			.collect()
	}

	pub async fn put_object_cache_entry(&self, arg: object::cache::put::Arg) -> tg::Result<()> {
		let cached_at = object::cache::stored_at_timestamp(arg.stored_at)?;
		self.put_object_cache_entry_inner(arg.id, arg.partition, cached_at)
			.await
	}

	pub async fn put_object_cache_entry_with_object(
		&self,
		arg: object::cache::put::object::Arg,
	) -> tg::Result<()> {
		let cached_at = object::cache::cached_at_timestamp(arg.cached_at)?;
		self.put_object_cache_entry_inner(arg.object.id.clone(), arg.partition, cached_at)
			.await?;
		self.put_object_with_timestamp(arg.object, cached_at)
			.await?;

		Ok(())
	}

	async fn put_object_with_timestamp(
		&self,
		arg: object::put::Arg,
		timestamp: i64,
	) -> tg::Result<()> {
		let id = &arg.id;
		if arg.checkout_pointer.is_some() {
			return Err(tg::error!(
				%id,
				"checkout pointers are not supported by the scylla store"
			));
		}
		let bytes = arg.bytes;
		let id_bytes = id.to_bytes().to_vec();
		let stored_at = arg.stored_at;
		let params = (bytes, id_bytes, stored_at, timestamp);
		self.session
			.execute_unpaged(&self.statements.put_object, params)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to execute the query"))?;

		Ok(())
	}

	async fn put_object_cache_entry_inner(
		&self,
		id: tg::object::Id,
		partition: u64,
		cached_at: i64,
	) -> tg::Result<()> {
		let partition = super::physical_partition(partition, self.partition_offset)?;
		let id = id.to_bytes();
		let params = (cached_at, id.as_ref(), partition, cached_at);
		self.session
			.execute_unpaged(&self.statements.put_object_cache_entry, params)
			.await
			.map_err(|error| tg::error!(!error, "failed to put an object cache entry"))?;

		Ok(())
	}
}
