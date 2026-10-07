use {
	crate::Cache,
	bytes::Bytes,
	futures::{FutureExt as _, StreamExt as _, TryStreamExt as _, stream},
	num::ToPrimitive as _,
	std::borrow::Cow,
	tangram_client::prelude::*,
};

impl Cache {
	pub(super) async fn contains_object(
		&self,
		arg: tangram_cache::object::contains::Arg,
	) -> tg::Result<bool> {
		let contains = self
			.contains_object_inner(&arg, &self.statements.contains_object)
			.await?;
		if contains {
			return Ok(true);
		}

		let mut statement = self.statements.contains_object.clone();
		statement.set_consistency(scylla::statement::Consistency::LocalQuorum);

		self.contains_object_inner(&arg, &statement).await
	}

	async fn contains_object_inner(
		&self,
		arg: &tangram_cache::object::contains::Arg,
		statement: &scylla::statement::prepared::PreparedStatement,
	) -> tg::Result<bool> {
		let id = &arg.id;
		let id_bytes = id.to_bytes();
		let params = (id_bytes.as_ref(), arg.put.as_slice());
		let result = self
			.session
			.execute_unpaged(statement, params)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to execute the query"))?
			.into_rows_result()
			.map_err(|error| tg::error!(!error, %id, "failed to get the rows"))?;
		let contains = result
			.maybe_first_row::<(Vec<u8>,)>()
			.map_err(|error| tg::error!(!error, %id, "failed to get the row"))?
			.is_some();

		Ok(contains)
	}

	pub async fn delete_object_cache_entry(
		&self,
		arg: tangram_cache::object::cache::delete::Arg,
	) -> tg::Result<()> {
		let entry = arg.entry;
		let id = entry.id.to_bytes();
		let params = (id.as_ref(), entry.put.as_slice());
		self.session
			.execute_unpaged(&self.statements.delete_object, params)
			.await
			.map_err(|error| tg::error!(!error, id = %entry.id, "failed to delete the object"))?;

		let partition = crate::physical_partition(entry.partition, self.partition_offset)?;
		let params = (partition, entry.cache.as_slice());
		self.session
			.execute_unpaged(&self.statements.delete_object_cache_entry, params)
			.await
			.map_err(|error| tg::error!(!error, "failed to delete an object cache entry"))?;

		Ok(())
	}

	pub(super) async fn delete_object(
		&self,
		arg: tangram_cache::object::delete::Arg,
	) -> tg::Result<()> {
		let id = &arg.id;
		let id_bytes = id.to_bytes().to_vec();
		let params = (id_bytes, arg.put.as_slice());
		self.session
			.execute_unpaged(&self.statements.delete_object, params)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to execute the query"))?;
		Ok(())
	}

	pub(super) async fn delete_object_batch(
		&self,
		args: Vec<tangram_cache::object::delete::Arg>,
	) -> tg::Result<()> {
		stream::iter(args.into_iter().map(Ok))
			.try_for_each_concurrent(crate::OBJECT_CONCURRENCY, |arg| self.delete_object(arg))
			.await?;

		Ok(())
	}

	pub async fn get_object_cache_entries(
		&self,
		arg: tangram_cache::object::cache::get::Arg,
	) -> tg::Result<Vec<tangram_cache::object::cache::Entry>> {
		if arg.batch_size == 0 {
			return Ok(Vec::new());
		}
		let partition = crate::physical_partition(arg.partition, self.partition_offset)?;
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
			object: &'a [u8],
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
				let id = tg::object::Id::from_slice(row.object)?;
				let partition = crate::logical_partition(row.partition, self.partition_offset)?;
				let put = row
					.put
					.try_into()
					.map_err(|_| tg::error!("invalid object cache put"))?;
				let entry = tangram_cache::object::cache::Entry {
					cache,
					id,
					partition,
					put,
				};

				Ok(entry)
			})
			.collect()
	}

	pub async fn put_object_cache_entry(
		&self,
		arg: tangram_cache::object::cache::put::Arg,
	) -> tg::Result<()> {
		self.put_object_cache_entry_inner(arg.cache, arg.id, arg.partition, arg.put)
			.await
	}

	async fn put_object_cache_entry_inner(
		&self,
		cache: [u8; 16],
		id: tg::object::Id,
		partition: u64,
		put: [u8; 16],
	) -> tg::Result<()> {
		let partition = crate::physical_partition(partition, self.partition_offset)?;
		let id = id.to_bytes();
		let params = (cache.as_slice(), id.as_ref(), partition, put.as_slice());
		self.session
			.execute_unpaged(&self.statements.put_object_cache_entry, params)
			.await
			.map_err(|error| tg::error!(!error, "failed to put an object cache entry"))?;

		Ok(())
	}

	pub async fn put_object_cache_entry_with_object(
		&self,
		arg: tangram_cache::object::cache::put::object::Arg,
	) -> tg::Result<()> {
		let object = arg.object;
		let id = object.id.clone();
		let put = object.put;
		self.put_object(object).await?;
		let result = self
			.put_object_cache_entry_inner(arg.cache, id.clone(), arg.partition, put)
			.await;
		if let Err(error) = result {
			let arg = tangram_cache::object::delete::Arg {
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

	pub(super) async fn put_object(&self, arg: tangram_cache::object::put::Arg) -> tg::Result<()> {
		let id = &arg.id;
		if arg.checkout_pointer.is_some() {
			return Err(tg::error!(
				%id,
				"checkout pointers are not supported by the scylla cache"
			));
		}
		let bytes = arg.bytes;
		let id_bytes = id.to_bytes().to_vec();
		let params = (bytes, id_bytes, arg.put.as_slice());
		self.session
			.execute_unpaged(&self.statements.put_object, params)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to execute the query"))?;
		Ok(())
	}

	pub(super) async fn put_object_batch(
		&self,
		args: Vec<tangram_cache::object::put::Arg>,
	) -> tg::Result<()> {
		if let Some(arg) = args.iter().find(|arg| arg.checkout_pointer.is_some()) {
			return Err(tg::error!(
				id = %arg.id,
				"checkout pointers are not supported by the scylla cache"
			));
		}
		stream::iter(args.into_iter().map(Ok))
			.try_for_each_concurrent(crate::OBJECT_CONCURRENCY, |arg| self.put_object(arg))
			.await?;

		Ok(())
	}

	pub(super) async fn try_get_object(
		&self,
		arg: tangram_cache::object::get::Arg,
	) -> tg::Result<tangram_cache::object::get::Output> {
		let statement = match (arg.bytes, arg.put.is_some()) {
			(false, false) => &self.statements.get_object_info,
			(false, true) => &self.statements.get_object_info_for_put,
			(true, false) => &self.statements.get_object,
			(true, true) => &self.statements.get_object_for_put,
		};
		let object = self.try_get_object_inner(&arg, statement).await?;
		if object.is_some() {
			return Ok(tangram_cache::object::get::Output { object });
		}

		let mut object_statement = statement.clone();
		object_statement.set_consistency(scylla::statement::Consistency::LocalQuorum);
		let object = self.try_get_object_inner(&arg, &object_statement).await?;
		Ok(tangram_cache::object::get::Output { object })
	}

	async fn try_get_object_inner(
		&self,
		arg: &tangram_cache::object::get::Arg,
		statement: &scylla::statement::prepared::PreparedStatement,
	) -> tg::Result<Option<tangram_cache::object::Object<'static>>> {
		let id = &arg.id;
		let id_bytes = id.to_bytes();
		#[derive(scylla::DeserializeRow)]
		struct Row<'a> {
			bytes: Option<&'a [u8]>,
			put: &'a [u8],
		}
		let result = if let Some(put) = arg.put {
			let params = (id_bytes.as_ref(), put.as_slice());
			self.session
				.execute_unpaged(statement, params)
				.boxed()
				.await
		} else {
			let params = (id_bytes.as_ref(),);
			self.session
				.execute_unpaged(statement, params)
				.boxed()
				.await
		}
		.map_err(|error| tg::error!(!error, %id, "failed to execute the query"))?
		.into_rows_result()
		.map_err(|error| tg::error!(!error, %id, "failed to get the rows"))?;
		if !arg.bytes {
			let row = result
				.maybe_first_row::<(Option<i64>, Vec<u8>)>()
				.map_err(|error| tg::error!(!error, %id, "failed to get the object info row"))?;
			let Some((Some(_), put)) = row else {
				return Ok(None);
			};
			let put = put
				.try_into()
				.map_err(|_| tg::error!(%id, "invalid object put"))?;
			let object = tangram_cache::object::Object {
				bytes: None,
				checkout_pointer: None,
				length: None,
				put,
			};
			return Ok(Some(object));
		}
		let Some(row) = result
			.maybe_first_row::<Row>()
			.map_err(|error| tg::error!(!error, %id, "failed to get the row"))?
		else {
			return Ok(None);
		};
		let Some(bytes) = row.bytes else {
			return Ok(None);
		};
		let bytes = Cow::Owned(Bytes::copy_from_slice(bytes).to_vec());
		let put = row
			.put
			.try_into()
			.map_err(|_| tg::error!(%id, "invalid object put"))?;
		Ok(Some(tangram_cache::object::Object {
			bytes: Some(bytes),
			checkout_pointer: None,
			length: None,
			put,
		}))
	}

	pub(super) async fn try_get_object_batch(
		&self,
		arg: tangram_cache::object::get::batch::Arg,
	) -> tg::Result<Vec<tangram_cache::object::get::Output>> {
		let mut output = stream::iter(arg.ids.into_iter().enumerate())
			.map(|(index, id)| async move {
				let arg = tangram_cache::object::get::Arg {
					bytes: arg.bytes,
					id,
					put: None,
				};
				let output = self.try_get_object(arg).await?;

				Ok::<_, tg::Error>((index, output))
			})
			.buffer_unordered(crate::OBJECT_CONCURRENCY)
			.try_collect::<Vec<_>>()
			.await?;
		output.sort_unstable_by_key(|(index, _)| *index);
		let output = output.into_iter().map(|(_, output)| output).collect();

		Ok(output)
	}
}

impl tangram_cache::object::Cache for Cache {
	async fn contains_object(&self, arg: tangram_cache::object::contains::Arg) -> tg::Result<bool> {
		self.contains_object(arg).await
	}

	async fn delete_object_cache_entry(
		&self,
		arg: tangram_cache::object::cache::delete::Arg,
	) -> tg::Result<()> {
		self.delete_object_cache_entry(arg).await
	}

	async fn delete_object(&self, arg: tangram_cache::object::delete::Arg) -> tg::Result<()> {
		self.delete_object(arg).await
	}

	async fn delete_object_batch(
		&self,
		args: Vec<tangram_cache::object::delete::Arg>,
	) -> tg::Result<()> {
		self.delete_object_batch(args).await
	}

	async fn get_object_cache_entries(
		&self,
		arg: tangram_cache::object::cache::get::Arg,
	) -> tg::Result<Vec<tangram_cache::object::cache::Entry>> {
		self.get_object_cache_entries(arg).await
	}

	async fn put_object_cache_entry(
		&self,
		arg: tangram_cache::object::cache::put::Arg,
	) -> tg::Result<()> {
		self.put_object_cache_entry(arg).await
	}

	async fn put_object_cache_entry_with_object(
		&self,
		arg: tangram_cache::object::cache::put::object::Arg,
	) -> tg::Result<()> {
		self.put_object_cache_entry_with_object(arg).await
	}

	async fn put_object(&self, arg: tangram_cache::object::put::Arg) -> tg::Result<()> {
		self.put_object(arg).await
	}

	async fn put_object_batch(&self, args: Vec<tangram_cache::object::put::Arg>) -> tg::Result<()> {
		self.put_object_batch(args).await
	}

	async fn try_get_object(
		&self,
		arg: tangram_cache::object::get::Arg,
	) -> tg::Result<tangram_cache::object::get::Output> {
		self.try_get_object(arg).await
	}

	async fn try_get_object_batch(
		&self,
		arg: tangram_cache::object::get::batch::Arg,
	) -> tg::Result<Vec<tangram_cache::object::get::Output>> {
		self.try_get_object_batch(arg).await
	}
}
