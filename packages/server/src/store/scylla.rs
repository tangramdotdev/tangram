use {
	super::CacheReference,
	crate::store::{DeleteArg, PutArg},
	bytes::Bytes,
	futures::FutureExt as _,
	indoc::indoc,
	num::ToPrimitive as _,
	std::collections::HashMap,
	tangram_client as tg,
};

pub struct Scylla {
	delete_statement: scylla::statement::prepared::PreparedStatement,
	get_batch_statement: scylla::statement::prepared::PreparedStatement,
	get_cache_reference_statement: scylla::statement::prepared::PreparedStatement,
	get_statement: scylla::statement::prepared::PreparedStatement,
	put_statement: scylla::statement::prepared::PreparedStatement,
	session: scylla::client::session::Session,
}

impl Scylla {
	pub async fn new(config: &crate::config::ScyllaStore) -> tg::Result<Self> {
		let mut builder =
			scylla::client::session_builder::SessionBuilder::new().known_node(&config.addr);
		if let (Some(username), Some(password)) = (&config.username, &config.password) {
			builder = builder.user(username, password);
		}
		let session = builder
			.build()
			.boxed()
			.await
			.map_err(|source| tg::error!(!source, "failed to connect"))?;
		session
			.use_keyspace(&config.keyspace, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to use the keyspace"))?;

		let statement = indoc!(
			"
				select bytes
				from objects
				where id = ?;
			"
		);
		let mut get_statement = session
			.prepare(statement)
			.await
			.map_err(|source| tg::error!(!source, "failed to prepare the get statement"))?;
		get_statement.set_consistency(scylla::statement::Consistency::One);

		let statement = indoc!(
			"
				select id, bytes
				from objects
				where id in ?;
			"
		);
		let mut get_batch_statement = session
			.prepare(statement)
			.await
			.map_err(|source| tg::error!(!source, "failed to prepare the get batch statement"))?;
		get_batch_statement.set_consistency(scylla::statement::Consistency::One);

		let statement = indoc!(
			"
				select cache_reference
				from objects
				where id = ?;
			"
		);
		let mut get_cache_reference_statement =
			session.prepare(statement).await.map_err(|source| {
				tg::error!(
					!source,
					"failed to prepare the get cache reference statement"
				)
			})?;
		get_cache_reference_statement.set_consistency(scylla::statement::Consistency::One);

		let statement = indoc!(
			"
				insert into objects (id, bytes, cache_reference, touched_at)
				values (?, ?, ?, ?);
			"
		);
		let mut put_statement = session
			.prepare(statement)
			.await
			.map_err(|source| tg::error!(!source, "failed to prepare put statement"))?;
		put_statement.set_consistency(scylla::statement::Consistency::LocalQuorum);

		let statement = indoc!(
			"
				delete from objects
				where id = ? if touched_at < ?;
			"
		);
		let mut delete_statement = session
			.prepare(statement)
			.await
			.map_err(|source| tg::error!(!source, "failed to prepare delete statement"))?;
		delete_statement.set_consistency(scylla::statement::Consistency::LocalQuorum);

		let scylla = Self {
			delete_statement,
			get_batch_statement,
			get_cache_reference_statement,
			get_statement,
			put_statement,
			session,
		};

		Ok(scylla)
	}

	pub async fn try_get(&self, id: &tg::object::Id) -> tg::Result<Option<Bytes>> {
		// Attempt to get the object with the default consistency.
		let bytes = self.try_get_inner(id, &self.get_statement).await?;
		if let Some(bytes) = bytes {
			return Ok(Some(bytes));
		}

		// Attempt to get the object with local quorum consistency.
		let mut statement = self.get_statement.clone();
		statement.set_consistency(scylla::statement::Consistency::LocalQuorum);
		let bytes = self.try_get_inner(id, &statement).await?;
		if let Some(bytes) = bytes {
			return Ok(Some(bytes));
		}

		Ok(None)
	}

	async fn try_get_inner(
		&self,
		id: &tg::object::Id,
		statement: &scylla::statement::prepared::PreparedStatement,
	) -> tg::Result<Option<Bytes>> {
		let params = (id.to_bytes().to_vec(),);
		#[derive(scylla::DeserializeRow)]
		struct Row<'a> {
			bytes: &'a [u8],
		}
		let result = self
			.session
			.execute_unpaged(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the get statement"))?
			.into_rows_result()
			.map_err(|source| tg::error!(!source, "failed to get the rows"))?;
		let Some(row) = result
			.maybe_first_row::<Row>()
			.map_err(|source| tg::error!(!source, "expected a single row"))?
		else {
			return Ok(None);
		};
		let bytes = Bytes::copy_from_slice(row.bytes);
		Ok(Some(bytes))
	}

	pub async fn try_get_batch(&self, ids: &[tg::object::Id]) -> tg::Result<Vec<Option<Bytes>>> {
		// Attempt to get the objects with the default consistency.
		let mut map = self.get_batch_inner(ids, &self.get_batch_statement).await?;

		// Attempt to get missing objects with local quorum consistency.
		let missing = ids
			.iter()
			.filter(|id| !map.contains_key(id))
			.collect::<Vec<_>>();
		if !missing.is_empty() {
			let mut statement = self.get_batch_statement.clone();
			statement.set_consistency(scylla::statement::Consistency::LocalQuorum);
			let missing_map = self.get_batch_inner(ids, &statement).await?;
			map.extend(missing_map);
		}

		// Create the output.
		let output = ids.iter().map(|id| map.get(id).cloned()).collect();

		Ok(output)
	}

	async fn get_batch_inner(
		&self,
		ids: &[tg::object::Id],
		statement: &scylla::statement::prepared::PreparedStatement,
	) -> tg::Result<HashMap<tg::object::Id, Bytes, tg::id::BuildHasher>> {
		let ids = ids
			.iter()
			.map(|id| id.to_bytes().to_vec())
			.collect::<Vec<_>>();
		let params = (ids,);
		#[derive(scylla::DeserializeRow)]
		struct Row<'a> {
			id: &'a [u8],
			bytes: &'a [u8],
		}
		let result = self
			.session
			.execute_unpaged(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the batch statement"))?
			.into_rows_result()
			.map_err(|source| tg::error!(!source, "failed to get the rows"))?;
		let map = result
			.rows::<Row>()
			.map_err(|source| tg::error!(!source, "failed to get the rows"))?
			.map(|result| {
				result
					.map_err(|source| tg::error!(!source, "failed to deserialize the row"))
					.and_then(|row| {
						let id = tg::object::Id::from_slice(row.id)?;
						let bytes = Bytes::copy_from_slice(row.bytes);
						Ok((id, bytes))
					})
			})
			.collect::<tg::Result<_>>()?;
		Ok(map)
	}

	pub async fn try_get_cache_reference(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<CacheReference>> {
		// Attempt to get the cache reference with the default consistency.
		let cache_reference = self
			.try_get_cache_reference_inner(id, &self.get_cache_reference_statement)
			.await?;
		if let Some(cache_reference) = cache_reference {
			return Ok(Some(cache_reference));
		}

		// Attempt to get the cache reference with local quorum consistency.
		let mut statement = self.get_cache_reference_statement.clone();
		statement.set_consistency(scylla::statement::Consistency::LocalQuorum);
		let cache_reference = self.try_get_cache_reference_inner(id, &statement).await?;
		if let Some(cache_reference) = cache_reference {
			return Ok(Some(cache_reference));
		}

		Ok(None)
	}

	async fn try_get_cache_reference_inner(
		&self,
		id: &tg::object::Id,
		statement: &scylla::statement::prepared::PreparedStatement,
	) -> tg::Result<Option<CacheReference>> {
		let params = (id.to_bytes().to_vec(),);
		#[derive(scylla::DeserializeRow)]
		struct Row<'a> {
			cache_reference: &'a [u8],
		}
		let result = self
			.session
			.execute_unpaged(statement, params)
			.await
			.map_err(|source| {
				tg::error!(
					!source,
					"failed to execute the get cache reference statement"
				)
			})?
			.into_rows_result()
			.map_err(|source| tg::error!(!source, "failed to get the rows"))?;
		let Some(row) = result
			.maybe_first_row::<Row>()
			.map_err(|source| tg::error!(!source, "expected a single row"))?
		else {
			return Ok(None);
		};
		let cache_reference = CacheReference::deserialize(row.cache_reference)
			.map_err(|source| tg::error!(!source, "failed to deserialize the cache reference"))?;
		Ok(Some(cache_reference))
	}

	pub async fn put(&self, arg: super::PutArg) -> tg::Result<()> {
		let id = arg.id.to_bytes().to_vec();
		let bytes = arg.bytes;
		let cache_reference = if let Some(cache_reference) = &arg.cache_reference {
			let cache_reference = cache_reference
				.serialize()
				.map_err(|source| tg::error!(!source, "failed to serialize the cache reference"))?;
			Some(cache_reference)
		} else {
			None
		};
		let touched_at = arg.touched_at;
		let params = (id, bytes, cache_reference, touched_at);
		self.session
			.execute_unpaged(&self.put_statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the put statement"))?;
		Ok(())
	}

	pub async fn put_batch(&self, args: Vec<PutArg>) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let mut batch =
			scylla::statement::batch::Batch::new(scylla::statement::batch::BatchType::Unlogged);
		batch.set_consistency(self.put_statement.get_consistency().unwrap());
		for _ in &args {
			batch.append_statement(scylla::statement::batch::BatchStatement::PreparedStatement(
				self.put_statement.clone(),
			));
		}
		let params = args
			.iter()
			.map(|arg| {
				let id = arg.id.to_bytes().to_vec();
				let bytes = arg.bytes.clone();
				let cache_reference = if let Some(cache_reference) = &arg.cache_reference {
					let cache_reference = cache_reference.serialize().map_err(|source| {
						tg::error!(!source, "failed to serialize the cache reference")
					})?;
					Some(cache_reference)
				} else {
					None
				};
				let touched_at = arg.touched_at;
				let params = (id, bytes, cache_reference, touched_at);
				Ok(params)
			})
			.collect::<tg::Result<Vec<_>>>()?;
		self.session
			.batch(&batch, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the batch put"))?;
		Ok(())
	}

	pub async fn delete(&self, arg: DeleteArg) -> tg::Result<()> {
		let id = arg.id.to_bytes().to_vec();
		let max_touched_at = arg.now - arg.ttl.to_i64().unwrap();
		let params = (id, max_touched_at);
		self.session
			.execute_unpaged(&self.delete_statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the delete statement"))?;
		Ok(())
	}

	pub async fn delete_batch(&self, args: Vec<DeleteArg>) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let mut batch =
			scylla::statement::batch::Batch::new(scylla::statement::batch::BatchType::Unlogged);
		batch.set_consistency(self.delete_statement.get_consistency().unwrap());
		for _ in &args {
			batch.append_statement(scylla::statement::batch::BatchStatement::PreparedStatement(
				self.delete_statement.clone(),
			));
		}
		let params = args
			.iter()
			.map(|arg| {
				let id = arg.id.to_bytes().to_vec();
				let max_touched_at = arg.now - arg.ttl.to_i64().unwrap();
				(id, max_touched_at)
			})
			.collect::<Vec<_>>();
		self.session
			.batch(&batch, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the batch delete"))?;
		Ok(())
	}

	pub async fn sync(&self) -> tg::Result<()> {
		Ok(())
	}
}
