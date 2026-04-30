use {
	crate::{CachePointer, DeleteArg, PutArg},
	bytes::Bytes,
	futures::FutureExt as _,
	indoc::indoc,
	num::ToPrimitive as _,
	std::{borrow::Cow, collections::HashMap},
	tangram_client::prelude::*,
};

#[derive(Clone, Debug)]
pub struct Config {
	pub addr: String,
	pub connections: Option<usize>,
	pub keyspace: String,
	pub password: Option<String>,
	pub speculative_execution: Option<SpeculativeExecution>,
	pub username: Option<String>,
}

#[derive(Clone, Debug)]
pub enum SpeculativeExecution {
	Percentile {
		max_retry_count: usize,
		percentile: f64,
	},
	Simple {
		max_retry_count: usize,
		retry_interval: std::time::Duration,
	},
}

pub struct Store {
	statements: Statements,
	session: scylla::client::session::Session,
}

struct Statements {
	delete_object: scylla::statement::prepared::PreparedStatement,
	get_object_batch: scylla::statement::prepared::PreparedStatement,
	get_object_cache_pointer: scylla::statement::prepared::PreparedStatement,
	get_object: scylla::statement::prepared::PreparedStatement,
	put_object: scylla::statement::prepared::PreparedStatement,
}

impl Store {
	pub async fn new(config: &Config) -> tg::Result<Self> {
		let mut builder =
			scylla::client::session_builder::SessionBuilder::new().known_node(&config.addr);
		if let (Some(username), Some(password)) = (&config.username, &config.password) {
			builder = builder.user(username, password);
		}
		if let Some(speculative_execution) = &config.speculative_execution {
			let policy: std::sync::Arc<
				dyn scylla::policies::speculative_execution::SpeculativeExecutionPolicy,
			> = match speculative_execution {
				SpeculativeExecution::Percentile {
					max_retry_count,
					percentile,
				} => std::sync::Arc::new(
					scylla::policies::speculative_execution::PercentileSpeculativeExecutionPolicy {
						max_retry_count: *max_retry_count,
						percentile: *percentile,
					},
				),
				SpeculativeExecution::Simple {
					max_retry_count,
					retry_interval,
				} => std::sync::Arc::new(
					scylla::policies::speculative_execution::SimpleSpeculativeExecutionPolicy {
						max_retry_count: *max_retry_count,
						retry_interval: *retry_interval,
					},
				),
			};
			let handle = scylla::client::execution_profile::ExecutionProfile::builder()
				.speculative_execution_policy(Some(policy))
				.build()
				.into_handle();
			builder = builder.default_execution_profile_handle(handle);
		}
		if let Some(connections) = config.connections.and_then(std::num::NonZeroUsize::new) {
			builder = builder.pool_size(scylla::client::PoolSize::PerHost(connections));
		}
		let session = builder.build().boxed().await.map_err(
			|source| tg::error!(!source, addr = %config.addr, "failed to build the session"),
		)?;
		session.use_keyspace(&config.keyspace, true).await.map_err(
			|source| tg::error!(!source, keyspace = %config.keyspace, "failed to use the keyspace"),
		)?;

		let statement = indoc!(
			"
				delete from objects
				where id = ? if touched_at < ?;
			"
		);
		let mut delete_object = session
			.prepare(statement)
			.await
			.map_err(|source| tg::error!(!source, "failed to prepare the delete statement"))?;
		delete_object.set_consistency(scylla::statement::Consistency::LocalQuorum);

		let statement = indoc!(
			"
				select id, bytes
				from objects
				where id in ?;
			"
		);
		let mut get_object_batch = session
			.prepare(statement)
			.await
			.map_err(|source| tg::error!(!source, "failed to prepare the get batch statement"))?;
		get_object_batch.set_consistency(scylla::statement::Consistency::One);

		let statement = indoc!(
			"
				select cache_pointer
				from objects
				where id = ?;
			"
		);
		let mut get_object_cache_pointer = session.prepare(statement).await.map_err(|source| {
			tg::error!(!source, "failed to prepare the get cache pointer statement")
		})?;
		get_object_cache_pointer.set_consistency(scylla::statement::Consistency::One);

		let statement = indoc!(
			"
				select bytes
				from objects
				where id = ?;
			"
		);
		let mut get_object = session
			.prepare(statement)
			.await
			.map_err(|source| tg::error!(!source, "failed to prepare the get statement"))?;
		get_object.set_consistency(scylla::statement::Consistency::One);

		let statement = indoc!(
			"
				insert into objects (id, bytes, cache_pointer, touched_at)
				values (?, ?, ?, ?);
			"
		);
		let mut put_object = session
			.prepare(statement)
			.await
			.map_err(|source| tg::error!(!source, "failed to prepare the put statement"))?;
		put_object.set_consistency(scylla::statement::Consistency::LocalQuorum);

		let scylla = Self {
			statements: Statements {
				delete_object,
				get_object_batch,
				get_object_cache_pointer,
				get_object,
				put_object,
			},
			session,
		};

		Ok(scylla)
	}

	async fn try_get_inner(
		&self,
		id: &tg::object::Id,
		statement: &scylla::statement::prepared::PreparedStatement,
	) -> tg::Result<Option<Bytes>> {
		let params = (id.to_bytes().to_vec(),);
		#[derive(scylla::DeserializeRow)]
		struct Row<'a> {
			bytes: Option<&'a [u8]>,
		}
		let result = self
			.session
			.execute_unpaged(statement, params)
			.boxed()
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to execute the query"))?
			.into_rows_result()
			.map_err(|source| tg::error!(!source, %id, "failed to get the rows"))?;
		let Some(row) = result
			.maybe_first_row::<Row>()
			.map_err(|source| tg::error!(!source, %id, "failed to get the row"))?
		else {
			return Ok(None);
		};
		let Some(bytes) = row.bytes else {
			return Ok(None);
		};
		let bytes = Bytes::copy_from_slice(bytes);
		Ok(Some(bytes))
	}

	async fn get_batch_inner(
		&self,
		ids: &[tg::object::Id],
		statement: &scylla::statement::prepared::PreparedStatement,
	) -> tg::Result<HashMap<tg::object::Id, Bytes, tg::id::BuildHasher>> {
		let id_bytes = ids
			.iter()
			.map(|id| id.to_bytes().to_vec())
			.collect::<Vec<_>>();
		let params = (id_bytes,);
		#[derive(scylla::DeserializeRow)]
		struct Row<'a> {
			id: &'a [u8],
			bytes: Option<&'a [u8]>,
		}
		let result = self
			.session
			.execute_unpaged(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the query"))?
			.into_rows_result()
			.map_err(|source| tg::error!(!source, "failed to get the rows"))?;
		let map = result
			.rows::<Row>()
			.map_err(|source| tg::error!(!source, "failed to iterate the rows"))?
			.filter_map(|result| {
				result
					.map_err(|source| tg::error!(!source, "failed to get the row"))
					.and_then(|row| {
						let Some(bytes) = row.bytes else {
							return Ok(None);
						};
						let id = tg::object::Id::from_slice(row.id)
							.map_err(|source| tg::error!(!source, "failed to parse the id"))?;
						let bytes = Bytes::copy_from_slice(bytes);
						Ok(Some((id, bytes)))
					})
					.transpose()
			})
			.collect::<tg::Result<_>>()?;
		Ok(map)
	}

	async fn try_get_cache_pointer_inner(
		&self,
		id: &tg::object::Id,
		statement: &scylla::statement::prepared::PreparedStatement,
	) -> tg::Result<Option<CachePointer>> {
		let params = (id.to_bytes().to_vec(),);
		#[derive(scylla::DeserializeRow)]
		struct Row<'a> {
			cache_pointer: Option<&'a [u8]>,
		}
		let result = self
			.session
			.execute_unpaged(statement, params)
			.boxed()
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to execute the query"))?
			.into_rows_result()
			.map_err(|source| tg::error!(!source, %id, "failed to get the rows"))?;
		let Some(row) = result
			.maybe_first_row::<Row>()
			.map_err(|source| tg::error!(!source, %id, "failed to get the row"))?
		else {
			return Ok(None);
		};
		let Some(cache_pointer) = row.cache_pointer else {
			return Ok(None);
		};
		let cache_pointer = CachePointer::deserialize(cache_pointer).map_err(
			|source| tg::error!(!source, %id, "failed to deserialize the cache pointer"),
		)?;
		Ok(Some(cache_pointer))
	}
}

impl crate::Store for Store {
	async fn try_get(&self, id: &tg::object::Id) -> tg::Result<Option<crate::Object<'static>>> {
		// Attempt to get the object with the default consistency.
		let bytes = self.try_get_inner(id, &self.statements.get_object).await?;
		if let Some(bytes) = bytes {
			// Get the cache pointer.
			let cache_pointer = self
				.try_get_cache_pointer_inner(id, &self.statements.get_object_cache_pointer)
				.await?;
			return Ok(Some(crate::Object {
				bytes: Some(Cow::Owned(bytes.to_vec())),
				touched_at: 0,
				cache_pointer,
			}));
		}

		// Attempt to get the object with local quorum consistency.
		let mut statement = self.statements.get_object.clone();
		statement.set_consistency(scylla::statement::Consistency::LocalQuorum);
		let bytes = self.try_get_inner(id, &statement).await?;
		if let Some(bytes) = bytes {
			// Get the cache pointer with local quorum consistency.
			let mut cache_pointer_statement = self.statements.get_object_cache_pointer.clone();
			cache_pointer_statement.set_consistency(scylla::statement::Consistency::LocalQuorum);
			let cache_pointer = self
				.try_get_cache_pointer_inner(id, &cache_pointer_statement)
				.await?;
			return Ok(Some(crate::Object {
				bytes: Some(Cow::Owned(bytes.to_vec())),
				touched_at: 0,
				cache_pointer,
			}));
		}

		Ok(None)
	}

	async fn try_get_batch(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<crate::Object<'static>>>> {
		// Attempt to get the objects with the default consistency.
		let mut map = self
			.get_batch_inner(ids, &self.statements.get_object_batch)
			.await?;

		// Attempt to get missing objects with local quorum consistency.
		let missing = ids
			.iter()
			.filter(|id| !map.contains_key(id))
			.collect::<Vec<_>>();
		if !missing.is_empty() {
			let mut statement = self.statements.get_object_batch.clone();
			statement.set_consistency(scylla::statement::Consistency::LocalQuorum);
			let missing_map = self.get_batch_inner(ids, &statement).await?;
			map.extend(missing_map);
		}

		// Create the output.
		let output = ids
			.iter()
			.map(|id| {
				map.get(id).cloned().map(|bytes| crate::Object {
					bytes: Some(Cow::Owned(bytes.to_vec())),
					touched_at: 0,
					cache_pointer: None,
				})
			})
			.collect();

		Ok(output)
	}

	async fn put(&self, arg: PutArg) -> tg::Result<()> {
		let id = &arg.id;
		let id_bytes = id.to_bytes().to_vec();
		let bytes = arg.bytes;
		let cache_pointer = if let Some(cache_pointer) = &arg.cache_pointer {
			let cache_pointer = cache_pointer.serialize().map_err(
				|source| tg::error!(!source, %id, "failed to serialize the cache pointer"),
			)?;
			Some(cache_pointer)
		} else {
			None
		};
		let touched_at = arg.touched_at;
		let params = (id_bytes, bytes, cache_pointer, touched_at);
		self.session
			.execute_unpaged(&self.statements.put_object, params)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to execute the query"))?;
		Ok(())
	}

	async fn put_batch(&self, args: Vec<PutArg>) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let mut batch =
			scylla::statement::batch::Batch::new(scylla::statement::batch::BatchType::Unlogged);
		batch.set_consistency(self.statements.put_object.get_consistency().unwrap());
		for _ in &args {
			batch.append_statement(scylla::statement::batch::BatchStatement::PreparedStatement(
				self.statements.put_object.clone(),
			));
		}
		let params = args
			.iter()
			.map(|arg| {
				let id = &arg.id;
				let id_bytes = id.to_bytes().to_vec();
				let bytes = arg.bytes.clone();
				let cache_pointer = if let Some(cache_pointer) = &arg.cache_pointer {
					let cache_pointer = cache_pointer.serialize().map_err(
						|source| tg::error!(!source, %id, "failed to serialize the cache pointer"),
					)?;
					Some(cache_pointer)
				} else {
					None
				};
				let touched_at = arg.touched_at;
				let params = (id_bytes, bytes, cache_pointer, touched_at);
				Ok(params)
			})
			.collect::<tg::Result<Vec<_>>>()?;
		self.session
			.batch(&batch, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the batch"))?;
		Ok(())
	}

	async fn delete(&self, arg: DeleteArg) -> tg::Result<()> {
		let id = &arg.id;
		let id_bytes = id.to_bytes().to_vec();
		let max_touched_at = arg.now - arg.ttl.to_i64().unwrap();
		let params = (id_bytes, max_touched_at);
		self.session
			.execute_unpaged(&self.statements.delete_object, params)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to execute the query"))?;
		Ok(())
	}

	async fn delete_batch(&self, args: Vec<DeleteArg>) -> tg::Result<()> {
		futures::future::try_join_all(args.into_iter().map(|arg| self.delete(arg))).await?;
		Ok(())
	}

	async fn flush(&self) -> tg::Result<()> {
		Ok(())
	}
}
