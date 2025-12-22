use {
	crate::{CacheReference, DeleteArg, DeleteLogArg, Error as _, GetLogArg, PutArg, PutLogArg},
	bytes::Bytes,
	futures::FutureExt as _,
	indoc::indoc,
	num::ToPrimitive as _,
	std::collections::HashMap,
	tangram_client::prelude::*,
};

#[derive(Clone, Debug)]
pub struct Config {
	pub addr: String,
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
	delete_statement: scylla::statement::prepared::PreparedStatement,
	get_batch_statement: scylla::statement::prepared::PreparedStatement,
	get_cache_reference_statement: scylla::statement::prepared::PreparedStatement,
	get_statement: scylla::statement::prepared::PreparedStatement,
	put_statement: scylla::statement::prepared::PreparedStatement,
	session: scylla::client::session::Session,
}

#[derive(Debug, derive_more::Display, derive_more::Error, derive_more::From)]
pub enum Error {
	Deserialization(scylla::deserialize::DeserializationError),
	Execution(scylla::errors::ExecutionError),
	IntoRowsResult(scylla::errors::IntoRowsResultError),
	MaybeFirstRow(scylla::errors::MaybeFirstRowError),
	NewSession(scylla::errors::NewSessionError),
	Prepare(scylla::errors::PrepareError),
	Rows(scylla::errors::RowsError),
	UseKeyspace(scylla::errors::UseKeyspaceError),
	Other(Box<dyn std::error::Error + Send + Sync>),
}

impl crate::Error for Error {
	fn other(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
		Self::Other(error.into())
	}
}

impl Store {
	pub async fn new(config: &Config) -> Result<Self, Error> {
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
		let session = builder.build().boxed().await.map_err(|source| {
			Error::other(tg::error!(!source, addr = %config.addr, "failed to build the session"))
		})?;
		session
			.use_keyspace(&config.keyspace, true)
			.await
			.map_err(|source| {
				Error::other(
					tg::error!(!source, keyspace = %config.keyspace, "failed to use the keyspace"),
				)
			})?;

		let statement = indoc!(
			"
				select bytes
				from objects
				where id = ?;
			"
		);
		let mut get_statement = session.prepare(statement).await.map_err(|source| {
			Error::other(tg::error!(!source, "failed to prepare the get statement"))
		})?;
		get_statement.set_consistency(scylla::statement::Consistency::One);

		let statement = indoc!(
			"
				select id, bytes
				from objects
				where id in ?;
			"
		);
		let mut get_batch_statement = session.prepare(statement).await.map_err(|source| {
			Error::other(tg::error!(
				!source,
				"failed to prepare the get batch statement"
			))
		})?;
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
				Error::other(tg::error!(
					!source,
					"failed to prepare the get cache reference statement"
				))
			})?;
		get_cache_reference_statement.set_consistency(scylla::statement::Consistency::One);

		let statement = indoc!(
			"
				insert into objects (id, bytes, cache_reference, touched_at)
				values (?, ?, ?, ?);
			"
		);
		let mut put_statement = session.prepare(statement).await.map_err(|source| {
			Error::other(tg::error!(!source, "failed to prepare the put statement"))
		})?;
		put_statement.set_consistency(scylla::statement::Consistency::LocalQuorum);

		let statement = indoc!(
			"
				delete from objects
				where id = ? if touched_at < ?;
			"
		);
		let mut delete_statement = session.prepare(statement).await.map_err(|source| {
			Error::other(tg::error!(
				!source,
				"failed to prepare the delete statement"
			))
		})?;
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

	async fn try_get_inner(
		&self,
		id: &tg::object::Id,
		statement: &scylla::statement::prepared::PreparedStatement,
	) -> Result<Option<Bytes>, Error> {
		let params = (id.to_bytes().to_vec(),);
		#[derive(scylla::DeserializeRow)]
		struct Row<'a> {
			bytes: &'a [u8],
		}
		let result = self
			.session
			.execute_unpaged(statement, params)
			.boxed()
			.await
			.map_err(|source| {
				Error::other(tg::error!(!source, %id, "failed to execute the query"))
			})?
			.into_rows_result()
			.map_err(|source| Error::other(tg::error!(!source, %id, "failed to get the rows")))?;
		let Some(row) = result
			.maybe_first_row::<Row>()
			.map_err(|source| Error::other(tg::error!(!source, %id, "failed to get the row")))?
		else {
			return Ok(None);
		};
		let bytes = Bytes::copy_from_slice(row.bytes);
		Ok(Some(bytes))
	}

	async fn get_batch_inner(
		&self,
		ids: &[tg::object::Id],
		statement: &scylla::statement::prepared::PreparedStatement,
	) -> Result<HashMap<tg::object::Id, Bytes, tg::id::BuildHasher>, Error> {
		let id_bytes = ids
			.iter()
			.map(|id| id.to_bytes().to_vec())
			.collect::<Vec<_>>();
		let params = (id_bytes,);
		#[derive(scylla::DeserializeRow)]
		struct Row<'a> {
			id: &'a [u8],
			bytes: &'a [u8],
		}
		let result = self
			.session
			.execute_unpaged(statement, params)
			.await
			.map_err(|source| Error::other(tg::error!(!source, "failed to execute the query")))?
			.into_rows_result()
			.map_err(|source| Error::other(tg::error!(!source, "failed to get the rows")))?;
		let map = result
			.rows::<Row>()
			.map_err(|source| Error::other(tg::error!(!source, "failed to iterate the rows")))?
			.map(|result| {
				result.map_err(Into::into).and_then(|row| {
					let id = tg::object::Id::from_slice(row.id).map_err(|source| {
						Error::other(tg::error!(!source, "failed to parse the id"))
					})?;
					let bytes = Bytes::copy_from_slice(row.bytes);
					Ok((id, bytes))
				})
			})
			.collect::<Result<_, Error>>()?;
		Ok(map)
	}

	async fn try_get_cache_reference_inner(
		&self,
		id: &tg::object::Id,
		statement: &scylla::statement::prepared::PreparedStatement,
	) -> Result<Option<CacheReference>, Error> {
		let params = (id.to_bytes().to_vec(),);
		#[derive(scylla::DeserializeRow)]
		struct Row<'a> {
			cache_reference: &'a [u8],
		}
		let result = self
			.session
			.execute_unpaged(statement, params)
			.boxed()
			.await
			.map_err(|source| {
				Error::other(tg::error!(!source, %id, "failed to execute the query"))
			})?
			.into_rows_result()
			.map_err(|source| Error::other(tg::error!(!source, %id, "failed to get the rows")))?;
		let Some(row) = result
			.maybe_first_row::<Row>()
			.map_err(|source| Error::other(tg::error!(!source, %id, "failed to get the row")))?
		else {
			return Ok(None);
		};
		let cache_reference =
			CacheReference::deserialize(row.cache_reference).map_err(|source| {
				Error::other(tg::error!(!source, %id, "failed to deserialize the cache reference"))
			})?;
		Ok(Some(cache_reference))
	}
}

impl crate::Store for Store {
	type Error = Error;

	async fn try_get(&self, id: &tg::object::Id) -> Result<Option<Bytes>, Self::Error> {
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

	async fn try_read_log(&self, _arg: GetLogArg) -> Result<Option<Bytes>, Self::Error> {
		todo!()
	}

	async fn try_get_log_length(
		&self,
		_id: &tg::process::Id,
		_stream: Option<tg::process::log::Stream>,
	) -> Result<Option<u64>, Self::Error> {
		todo!()
	}

	async fn try_get_batch(
		&self,
		ids: &[tg::object::Id],
	) -> Result<Vec<Option<Bytes>>, Self::Error> {
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

	async fn try_get_cache_reference(
		&self,
		id: &tg::object::Id,
	) -> Result<Option<CacheReference>, Self::Error> {
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

	async fn put(&self, arg: PutArg) -> Result<(), Self::Error> {
		let id = &arg.id;
		let id_bytes = id.to_bytes().to_vec();
		let bytes = arg.bytes;
		let cache_reference = if let Some(cache_reference) = &arg.cache_reference {
			let cache_reference = cache_reference.serialize().map_err(|source| {
				Error::other(tg::error!(!source, %id, "failed to serialize the cache reference"))
			})?;
			Some(cache_reference)
		} else {
			None
		};
		let touched_at = arg.touched_at;
		let params = (id_bytes, bytes, cache_reference, touched_at);
		self.session
			.execute_unpaged(&self.put_statement, params)
			.await
			.map_err(|source| {
				Error::other(tg::error!(!source, %id, "failed to execute the query"))
			})?;
		Ok(())
	}

	async fn put_log(&self, _arg: PutLogArg) -> Result<(), Self::Error> {
		todo!()
	}

	async fn put_batch(&self, args: Vec<PutArg>) -> Result<(), Self::Error> {
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
				let id = &arg.id;
				let id_bytes = id.to_bytes().to_vec();
				let bytes = arg.bytes.clone();
				let cache_reference = if let Some(cache_reference) = &arg.cache_reference {
					let cache_reference = cache_reference.serialize().map_err(|source| {
						Error::other(
							tg::error!(!source, %id, "failed to serialize the cache reference"),
						)
					})?;
					Some(cache_reference)
				} else {
					None
				};
				let touched_at = arg.touched_at;
				let params = (id_bytes, bytes, cache_reference, touched_at);
				Ok(params)
			})
			.collect::<Result<Vec<_>, Error>>()?;
		self.session
			.batch(&batch, params)
			.await
			.map_err(|source| Error::other(tg::error!(!source, "failed to execute the batch")))?;
		Ok(())
	}

	async fn put_log_batch(&self, _args: Vec<PutLogArg>) -> Result<(), Self::Error> {
		todo!()
	}

	async fn delete(&self, arg: DeleteArg) -> Result<(), Self::Error> {
		let id = &arg.id;
		let id_bytes = id.to_bytes().to_vec();
		let max_touched_at = arg.now - arg.ttl.to_i64().unwrap();
		let params = (id_bytes, max_touched_at);
		self.session
			.execute_unpaged(&self.delete_statement, params)
			.await
			.map_err(|source| {
				Error::other(tg::error!(!source, %id, "failed to execute the query"))
			})?;
		Ok(())
	}

	async fn delete_log(&self, _arg: DeleteLogArg) -> Result<(), Self::Error> {
		todo!()
	}

	async fn delete_batch(&self, args: Vec<DeleteArg>) -> Result<(), Self::Error> {
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
				let id_bytes = arg.id.to_bytes().to_vec();
				let max_touched_at = arg.now - arg.ttl.to_i64().unwrap();
				(id_bytes, max_touched_at)
			})
			.collect::<Vec<_>>();
		self.session
			.batch(&batch, params)
			.await
			.map_err(|source| Error::other(tg::error!(!source, "failed to execute the batch")))?;
		Ok(())
	}

	async fn delete_log_batch(&self, _args: Vec<DeleteLogArg>) -> Result<(), Self::Error> {
		todo!()
	}

	async fn flush(&self) -> Result<(), Self::Error> {
		Ok(())
	}
}
