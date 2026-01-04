use {
	crate::{
		CachePointer, DeleteObjectArg, DeleteProcessLogArg, Error as _, PutObjectArg,
		PutProcessLogArg, ReadProcessLogArg,
	},
	bytes::Bytes,
	futures::{FutureExt as _, TryStreamExt as _},
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
	// Objects
	delete_object_statement: scylla::statement::prepared::PreparedStatement,
	get_object_batch_statement: scylla::statement::prepared::PreparedStatement,
	get_object_cache_pointer_statement: scylla::statement::prepared::PreparedStatement,
	get_object_statement: scylla::statement::prepared::PreparedStatement,
	put_object_statement: scylla::statement::prepared::PreparedStatement,

	// Process log entries.
	delete_process_log_entries_statement: scylla::statement::prepared::PreparedStatement,
	delete_process_log_stream_positions_statement: scylla::statement::prepared::PreparedStatement,
	get_combined_statement: scylla::statement::prepared::PreparedStatement,
	get_last_combined_statement: scylla::statement::prepared::PreparedStatement,
	get_last_stream_statement: scylla::statement::prepared::PreparedStatement,
	get_stream_index_at_or_before_statement: scylla::statement::prepared::PreparedStatement,
	put_combined_statement: scylla::statement::prepared::PreparedStatement,
	put_stream_statement: scylla::statement::prepared::PreparedStatement,
	read_combined_statement: scylla::statement::prepared::PreparedStatement,

	session: scylla::client::session::Session,
}

#[derive(Debug, derive_more::Display, derive_more::Error, derive_more::From)]
pub enum Error {
	Deserialization(scylla::deserialize::DeserializationError),
	Execution(scylla::errors::ExecutionError),
	IntoRowsResult(scylla::errors::IntoRowsResultError),
	MaybeFirstRow(scylla::errors::MaybeFirstRowError),
	NewSession(scylla::errors::NewSessionError),
	NextRowError(scylla::errors::NextRowError),
	Other(Box<dyn std::error::Error + Send + Sync>),
	PagerExecution(scylla::errors::PagerExecutionError),
	Prepare(scylla::errors::PrepareError),
	Rows(scylla::errors::RowsError),
	SingleRow(scylla::errors::SingleRowError),
	TypeCheckError(scylla::errors::TypeCheckError),
	UseKeyspace(scylla::errors::UseKeyspaceError),
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
		if let Some(connections) = config.connections.and_then(std::num::NonZeroUsize::new) {
			builder = builder.pool_size(scylla::client::PoolSize::PerHost(connections));
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
				delete from objects
				where id = ? if touched_at < ?;
			"
		);
		let mut delete_object_statement = session.prepare(statement).await.map_err(|source| {
			Error::other(tg::error!(
				!source,
				"failed to prepare the delete statement"
			))
		})?;
		delete_object_statement.set_consistency(scylla::statement::Consistency::LocalQuorum);

		let statement = indoc!(
			"
				select id, bytes
				from objects
				where id in ?;
			"
		);
		let mut get_object_batch_statement =
			session.prepare(statement).await.map_err(|source| {
				Error::other(tg::error!(
					!source,
					"failed to prepare the get batch statement"
				))
			})?;
		get_object_batch_statement.set_consistency(scylla::statement::Consistency::One);

		let statement = indoc!(
			"
				select cache_pointer
				from objects
				where id = ?;
			"
		);
		let mut get_object_cache_pointer_statement =
			session.prepare(statement).await.map_err(|source| {
				Error::other(tg::error!(
					!source,
					"failed to prepare the get cache pointer statement"
				))
			})?;
		get_object_cache_pointer_statement.set_consistency(scylla::statement::Consistency::One);

		let statement = indoc!(
			"
				select bytes
				from objects
				where id = ?;
			"
		);
		let mut get_object_statement = session.prepare(statement).await.map_err(|source| {
			Error::other(tg::error!(!source, "failed to prepare the get statement"))
		})?;
		get_object_statement.set_consistency(scylla::statement::Consistency::One);

		let statement = indoc!(
			"
				insert into objects (id, bytes, cache_pointer, touched_at)
				values (?, ?, ?, ?);
			"
		);
		let mut put_object_statement = session.prepare(statement).await.map_err(|source| {
			Error::other(tg::error!(!source, "failed to prepare the put statement"))
		})?;
		put_object_statement.set_consistency(scylla::statement::Consistency::LocalQuorum);

		// Process log statements.
		let statement = indoc!(
			"
				delete from process_log_entries
				where process = ?;
			"
		);
		let mut delete_process_log_entries_statement = session.prepare(statement).await?;
		delete_process_log_entries_statement
			.set_consistency(scylla::statement::Consistency::LocalQuorum);

		let statement = indoc!(
			"
				delete from process_log_stream_positions
				where process = ?;
			"
		);
		let mut delete_process_log_stream_positions_statement = session.prepare(statement).await?;
		delete_process_log_stream_positions_statement
			.set_consistency(scylla::statement::Consistency::LocalQuorum);

		let statement = indoc!(
			"
				select stream_position, bytes
				from process_log_entries
				where process = ? and position = ?;
			"
		);
		let mut get_combined_statement = session.prepare(statement).await?;
		get_combined_statement.set_consistency(scylla::statement::Consistency::One);

		let statement = indoc!(
			"
				select position, bytes
				from process_log_entries
				where process = ?
				order by position desc
				limit 1;
			"
		);
		let mut get_last_combined_statement = session.prepare(statement).await?;
		get_last_combined_statement.set_consistency(scylla::statement::Consistency::One);

		let statement = indoc!(
			"
				select stream_position, position
				from process_log_stream_positions
				where process = ? and stream = ?
				order by stream_position desc
				limit 1;
			"
		);
		let mut get_last_stream_statement = session.prepare(statement).await?;
		get_last_stream_statement.set_consistency(scylla::statement::Consistency::One);

		let statement = indoc!(
			"
				select stream_position, position
				from process_log_stream_positions
				where process = ? and stream = ? and stream_position <= ?
				order by stream_position desc
				limit 1;
			"
		);
		let mut get_stream_index_at_or_before_statement = session.prepare(statement).await?;
		get_stream_index_at_or_before_statement
			.set_consistency(scylla::statement::Consistency::One);

		let statement = indoc!(
			"
				insert into process_log_entries (process, position, bytes, stream, stream_position, timestamp)
				values (?, ?, ?, ?, ?, ?)
				if not exists;
			"
		);
		let mut put_combined_statement = session.prepare(statement).await?;
		put_combined_statement.set_consistency(scylla::statement::Consistency::LocalQuorum);

		let statement = indoc!(
			"
				insert into process_log_stream_positions (process, stream, stream_position, position)
				values (?, ?, ?, ?);
			"
		);
		let mut put_stream_statement = session.prepare(statement).await?;
		put_stream_statement.set_consistency(scylla::statement::Consistency::LocalQuorum);

		let statement = indoc!(
			"
				select position, bytes, stream, stream_position, timestamp
				from process_log_entries
				where process = ? and position >= ?;
			"
		);
		let mut read_combined_statement = session.prepare(statement).await?;
		read_combined_statement.set_consistency(scylla::statement::Consistency::One);

		let scylla = Self {
			delete_object_statement,
			get_object_batch_statement,
			get_object_cache_pointer_statement,
			get_object_statement,
			put_object_statement,
			delete_process_log_entries_statement,
			delete_process_log_stream_positions_statement,
			get_combined_statement,
			get_last_combined_statement,
			get_last_stream_statement,
			get_stream_index_at_or_before_statement,
			put_combined_statement,
			put_stream_statement,
			read_combined_statement,
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
			bytes: Option<&'a [u8]>,
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
	) -> Result<HashMap<tg::object::Id, Bytes, tg::id::BuildHasher>, Error> {
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
			.map_err(|source| Error::other(tg::error!(!source, "failed to execute the query")))?
			.into_rows_result()
			.map_err(|source| Error::other(tg::error!(!source, "failed to get the rows")))?;
		let map = result
			.rows::<Row>()
			.map_err(|source| Error::other(tg::error!(!source, "failed to iterate the rows")))?
			.filter_map(|result| {
				result
					.map_err(Into::into)
					.and_then(|row| {
						let Some(bytes) = row.bytes else {
							return Ok(None);
						};
						let id = tg::object::Id::from_slice(row.id).map_err(|source| {
							Error::other(tg::error!(!source, "failed to parse the id"))
						})?;
						let bytes = Bytes::copy_from_slice(bytes);
						Ok(Some((id, bytes)))
					})
					.transpose()
			})
			.collect::<Result<_, Error>>()?;
		Ok(map)
	}

	async fn try_get_cache_pointer_inner(
		&self,
		id: &tg::object::Id,
		statement: &scylla::statement::prepared::PreparedStatement,
	) -> Result<Option<CachePointer>, Error> {
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
		let Some(cache_pointer) = row.cache_pointer else {
			return Ok(None);
		};
		let cache_pointer = CachePointer::deserialize(cache_pointer).map_err(|source| {
			Error::other(tg::error!(!source, %id, "failed to deserialize the cache pointer"))
		})?;
		Ok(Some(cache_pointer))
	}
}

impl crate::Store for Store {
	type Error = Error;

	async fn try_get_object(
		&self,
		id: &tg::object::Id,
	) -> Result<Option<crate::Object<'static>>, Self::Error> {
		// Attempt to get the object with the default consistency.
		let bytes = self.try_get_inner(id, &self.get_object_statement).await?;
		if let Some(bytes) = bytes {
			// Get the cache pointer.
			let cache_pointer = self
				.try_get_cache_pointer_inner(id, &self.get_object_cache_pointer_statement)
				.await?;
			return Ok(Some(crate::Object {
				bytes: Some(Cow::Owned(bytes.to_vec())),
				touched_at: 0,
				cache_pointer,
			}));
		}

		// Attempt to get the object with local quorum consistency.
		let mut statement = self.get_object_statement.clone();
		statement.set_consistency(scylla::statement::Consistency::LocalQuorum);
		let bytes = self.try_get_inner(id, &statement).await?;
		if let Some(bytes) = bytes {
			// Get the cache pointer with local quorum consistency.
			let mut cache_pointer_statement = self.get_object_cache_pointer_statement.clone();
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

	async fn try_get_object_batch(
		&self,
		ids: &[tg::object::Id],
	) -> Result<Vec<Option<crate::Object<'static>>>, Self::Error> {
		// Attempt to get the objects with the default consistency.
		let mut map = self
			.get_batch_inner(ids, &self.get_object_batch_statement)
			.await?;

		// Attempt to get missing objects with local quorum consistency.
		let missing = ids
			.iter()
			.filter(|id| !map.contains_key(id))
			.collect::<Vec<_>>();
		if !missing.is_empty() {
			let mut statement = self.get_object_batch_statement.clone();
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

	async fn put_object(&self, arg: PutObjectArg) -> Result<(), Self::Error> {
		let id = &arg.id;
		let id_bytes = id.to_bytes().to_vec();
		let bytes = arg.bytes;
		let cache_pointer = if let Some(cache_pointer) = &arg.cache_pointer {
			let cache_pointer = cache_pointer.serialize().map_err(|source| {
				Error::other(tg::error!(!source, %id, "failed to serialize the cache pointer"))
			})?;
			Some(cache_pointer)
		} else {
			None
		};
		let touched_at = arg.touched_at;
		let params = (id_bytes, bytes, cache_pointer, touched_at);
		self.session
			.execute_unpaged(&self.put_object_statement, params)
			.await
			.map_err(|source| {
				Error::other(tg::error!(!source, %id, "failed to execute the query"))
			})?;
		Ok(())
	}

	async fn put_object_batch(&self, args: Vec<PutObjectArg>) -> Result<(), Self::Error> {
		if args.is_empty() {
			return Ok(());
		}
		let mut batch =
			scylla::statement::batch::Batch::new(scylla::statement::batch::BatchType::Unlogged);
		batch.set_consistency(self.put_object_statement.get_consistency().unwrap());
		for _ in &args {
			batch.append_statement(scylla::statement::batch::BatchStatement::PreparedStatement(
				self.put_object_statement.clone(),
			));
		}
		let params = args
			.iter()
			.map(|arg| {
				let id = &arg.id;
				let id_bytes = id.to_bytes().to_vec();
				let bytes = arg.bytes.clone();
				let cache_pointer = if let Some(cache_pointer) = &arg.cache_pointer {
					let cache_pointer = cache_pointer.serialize().map_err(|source| {
						Error::other(
							tg::error!(!source, %id, "failed to serialize the cache pointer"),
						)
					})?;
					Some(cache_pointer)
				} else {
					None
				};
				let touched_at = arg.touched_at;
				let params = (id_bytes, bytes, cache_pointer, touched_at);
				Ok(params)
			})
			.collect::<Result<Vec<_>, Error>>()?;
		self.session
			.batch(&batch, params)
			.await
			.map_err(|source| Error::other(tg::error!(!source, "failed to execute the batch")))?;
		Ok(())
	}

	async fn delete_object(&self, arg: DeleteObjectArg) -> Result<(), Self::Error> {
		let id = &arg.id;
		let id_bytes = id.to_bytes().to_vec();
		let max_touched_at = arg.now - arg.ttl.to_i64().unwrap();
		let params = (id_bytes, max_touched_at);
		self.session
			.execute_unpaged(&self.delete_object_statement, params)
			.await
			.map_err(|source| {
				Error::other(tg::error!(!source, %id, "failed to execute the query"))
			})?;
		Ok(())
	}

	async fn delete_object_batch(&self, args: Vec<DeleteObjectArg>) -> Result<(), Self::Error> {
		if args.is_empty() {
			return Ok(());
		}
		let mut batch =
			scylla::statement::batch::Batch::new(scylla::statement::batch::BatchType::Unlogged);
		batch.set_consistency(self.delete_object_statement.get_consistency().unwrap());
		for _ in &args {
			batch.append_statement(scylla::statement::batch::BatchStatement::PreparedStatement(
				self.delete_object_statement.clone(),
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

	async fn try_read_process_log(
		&self,
		arg: ReadProcessLogArg,
	) -> Result<Vec<crate::ProcessLogEntry<'static>>, Self::Error> {
		let process = arg.process.to_bytes().to_vec();

		// Find the starting position.
		let start = if let Some(stream) = arg.stream {
			let stream = match stream {
				tg::process::log::Stream::Stdout => 1i32,
				tg::process::log::Stream::Stderr => 2i32,
			};
			#[derive(scylla::DeserializeRow)]
			struct StreamRow {
				#[allow(dead_code)]
				stream_position: i64,
				position: i64,
			}
			let params = (&process, stream, arg.position.to_i64().unwrap());
			let result = self
				.session
				.execute_unpaged(&self.get_stream_index_at_or_before_statement, params)
				.await?
				.into_rows_result()?;
			result
				.maybe_first_row::<StreamRow>()?
				.map(|row| row.position.to_u64().unwrap())
		} else {
			Some(arg.position)
		};
		let Some(start) = start else {
			return Ok(Vec::new());
		};

		// Read combined entries from start position.
		#[derive(scylla::DeserializeRow)]
		struct Row {
			position: i64,
			bytes: Vec<u8>,
			stream: i32,
			stream_position: i64,
			timestamp: i64,
		}

		let params = (&process, start.to_i64().unwrap());
		let mut iter = self
			.session
			.execute_iter(self.read_combined_statement.clone(), params)
			.await?
			.rows_stream::<Row>()?;

		let mut remaining = arg.length;
		let mut output = Vec::new();
		let mut current: Option<crate::ProcessLogEntry> = None;

		while remaining > 0 {
			let Some(row) = iter.try_next().await? else {
				break;
			};

			// Determine the stream type.
			let chunk_stream = if row.stream == 1 {
				tg::process::log::Stream::Stdout
			} else {
				tg::process::log::Stream::Stderr
			};

			// Skip entries that do not match the stream filter.
			if arg.stream.is_some_and(|stream| stream != chunk_stream) {
				continue;
			}

			// Get the position based on stream filter.
			let position = if arg.stream.is_some() {
				row.stream_position.to_u64().unwrap()
			} else {
				row.position.to_u64().unwrap()
			};

			let offset = arg.position.saturating_sub(position);

			let available = row.bytes.len().to_u64().unwrap().saturating_sub(offset);
			let take = remaining.min(available);

			let bytes: Cow<'_, [u8]> = if offset > 0 || take < row.bytes.len().to_u64().unwrap() {
				Cow::Owned(
					row.bytes[offset.to_usize().unwrap()..(offset + take).to_usize().unwrap()]
						.to_vec(),
				)
			} else {
				Cow::Owned(row.bytes.clone())
			};

			// Combine sequential entries from the same stream.
			if let Some(ref mut entry) = current {
				if entry.stream == chunk_stream {
					let mut combined = entry.bytes.to_vec();
					combined.extend_from_slice(&bytes);
					entry.bytes = Cow::Owned(combined);
				} else {
					output.push(current.take().unwrap());
					current = Some(crate::ProcessLogEntry {
						bytes,
						position: row.position.to_u64().unwrap() + offset,
						stream_position: row.stream_position.to_u64().unwrap() + offset,
						stream: chunk_stream,
						timestamp: row.timestamp,
					});
				}
			} else {
				current = Some(crate::ProcessLogEntry {
					bytes,
					position: row.position.to_u64().unwrap() + offset,
					stream_position: row.stream_position.to_u64().unwrap() + offset,
					stream: chunk_stream,
					timestamp: row.timestamp,
				});
			}

			remaining -= take;
		}

		// Push the last entry if any.
		if let Some(entry) = current {
			output.push(entry);
		}

		Ok(output)
	}

	async fn try_get_process_log_length(
		&self,
		id: &tg::process::Id,
		stream: Option<tg::process::log::Stream>,
	) -> Result<Option<u64>, Self::Error> {
		let process = id.to_bytes().to_vec();

		match stream {
			None => {
				// Combined length: get last combined entry.
				#[derive(scylla::DeserializeRow)]
				struct Row {
					position: i64,
					bytes: Vec<u8>,
				}
				let params = (&process,);
				let result = self
					.session
					.execute_unpaged(&self.get_last_combined_statement, params)
					.await?
					.into_rows_result()?;
				let Some(row) = result.maybe_first_row::<Row>()? else {
					return Ok(None);
				};
				Ok(Some(
					row.position.to_u64().unwrap() + row.bytes.len().to_u64().unwrap(),
				))
			},
			Some(stream) => {
				// Stream length: get last stream index entry, then look up combined entry.
				let stream = match stream {
					tg::process::log::Stream::Stdout => 1i32,
					tg::process::log::Stream::Stderr => 2i32,
				};

				#[derive(scylla::DeserializeRow)]
				struct StreamRow {
					#[allow(dead_code)]
					stream_position: i64,
					position: i64,
				}
				let params = (&process, stream);
				let result = self
					.session
					.execute_unpaged(&self.get_last_stream_statement, params)
					.await?
					.into_rows_result()?;
				let Some(index_row) = result.maybe_first_row::<StreamRow>()? else {
					return Ok(None);
				};

				// Look up the entry to get stream_position and bytes length.
				#[derive(scylla::DeserializeRow)]
				struct EntryRow {
					stream_position: i64,
					bytes: Vec<u8>,
				}
				let params = (&process, index_row.position);
				let result = self
					.session
					.execute_unpaged(&self.get_combined_statement, params)
					.await?
					.into_rows_result()?;
				let Some(entry_row) = result.maybe_first_row::<EntryRow>()? else {
					return Ok(None);
				};

				Ok(Some(
					entry_row.stream_position.to_u64().unwrap()
						+ entry_row.bytes.len().to_u64().unwrap(),
				))
			},
		}
	}

	async fn put_process_log(&self, arg: PutProcessLogArg) -> Result<(), Self::Error> {
		let process = arg.process.to_bytes().to_vec();
		let stream = match arg.stream {
			tg::process::log::Stream::Stdout => 1i32,
			tg::process::log::Stream::Stderr => 2i32,
		};

		#[derive(scylla::DeserializeRow)]
		struct LastEntryRow {
			position: i64,
			bytes: Vec<u8>,
		}

		#[derive(scylla::DeserializeRow)]
		struct LastStreamPositionRow {
			#[allow(dead_code)]
			stream_position: i64,
			position: i64,
		}

		#[derive(scylla::DeserializeRow)]
		struct EntryInfoRow {
			bytes: Vec<u8>,
			stream_position: i64,
		}

		#[derive(scylla::DeserializeRow)]
		#[allow(dead_code)]
		struct Row {
			#[scylla(rename = "[applied]")]
			applied: bool,
			process: Option<Vec<u8>>,
			position: Option<i64>,
			bytes: Option<Vec<u8>>,
			stream: Option<i32>,
			stream_position: Option<i64>,
			timestamp: Option<i64>,
		}

		loop {
			let params = (&process,);
			let result = self
				.session
				.execute_unpaged(&self.get_last_combined_statement, params)
				.await?
				.into_rows_result()?;
			let position = result.maybe_first_row::<LastEntryRow>()?.map_or(0, |row| {
				row.position.to_u64().unwrap() + row.bytes.len().to_u64().unwrap()
			});

			let params = (&process, stream);
			let result = self
				.session
				.execute_unpaged(&self.get_last_stream_statement, params)
				.await?
				.into_rows_result()?;
			let stream_position =
				if let Some(index_row) = result.maybe_first_row::<LastStreamPositionRow>()? {
					let params = (&process, index_row.position);
					let result = self
						.session
						.execute_unpaged(&self.get_combined_statement, params)
						.await?
						.into_rows_result()?;
					result.maybe_first_row::<EntryInfoRow>()?.map_or(0, |row| {
						row.stream_position.to_u64().unwrap() + row.bytes.len().to_u64().unwrap()
					})
				} else {
					0
				};

			let params = (
				&process,
				position.to_i64().unwrap(),
				arg.bytes.as_ref(),
				stream,
				stream_position.to_i64().unwrap(),
				arg.timestamp,
			);
			let result = self
				.session
				.execute_unpaged(&self.put_combined_statement, params)
				.await?
				.into_rows_result()?;

			let row = result.single_row::<Row>()?;
			if !row.applied {
				continue;
			}

			let params = (
				&process,
				stream,
				stream_position.to_i64().unwrap(),
				position.to_i64().unwrap(),
			);
			let _ = self
				.session
				.execute_unpaged(&self.put_stream_statement, params)
				.await;

			return Ok(());
		}
	}

	async fn delete_process_log(&self, arg: DeleteProcessLogArg) -> Result<(), Self::Error> {
		let process = arg.process.to_bytes().to_vec();
		let params = (&process,);
		self.session
			.execute_unpaged(&self.delete_process_log_entries_statement, params)
			.await?;
		self.session
			.execute_unpaged(&self.delete_process_log_stream_positions_statement, params)
			.await?;
		Ok(())
	}

	async fn flush(&self) -> Result<(), Self::Error> {
		Ok(())
	}
}
