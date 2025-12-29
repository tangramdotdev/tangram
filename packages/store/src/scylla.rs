use {
	crate::{CacheReference, DeleteArg, DeleteLogArg, Error as _, PutArg, PutLogArg, ReadLogArg},
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
	// Objects
	delete_statement: scylla::statement::prepared::PreparedStatement,
	get_batch_statement: scylla::statement::prepared::PreparedStatement,
	get_cache_reference_statement: scylla::statement::prepared::PreparedStatement,
	get_statement: scylla::statement::prepared::PreparedStatement,
	put_statement: scylla::statement::prepared::PreparedStatement,

	// Logs.
	delete_logs_statement: scylla::statement::prepared::PreparedStatement,
	put_log_statement: scylla::statement::prepared::PreparedStatement,
	get_log_by_index_statement: scylla::statement::prepared::PreparedStatement,
	get_log_by_combined_position_statement: scylla::statement::prepared::PreparedStatement,
	get_log_by_stdout_position_statement: scylla::statement::prepared::PreparedStatement,
	get_log_by_stderr_position_statement: scylla::statement::prepared::PreparedStatement,

	get_last_log_statement: scylla::statement::prepared::PreparedStatement,
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
	PagerExecution(scylla::errors::PagerExecutionError),
	TypeCheckError(scylla::errors::TypeCheckError),
	NextRowError(scylla::errors::NextRowError),
	SingleRow(scylla::errors::SingleRowError),
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

		let statement = indoc!(
			"
				delete from logs
				where id = ?;
			"
		);
		let mut delete_logs_statement = session.prepare(statement).await?;
		delete_logs_statement.set_consistency(scylla::statement::Consistency::LocalQuorum);

		let statement = indoc!(
			"
				select bytes, stream, combined_start, combined_end, stdout_start, stdout_end, stderr_start, stderr_end, timestamp
				from logs
				where
					id = ? and log_index = ?
				allow filtering;
			"
		);
		let mut get_log_by_index_statement = session.prepare(statement).await?;
		get_log_by_index_statement.set_consistency(scylla::statement::Consistency::One);

		let statement = indoc!(
			"
				select bytes, stream, combined_start, combined_end, stdout_start, stdout_end, stderr_start, stderr_end, timestamp
				from logs
				where
					id = ? and combined_end > ? and combined_start <= ?
				order by log_index asc
				allow filtering;
			"
		);
		let mut get_log_by_combined_position_statement = session.prepare(statement).await?;
		get_log_by_combined_position_statement.set_consistency(scylla::statement::Consistency::One);

		let statement = indoc!(
			"
				select bytes, stream, combined_start, combined_end, stdout_start, stdout_end, stderr_start, stderr_end, timestamp
				from logs
				where
					id = ? and stream = ? and stdout_end > ? and stdout_start <= ?
				order by log_index asc
				allow filtering;
			"
		);
		let mut get_log_by_stdout_position_statement = session.prepare(statement).await?;
		get_log_by_stdout_position_statement.set_consistency(scylla::statement::Consistency::One);

		let statement = indoc!(
			"
				select bytes, stream, combined_start, combined_end, stdout_start, stdout_end, stderr_start, stderr_end, timestamp
				from logs
				where
					id = ? and stream = ? and stderr_end > ? and stderr_start <= ?
				order by log_index asc
				allow filtering;
			"
		);
		let mut get_log_by_stderr_position_statement = session.prepare(statement).await?;
		get_log_by_stderr_position_statement.set_consistency(scylla::statement::Consistency::One);

		let statement = indoc!(
			"
				select log_index, bytes, stream, combined_start, combined_end, stdout_start, stdout_end, stderr_start, stderr_end, timestamp
				from logs
				where id = ?
				order by log_index desc
				limit 1;
			"
		);
		let mut get_last_log_statement = session.prepare(statement).await?;
		get_last_log_statement.set_consistency(scylla::statement::Consistency::One);

		let statement = indoc!(
			"
				insert into logs (id, log_index, bytes, stream, combined_start, combined_end, stdout_start, stdout_end, stderr_start, stderr_end, timestamp)
				values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
				if not exists;
			"
		);
		let mut put_log_statement = session.prepare(statement).await?;
		put_log_statement.set_consistency(scylla::statement::Consistency::LocalQuorum);

		let scylla = Self {
			delete_statement,
			get_batch_statement,
			get_cache_reference_statement,
			get_statement,
			put_statement,
			delete_logs_statement,
			put_log_statement,
			get_log_by_index_statement,
			get_log_by_combined_position_statement,
			get_log_by_stdout_position_statement,
			get_log_by_stderr_position_statement,
			get_last_log_statement,
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

	async fn try_read_log(&self, arg: ReadLogArg) -> Result<Vec<crate::log::Entry>, Self::Error> {
		use futures::TryStreamExt as _;

		let id = arg.process.to_bytes().to_vec();
		let start_position = arg.position.to_i64().unwrap();
		let end_position = start_position + arg.length.to_i64().unwrap();

		#[derive(scylla::DeserializeRow)]
		#[allow(dead_code)]
		struct Row {
			bytes: Vec<u8>,
			stream: i64,
			combined_start: i64,
			combined_end: i64,
			stdout_start: i64,
			stdout_end: i64,
			stderr_start: i64,
			stderr_end: i64,
			timestamp: i64,
		}

		// Query for log entries that overlap the range [position, position + length).
		let mut iter = if let Some(stream) = arg.stream {
			let (stream_value, statement) = match stream {
				tg::process::log::Stream::Stdout => {
					(1i64, self.get_log_by_stdout_position_statement.clone())
				},
				tg::process::log::Stream::Stderr => {
					(2i64, self.get_log_by_stderr_position_statement.clone())
				},
			};
			let params = (id, stream_value, start_position, end_position);
			self.session
				.execute_iter(statement, params)
				.await?
				.rows_stream::<Row>()?
		} else {
			let params = (id, start_position, end_position);
			self.session
				.execute_iter(self.get_log_by_combined_position_statement.clone(), params)
				.await?
				.rows_stream::<Row>()?
		};

		let mut remaining = arg.length;
		let mut output = Vec::new();
		let mut current: Option<crate::log::Entry> = None;

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

			// Get the position based on stream filter.
			let position = if arg.stream.is_some() {
				if chunk_stream == tg::process::log::Stream::Stdout {
					row.stdout_start.to_u64().unwrap()
				} else {
					row.stderr_start.to_u64().unwrap()
				}
			} else {
				row.combined_start.to_u64().unwrap()
			};

			// Handle edge case: chunk starts before arg.position.
			let offset = arg.position.saturating_sub(position);

			// Handle edge case: chunk extends beyond arg.position + arg.length.
			let available = row.bytes.len().to_u64().unwrap().saturating_sub(offset);
			let take = remaining.min(available);

			// Slice the bytes if needed.
			let bytes: Bytes = if offset > 0 || take < row.bytes.len().to_u64().unwrap() {
				row.bytes[offset.to_usize().unwrap()..(offset + take).to_usize().unwrap()]
					.to_vec()
					.into()
			} else {
				row.bytes.into()
			};

			// Combine sequential entries from the same stream.
			if let Some(ref mut entry) = current {
				if entry.stream == chunk_stream {
					// Append bytes to current entry.
					let mut combined = entry.bytes.to_vec();
					combined.extend_from_slice(&bytes);
					entry.bytes = combined.into();
				} else {
					// Different stream, push current and start new.
					output.push(current.take().unwrap());
					current = Some(crate::log::Entry {
						bytes,
						combined_position: row.combined_start.to_u64().unwrap() + offset,
						stream_position: position + offset,
						stream: chunk_stream,
						timestamp: row.timestamp,
					});
				}
			} else {
				// Start first entry.
				current = Some(crate::log::Entry {
					bytes,
					combined_position: row.combined_start.to_u64().unwrap() + offset,
					stream_position: position + offset,
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

	async fn try_get_log_length(
		&self,
		id: &tg::process::Id,
		stream: Option<tg::process::log::Stream>,
	) -> Result<Option<u64>, Self::Error> {
		let id = id.to_bytes().to_vec();
		#[derive(scylla::DeserializeRow)]
		#[allow(dead_code)]
		struct Row<'a> {
			log_index: i64,
			bytes: &'a [u8],
			stream: i64,
			combined_start: i64,
			combined_end: i64,
			stdout_start: i64,
			stdout_end: i64,
			stderr_start: i64,
			stderr_end: i64,
			timestamp: i64,
		}
		let params = (id,);
		let result = self
			.session
			.execute_unpaged(&self.get_last_log_statement, params)
			.await?
			.into_rows_result()?;
		let Some(row) = result.maybe_first_row::<Row>()? else {
			return Ok(None);
		};
		let length = match stream {
			None => row.combined_end,
			Some(tg::process::log::Stream::Stdout) => row.stdout_end,
			Some(tg::process::log::Stream::Stderr) => row.stderr_end,
		};
		Ok(Some(length.to_u64().unwrap()))
	}

	async fn try_get_log_entry(
		&self,
		id: &tg::process::Id,
		index: u64,
	) -> Result<Option<crate::log::Entry>, Self::Error> {
		let id = id.to_bytes().to_vec();
		#[derive(scylla::DeserializeRow)]
		#[allow(dead_code)]
		struct Row<'a> {
			bytes: &'a [u8],
			stream: i64,
			combined_start: i64,
			combined_end: i64,
			stdout_start: i64,
			stdout_end: i64,
			stderr_start: i64,
			stderr_end: i64,
			timestamp: i64,
		}
		let params = (id, index.to_i64().unwrap());
		let result = self
			.session
			.execute_unpaged(&self.get_log_by_index_statement, params)
			.await?
			.into_rows_result()?;
		let Some(row) = result.maybe_first_row::<Row<'_>>()? else {
			return Ok(None);
		};
		let bytes = row.bytes.to_vec().into();
		let combined_position = row.combined_start.to_u64().unwrap();
		let (stream, stream_position) = if row.stream == 1 {
			(
				tg::process::log::Stream::Stdout,
				row.stdout_start.to_u64().unwrap(),
			)
		} else if row.stream == 2 {
			(
				tg::process::log::Stream::Stderr,
				row.stderr_start.to_u64().unwrap(),
			)
		} else {
			return Ok(None);
		};
		let timestamp = row.timestamp;
		Ok(Some(crate::log::Entry {
			bytes,
			combined_position,
			stream_position,
			stream,
			timestamp,
		}))
	}

	async fn try_get_num_log_entries(
		&self,
		id: &tg::process::Id,
	) -> Result<Option<u64>, Self::Error> {
		let id = id.to_bytes().to_vec();
		#[derive(scylla::DeserializeRow)]
		#[allow(dead_code)]
		struct Row<'a> {
			log_index: i64,
			bytes: &'a [u8],
			stream: i64,
			combined_start: i64,
			combined_end: i64,
			stdout_start: i64,
			stdout_end: i64,
			stderr_start: i64,
			stderr_end: i64,
			timestamp: i64,
		}
		let params = (id,);
		let result = self
			.session
			.execute_unpaged(&self.get_last_log_statement, params)
			.await?
			.into_rows_result()?;
		let Some(row) = result.maybe_first_row::<Row>()? else {
			return Ok(None);
		};
		Ok(Some(row.log_index.to_u64().unwrap()))
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

	async fn put_log(&self, arg: PutLogArg) -> Result<(), Self::Error> {
		let id = arg.process.to_bytes().to_vec();

		#[derive(scylla::DeserializeRow)]
		#[allow(dead_code)]
		struct LastRow<'a> {
			log_index: i64,
			bytes: &'a [u8],
			stream: i64,
			combined_start: i64,
			combined_end: i64,
			stdout_start: i64,
			stdout_end: i64,
			stderr_start: i64,
			stderr_end: i64,
			timestamp: i64,
		}

		#[derive(scylla::DeserializeRow)]
		#[allow(dead_code)]
		struct AppliedRow {
			applied: bool,
			id: Option<Vec<u8>>,
			log_index: Option<i64>,
			bytes: Option<Vec<u8>>,
			stream: Option<i64>,
			combined_start: Option<i64>,
			combined_end: Option<i64>,
			stdout_start: Option<i64>,
			stdout_end: Option<i64>,
			stderr_start: Option<i64>,
			stderr_end: Option<i64>,
			timestamp: Option<i64>,
		}

		loop {
			let params = (id.as_slice(),);
			let result = self
				.session
				.execute_unpaged(&self.get_last_log_statement, params)
				.await?
				.into_rows_result()?;

			let row = result.maybe_first_row::<LastRow>()?.unwrap_or(LastRow {
				log_index: 0,
				bytes: &[],
				stream: 0,
				combined_start: 0,
				combined_end: 0,
				stdout_start: 0,
				stdout_end: 0,
				stderr_start: 0,
				stderr_end: 0,
				timestamp: 0,
			});

			// Get the data for this entry.
			let index = row.log_index + 1;
			let bytes = arg.bytes.as_ref();
			let stream = match arg.stream {
				tg::process::log::Stream::Stdout => 1i64,
				tg::process::log::Stream::Stderr => 2i64,
			};

			// The new entry starts where the previous entry ended.
			let combined_start = row.combined_end;
			let combined_end = combined_start + bytes.len().to_i64().unwrap();

			// For stdout/stderr, only advance if the previous entry was from that stream.
			let (stdout_start, stdout_end) = if stream == 1 {
				(
					row.stdout_end,
					row.stdout_end + bytes.len().to_i64().unwrap(),
				)
			} else {
				(row.stdout_end, row.stdout_end)
			};
			let (stderr_start, stderr_end) = if stream == 2 {
				(
					row.stderr_end,
					row.stderr_end + bytes.len().to_i64().unwrap(),
				)
			} else {
				(row.stderr_end, row.stderr_end)
			};

			let timestamp = arg.timestamp;

			// Try to insert this row.
			let params = (
				id.clone(),
				index,
				bytes,
				stream,
				combined_start,
				combined_end,
				stdout_start,
				stdout_end,
				stderr_start,
				stderr_end,
				timestamp,
			);

			let result = self
				.session
				.execute_unpaged(&self.put_log_statement, params)
				.await?
				.into_rows_result()?;

			// If the row was inserted, exit the loop. Else try again.
			let result = result.single_row::<AppliedRow>()?;
			if result.applied {
				return Ok(());
			}
		}
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

	async fn delete_log(&self, arg: DeleteLogArg) -> Result<(), Self::Error> {
		// The delete_logs_statement deletes all logs for a process.
		// Stream-specific deletion is not currently supported.
		let id = arg.process.to_bytes().to_vec();
		let params = (id,);
		self.session
			.execute_unpaged(&self.delete_logs_statement, params)
			.await?;
		Ok(())
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

	async fn flush(&self) -> Result<(), Self::Error> {
		Ok(())
	}
}
