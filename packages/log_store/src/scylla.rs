use {
	crate::{DeleteProcessLogArg, PutProcessLogArg, ReadProcessLogArg},
	futures::{FutureExt as _, TryStreamExt as _},
	indoc::indoc,
	num::ToPrimitive as _,
	std::{borrow::Cow, collections::BTreeSet},
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

		// Process log statements.
		let statement = indoc!(
			"
				delete from process_log_entries
				where process = ?;
			"
		);
		let mut delete_process_log_entries_statement = session
			.prepare(statement)
			.await
			.map_err(|source| tg::error!(!source, "failed to prepare statement"))?;
		delete_process_log_entries_statement
			.set_consistency(scylla::statement::Consistency::LocalQuorum);

		let statement = indoc!(
			"
				delete from process_log_stream_positions
				where process = ?;
			"
		);
		let mut delete_process_log_stream_positions_statement = session
			.prepare(statement)
			.await
			.map_err(|source| tg::error!(!source, "failed to prepare statement"))?;
		delete_process_log_stream_positions_statement
			.set_consistency(scylla::statement::Consistency::LocalQuorum);

		let statement = indoc!(
			"
				select stream_position, bytes
				from process_log_entries
				where process = ? and position = ?;
			"
		);
		let mut get_combined_statement = session
			.prepare(statement)
			.await
			.map_err(|source| tg::error!(!source, "failed to prepare statement"))?;
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
		let mut get_last_combined_statement = session
			.prepare(statement)
			.await
			.map_err(|source| tg::error!(!source, "failed to prepare statement"))?;
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
		let mut get_last_stream_statement = session
			.prepare(statement)
			.await
			.map_err(|source| tg::error!(!source, "failed to prepare statement"))?;
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
		let mut get_stream_index_at_or_before_statement = session
			.prepare(statement)
			.await
			.map_err(|source| tg::error!(!source, "failed to prepare statement"))?;
		get_stream_index_at_or_before_statement
			.set_consistency(scylla::statement::Consistency::One);

		let statement = indoc!(
			"
				insert into process_log_entries (process, position, bytes, stream, stream_position, timestamp)
				values (?, ?, ?, ?, ?, ?)
				if not exists;
			"
		);
		let mut put_combined_statement = session
			.prepare(statement)
			.await
			.map_err(|source| tg::error!(!source, "failed to prepare statement"))?;
		put_combined_statement.set_consistency(scylla::statement::Consistency::LocalQuorum);

		let statement = indoc!(
			"
				insert into process_log_stream_positions (process, stream, stream_position, position)
				values (?, ?, ?, ?);
			"
		);
		let mut put_stream_statement = session
			.prepare(statement)
			.await
			.map_err(|source| tg::error!(!source, "failed to prepare statement"))?;
		put_stream_statement.set_consistency(scylla::statement::Consistency::LocalQuorum);

		let statement = indoc!(
			"
				select position, bytes, stream, stream_position, timestamp
				from process_log_entries
				where process = ? and position >= ?;
			"
		);
		let mut read_combined_statement = session
			.prepare(statement)
			.await
			.map_err(|source| tg::error!(!source, "failed to prepare statement"))?;
		read_combined_statement.set_consistency(scylla::statement::Consistency::One);

		let scylla = Self {
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
}

impl crate::Store for Store {
	async fn try_read_process_log(
		&self,
		arg: ReadProcessLogArg,
	) -> tg::Result<Vec<crate::ProcessLogEntry<'static>>> {
		if arg.streams.is_empty() {
			return Err(tg::error!("expected at least one log stream"));
		}
		if arg.streams.len() > 2 {
			return Err(tg::error!("invalid log streams"));
		}
		if arg.streams.contains(&tg::process::stdio::Stream::Stdin) {
			return Err(tg::error!("invalid stdio stream"));
		}
		let process = arg.process.to_bytes().to_vec();
		let combined = arg.streams.len() > 1;

		// Find the starting position.
		let start = if combined {
			Some(arg.position)
		} else {
			let stream = arg.streams.iter().next().copied().unwrap();
			let stream = match stream {
				tg::process::stdio::Stream::Stdout => 1i32,
				tg::process::stdio::Stream::Stderr => 2i32,
				tg::process::stdio::Stream::Stdin => {
					return Err(tg::error!("invalid stdio stream"));
				},
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
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the query"))?
				.into_rows_result()
				.map_err(|source| tg::error!(!source, "failed to get the rows"))?;
			result
				.maybe_first_row::<StreamRow>()
				.map_err(|source| tg::error!(!source, "failed to get the row"))?
				.map(|row| row.position.to_u64().unwrap())
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
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the query"))?
			.rows_stream::<Row>()
			.map_err(|source| tg::error!(!source, "failed to get the rows stream"))?;

		let mut remaining = arg.length;
		let mut output = Vec::new();
		let mut current: Option<crate::ProcessLogEntry> = None;

		while remaining > 0 {
			let Some(row) = iter
				.try_next()
				.await
				.map_err(|source| tg::error!(!source, "failed to get the next row"))?
			else {
				break;
			};

			// Determine the stream type.
			let chunk_stream = if row.stream == 1 {
				tg::process::stdio::Stream::Stdout
			} else {
				tg::process::stdio::Stream::Stderr
			};

			// Skip entries that do not match the stream filter.
			if !arg.streams.contains(&chunk_stream) {
				continue;
			}

			// Get the position based on stream filter.
			let position = if combined {
				row.position.to_u64().unwrap()
			} else {
				row.stream_position.to_u64().unwrap()
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
		streams: &BTreeSet<tg::process::stdio::Stream>,
	) -> tg::Result<Option<u64>> {
		if streams.is_empty() {
			return Err(tg::error!("expected at least one log stream"));
		}
		if streams.len() > 2 {
			return Err(tg::error!("invalid log streams"));
		}
		if streams.contains(&tg::process::stdio::Stream::Stdin) {
			return Err(tg::error!("invalid stdio stream"));
		}
		let process = id.to_bytes().to_vec();

		if streams.len() > 1 {
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
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the query"))?
				.into_rows_result()
				.map_err(|source| tg::error!(!source, "failed to get the rows"))?;
			let Some(row) = result
				.maybe_first_row::<Row>()
				.map_err(|source| tg::error!(!source, "failed to get the row"))?
			else {
				return Ok(None);
			};
			Ok(Some(
				row.position.to_u64().unwrap() + row.bytes.len().to_u64().unwrap(),
			))
		} else {
			let stream = streams.iter().next().copied().unwrap();
			// Stream length: get last stream index entry, then look up combined entry.
			let stream = match stream {
				tg::process::stdio::Stream::Stdout => 1i32,
				tg::process::stdio::Stream::Stderr => 2i32,
				tg::process::stdio::Stream::Stdin => {
					return Err(tg::error!("invalid stdio stream"));
				},
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
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the query"))?
				.into_rows_result()
				.map_err(|source| tg::error!(!source, "failed to get the rows"))?;
			let Some(index_row) = result
				.maybe_first_row::<StreamRow>()
				.map_err(|source| tg::error!(!source, "failed to get the row"))?
			else {
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
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the query"))?
				.into_rows_result()
				.map_err(|source| tg::error!(!source, "failed to get the rows"))?;
			let Some(entry_row) = result
				.maybe_first_row::<EntryRow>()
				.map_err(|source| tg::error!(!source, "failed to get the row"))?
			else {
				return Ok(None);
			};

			Ok(Some(
				entry_row.stream_position.to_u64().unwrap()
					+ entry_row.bytes.len().to_u64().unwrap(),
			))
		}
	}

	async fn put_process_log(&self, arg: PutProcessLogArg) -> tg::Result<()> {
		if arg.bytes.is_empty() {
			return Ok(());
		}

		let process = arg.process.to_bytes().to_vec();
		let stream = match arg.stream {
			tg::process::stdio::Stream::Stdout => 1i32,
			tg::process::stdio::Stream::Stderr => 2i32,
			tg::process::stdio::Stream::Stdin => {
				return Err(tg::error!("invalid stdio stream"));
			},
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
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the query"))?
				.into_rows_result()
				.map_err(|source| tg::error!(!source, "failed to get the rows"))?;
			let position = result
				.maybe_first_row::<LastEntryRow>()
				.map_err(|source| tg::error!(!source, "failed to get the row"))?
				.map_or(0, |row| {
					row.position.to_u64().unwrap() + row.bytes.len().to_u64().unwrap()
				});

			let params = (&process, stream);
			let result = self
				.session
				.execute_unpaged(&self.get_last_stream_statement, params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the query"))?
				.into_rows_result()
				.map_err(|source| tg::error!(!source, "failed to get the rows"))?;
			let stream_position = if let Some(index_row) = result
				.maybe_first_row::<LastStreamPositionRow>()
				.map_err(|source| tg::error!(!source, "failed to get the row"))?
			{
				let params = (&process, index_row.position);
				let result = self
					.session
					.execute_unpaged(&self.get_combined_statement, params)
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the query"))?
					.into_rows_result()
					.map_err(|source| tg::error!(!source, "failed to get the rows"))?;
				result
					.maybe_first_row::<EntryInfoRow>()
					.map_err(|source| tg::error!(!source, "failed to get the row"))?
					.map_or(0, |row| {
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
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the query"))?
				.into_rows_result()
				.map_err(|source| tg::error!(!source, "failed to get the rows"))?;

			let row = result
				.single_row::<Row>()
				.map_err(|source| tg::error!(!source, "failed to get the row"))?;
			if !row.applied {
				continue;
			}

			let params = (
				&process,
				stream,
				stream_position.to_i64().unwrap(),
				position.to_i64().unwrap(),
			);
			self.session
				.execute_unpaged(&self.put_stream_statement, params)
				.await
				.ok();

			return Ok(());
		}
	}

	async fn delete_process_log(&self, arg: DeleteProcessLogArg) -> tg::Result<()> {
		let process = arg.process.to_bytes().to_vec();
		let params = (&process,);
		self.session
			.execute_unpaged(&self.delete_process_log_entries_statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the query"))?;
		self.session
			.execute_unpaged(&self.delete_process_log_stream_positions_statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the query"))?;
		Ok(())
	}
}
