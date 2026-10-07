use {
	crate::Cache,
	bytes::Bytes,
	futures::TryStreamExt as _,
	indoc::indoc,
	scylla::value::MaybeUnset,
	std::{
		borrow::Cow,
		collections::{BTreeMap, BTreeSet},
	},
	tangram_cache::log,
	tangram_client::prelude::*,
};

const END_KIND: i8 = 3;
const ENTRY_KIND: i8 = 0;
const GET_BY_POSITIONS_BATCH_SIZE: usize = 128;

const STDERR_KIND: i8 = 2;
const STDOUT_KIND: i8 = 1;

pub(super) struct Statements {
	delete: scylla::statement::prepared::PreparedStatement,
	delete_cache_entry: scylla::statement::prepared::PreparedStatement,
	get_after: scylla::statement::prepared::PreparedStatement,
	get_at_or_before: scylla::statement::prepared::PreparedStatement,
	get_by_positions: scylla::statement::prepared::PreparedStatement,
	get_cache_entries: scylla::statement::prepared::PreparedStatement,
	get_end: scylla::statement::prepared::PreparedStatement,
	get_last: scylla::statement::prepared::PreparedStatement,
	put: scylla::statement::prepared::PreparedStatement,
	put_cache_entry: scylla::statement::prepared::PreparedStatement,
	put_end: scylla::statement::prepared::PreparedStatement,
}

#[derive(Clone, Debug)]
struct Record {
	bytes: Option<Bytes>,
	combined_position: u64,
	length: u64,
	position: u64,
	stream: tg::process::stdio::Stream,
	stream_position: u64,
	timestamp: i64,
}

#[derive(scylla::DeserializeRow)]
struct RecordRow {
	bytes: Option<Vec<u8>>,
	combined_position: i64,
	length: i32,
	position: i64,
	stream: i8,
	stream_position: i64,
	timestamp: i64,
}

#[derive(scylla::DeserializeRow)]
struct BytesRow {
	bytes: Option<Vec<u8>>,
	position: i64,
}

#[derive(scylla::DeserializeRow)]
struct LengthRow {
	length: i32,
	position: i64,
}

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

	pub(crate) async fn delete_log_inner(&self, arg: log::delete::Arg) -> tg::Result<()> {
		let process = arg.process.to_bytes().to_vec();
		self.session
			.execute_unpaged(&self.statements.log.delete, (process,))
			.await
			.map_err(
				|error| tg::error!(!error, process = %arg.process, "failed to execute the delete query"),
			)?;

		Ok(())
	}

	pub(crate) async fn put_log_inner(&self, arg: log::put::Arg) -> tg::Result<()> {
		self.put_log_batch_inner(vec![arg]).await
	}

	pub(crate) async fn put_log_batch_inner(&self, mut args: Vec<log::put::Arg>) -> tg::Result<()> {
		args.retain(|arg| !arg.bytes.is_empty());
		let Some(process) = args.first().map(|arg| arg.process.clone()) else {
			return Ok(());
		};
		if args.iter().any(|arg| arg.process != process) {
			return Err(tg::error!("expected log entries for one process"));
		}

		let mut batch =
			scylla::statement::batch::Batch::new(scylla::statement::batch::BatchType::Unlogged);
		batch.set_consistency(scylla::statement::Consistency::LocalQuorum);
		batch.set_is_idempotent(true);
		let mut values = Vec::with_capacity(args.len() * 2);
		for arg in args {
			let combined_position = i64::try_from(arg.position)
				.map_err(|_| tg::error!("the combined position is too large"))?;
			let kind = crate::log::kind_for_stream(arg.stream)?;
			let length = i32::try_from(arg.bytes.len())
				.map_err(|_| tg::error!("the log entry is too large"))?;
			let process = arg.process.to_bytes().to_vec();
			let stream_position = i64::try_from(arg.stream_position)
				.map_err(|_| tg::error!("the stream position is too large"))?;
			batch.append_statement(scylla::statement::batch::BatchStatement::PreparedStatement(
				self.statements.log.put.clone(),
			));
			values.push((
				MaybeUnset::Set(arg.bytes),
				combined_position,
				crate::log::ENTRY_KIND,
				length,
				combined_position,
				process.clone(),
				kind,
				stream_position,
				arg.timestamp,
			));
			batch.append_statement(scylla::statement::batch::BatchStatement::PreparedStatement(
				self.statements.log.put.clone(),
			));
			values.push((
				MaybeUnset::Unset,
				combined_position,
				kind,
				length,
				stream_position,
				process,
				kind,
				stream_position,
				arg.timestamp,
			));
		}
		self.session
			.batch(&batch, values)
			.await
			.map_err(|error| tg::error!(!error, %process, "failed to execute the put batch"))?;

		Ok(())
	}

	pub(crate) async fn put_log_end_inner(&self, arg: log::end::Arg) -> tg::Result<()> {
		let process = arg.process.to_bytes().to_vec();
		let position = i64::try_from(arg.end.position)
			.map_err(|_| tg::error!("the log position is too large"))?;
		let bytes = tangram_serialize::to_vec(&arg.end)
			.map_err(|error| tg::error!(!error, "failed to serialize the log end"))?;
		self.session
			.execute_unpaged(
				&self.statements.log.put_end,
				(process, crate::log::END_KIND, position, bytes),
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to cache the log end"))?;
		Ok(())
	}

	pub(crate) async fn try_get_log_end_inner(
		&self,
		process: &tg::process::Id,
	) -> tg::Result<Option<tg::process::log::End>> {
		let process = process.to_bytes().to_vec();
		let result = self
			.session
			.execute_unpaged(
				&self.statements.log.get_end,
				(process, crate::log::END_KIND),
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the log end"))?;
		let rows = result
			.into_rows_result()
			.map_err(|error| tg::error!(!error, "failed to get the log end rows"))?;
		let row = rows
			.maybe_first_row::<(Vec<u8>,)>()
			.map_err(|error| tg::error!(!error, "failed to read the log end row"))?;
		let output = row
			.map(|(bytes,)| tangram_serialize::from_slice(&bytes))
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to deserialize the log end"))?;
		Ok(output)
	}

	pub(crate) async fn try_get_log_length_inner(
		&self,
		arg: log::length::Arg,
	) -> tg::Result<Option<u64>> {
		let kind = crate::log::kind_for_streams(&arg.streams)?;
		let process_bytes = arg.process.to_bytes().to_vec();
		let result = self
			.session
			.execute_unpaged(&self.statements.log.get_last, (process_bytes, kind))
			.await
			.map_err(
				|error| tg::error!(!error, process = %arg.process, "failed to execute the get last query"),
			)?
			.into_rows_result()
			.map_err(
				|error| tg::error!(!error, process = %arg.process, "failed to get the log rows"),
			)?;
		let Some(row) = result.maybe_first_row::<LengthRow>().map_err(
			|error| tg::error!(!error, process = %arg.process, "failed to get the log row"),
		)?
		else {
			return Ok(None);
		};
		let position = u64::try_from(row.position)
			.map_err(|_| tg::error!(process = %arg.process, "the log position is invalid"))?;
		let entry_length = u64::try_from(row.length)
			.map_err(|_| tg::error!(process = %arg.process, "the log length is invalid"))?;
		let length = position
			.checked_add(entry_length)
			.ok_or_else(|| tg::error!("the log length is too large"))?;

		Ok(Some(length))
	}

	pub(crate) async fn try_read_log_inner(
		&self,
		arg: log::read::Arg,
	) -> tg::Result<Vec<log::read::Entry<'static>>> {
		if arg.length == 0 {
			return Ok(Vec::new());
		}

		let kind = crate::log::kind_for_streams(&arg.streams)?;
		let process = arg.process.to_bytes().to_vec();
		let position =
			i64::try_from(arg.position).map_err(|_| tg::error!("the log position is too large"))?;
		let Some(record) = self
			.try_get_record_at_or_before(&arg.process, &process, kind, position)
			.await?
		else {
			return Ok(Vec::new());
		};
		let start_position = record.position;
		let mut covered = record
			.length
			.saturating_sub(arg.position.saturating_sub(record.position));
		let mut records = vec![record];

		if covered < arg.length {
			let start_position = i64::try_from(start_position)
				.map_err(|_| tg::error!("the log position is too large"))?;
			let pager = self
				.session
				.execute_iter(
					self.statements.log.get_after.clone(),
					(process.clone(), kind, start_position),
				)
				.await
				.map_err(
					|error| tg::error!(!error, process = %arg.process, "failed to execute the get after query"),
				)?;
			let mut rows = pager.rows_stream::<RecordRow>().map_err(
				|error| tg::error!(!error, process = %arg.process, "failed to get the log rows"),
			)?;
			while let Some(row) = rows.try_next().await.map_err(
				|error| tg::error!(!error, process = %arg.process, "failed to read a log row"),
			)? {
				let record = Record::try_from(row)?;
				if record.position > arg.position.saturating_add(covered) {
					break;
				}
				covered = covered.saturating_add(record.length);
				records.push(record);
				if covered >= arg.length {
					break;
				}
			}
		}

		self.get_missing_bytes(&arg.process, &process, kind, &mut records)
			.await?;

		let mut builder = log::read::Builder::new(&arg);
		for record in records {
			if !arg.streams.contains(&record.stream) {
				return Err(tg::error!(stream = %record.stream, "invalid log stream"));
			}
			let bytes = record
				.bytes
				.ok_or_else(|| tg::error!("the log payload is missing"))?;
			if bytes.len() != usize::try_from(record.length).unwrap() {
				return Err(tg::error!("the log payload length is invalid"));
			}
			let entry = log::read::Entry {
				bytes: Cow::Borrowed(&bytes),
				position: record.combined_position,
				stream: record.stream,
				stream_position: record.stream_position,
				timestamp: record.timestamp,
			};
			if !builder.push(&entry) {
				break;
			}
		}
		let output = builder.finish();

		Ok(output)
	}

	async fn get_missing_bytes(
		&self,
		process: &tg::process::Id,
		process_bytes: &[u8],
		kind: i8,
		records: &mut [Record],
	) -> tg::Result<()> {
		let mut positions = BTreeMap::<i8, BTreeSet<i64>>::new();
		for record in records.iter().filter(|record| record.bytes.is_none()) {
			let (kind, position) = counterpart(kind, record)?;
			positions.entry(kind).or_default().insert(position);
		}

		let mut bytes = BTreeMap::new();
		for (kind, positions) in positions {
			let positions = positions.into_iter().collect::<Vec<_>>();
			for positions in positions.chunks(GET_BY_POSITIONS_BATCH_SIZE) {
				let result = self
					.session
					.execute_unpaged(
						&self.statements.log.get_by_positions,
						(process_bytes, kind, positions),
					)
					.await
					.map_err(
						|error| tg::error!(!error, %process, "failed to execute the get by positions query"),
					)?
					.into_rows_result()
					.map_err(|error| tg::error!(!error, %process, "failed to get the log rows"))?;
				for row in result.rows::<BytesRow>().map_err(
					|error| tg::error!(!error, %process, "failed to iterate the log rows"),
				)? {
					let row = row.map_err(
						|error| tg::error!(!error, %process, "failed to get the log row"),
					)?;
					if let Some(value) = row.bytes {
						bytes.insert((kind, row.position), Bytes::from(value));
					}
				}
			}
		}

		for record in records.iter_mut().filter(|record| record.bytes.is_none()) {
			let key = counterpart(kind, record)?;
			let value = bytes
				.get(&key)
				.cloned()
				.ok_or_else(|| tg::error!(%process, "the counterpart log payload is missing"))?;
			record.bytes.replace(value);
		}

		Ok(())
	}

	async fn try_get_record_at_or_before(
		&self,
		process: &tg::process::Id,
		process_bytes: &[u8],
		kind: i8,
		position: i64,
	) -> tg::Result<Option<Record>> {
		let result = self
			.session
			.execute_unpaged(
				&self.statements.log.get_at_or_before,
				(process_bytes, kind, position),
			)
			.await
			.map_err(
				|error| tg::error!(!error, %process, "failed to execute the get at or before query"),
			)?
			.into_rows_result()
			.map_err(|error| tg::error!(!error, %process, "failed to get the log rows"))?;
		let row = result
			.maybe_first_row::<RecordRow>()
			.map_err(|error| tg::error!(!error, %process, "failed to get the log row"))?;
		let record = row.map(Record::try_from).transpose()?;

		Ok(record)
	}
}

impl Statements {
	pub(super) async fn new(session: &scylla::client::session::Session) -> tg::Result<Self> {
		let mut delete_cache_entry = session
			.prepare(
				"delete from log_cache where partition = ? and expires_at = ? and process = ?;",
			)
			.await
			.map_err(|error| {
				tg::error!(!error, "failed to prepare the log cache delete statement")
			})?;
		let mut get_cache_entries = session
			.prepare(
				"select expires_at, process from log_cache where partition = ? and expires_at <= ? limit ?;",
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to prepare the log cache get statement"))?;
		let mut put_cache_entry = session
			.prepare("insert into log_cache (partition, expires_at, process) values (?, ?, ?);")
			.await
			.map_err(|error| tg::error!(!error, "failed to prepare the log cache put statement"))?;
		for statement in [
			&mut delete_cache_entry,
			&mut get_cache_entries,
			&mut put_cache_entry,
		] {
			statement.set_consistency(scylla::statement::Consistency::LocalQuorum);
			statement.set_is_idempotent(true);
		}

		let statement = indoc!(
			"
				delete from logs
				where process = ?;
			"
		);
		let mut delete = session
			.prepare(statement)
			.await
			.map_err(|error| tg::error!(!error, "failed to prepare the log delete statement"))?;
		delete.set_consistency(scylla::statement::Consistency::LocalQuorum);
		delete.set_is_idempotent(true);

		let statement = indoc!(
			"
				select bytes, combined_position, length, position, stream, stream_position, \"timestamp\"
				from logs
				where process = ? and kind = ? and position <= ?
				order by position desc
				limit 1;
			"
		);
		let mut get_at_or_before = session.prepare(statement).await.map_err(|error| {
			tg::error!(
				!error,
				"failed to prepare the log get at or before statement"
			)
		})?;
		get_at_or_before.set_consistency(scylla::statement::Consistency::LocalQuorum);
		get_at_or_before.set_is_idempotent(true);

		let statement = indoc!(
			"
				select bytes, position
				from logs
				where process = ? and kind = ? and position in ?;
			"
		);
		let mut get_by_positions = session.prepare(statement).await.map_err(|error| {
			tg::error!(
				!error,
				"failed to prepare the log get by positions statement"
			)
		})?;
		get_by_positions.set_consistency(scylla::statement::Consistency::LocalQuorum);
		get_by_positions.set_is_idempotent(true);

		let statement = indoc!(
			"
				select length, position
				from logs
				where process = ? and kind = ?
				order by position desc
				limit 1;
			"
		);
		let mut get_last = session
			.prepare(statement)
			.await
			.map_err(|error| tg::error!(!error, "failed to prepare the log get last statement"))?;
		get_last.set_consistency(scylla::statement::Consistency::LocalQuorum);
		get_last.set_is_idempotent(true);

		let statement = indoc!(
			"
				select bytes, combined_position, length, position, stream, stream_position, \"timestamp\"
				from logs
				where process = ? and kind = ? and position > ?;
			"
		);
		let mut get_after = session
			.prepare(statement)
			.await
			.map_err(|error| tg::error!(!error, "failed to prepare the log get after statement"))?;
		get_after.set_consistency(scylla::statement::Consistency::LocalQuorum);
		get_after.set_is_idempotent(true);
		get_after.set_page_size(128);

		let statement = indoc!(
			"
				insert into logs (
					bytes, combined_position, kind, length, position, process, stream,
					stream_position, \"timestamp\"
				)
				values (?, ?, ?, ?, ?, ?, ?, ?, ?);
			"
		);
		let mut put = session
			.prepare(statement)
			.await
			.map_err(|error| tg::error!(!error, "failed to prepare the log entry statement"))?;
		put.set_consistency(scylla::statement::Consistency::LocalQuorum);
		put.set_is_idempotent(true);
		let mut get_end = session
			.prepare(
				"select bytes from logs where process = ? and kind = ? order by position desc limit 1;",
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to prepare the log end read statement"))?;
		get_end.set_consistency(scylla::statement::Consistency::LocalQuorum);
		get_end.set_is_idempotent(true);
		let mut put_end = session
			.prepare("insert into logs (process, kind, position, bytes) values (?, ?, ?, ?);")
			.await
			.map_err(|error| tg::error!(!error, "failed to prepare the log end write statement"))?;
		put_end.set_consistency(scylla::statement::Consistency::LocalQuorum);
		put_end.set_is_idempotent(true);
		let statements = Self {
			delete,
			delete_cache_entry,
			get_after,
			get_at_or_before,
			get_by_positions,
			get_cache_entries,
			get_end,
			get_last,
			put,
			put_cache_entry,
			put_end,
		};

		Ok(statements)
	}
}

impl TryFrom<RecordRow> for Record {
	type Error = tg::Error;

	fn try_from(row: RecordRow) -> Result<Self, Self::Error> {
		let bytes = row.bytes.map(Bytes::from);
		let combined_position = row
			.combined_position
			.try_into()
			.map_err(|_| tg::error!("the combined position is invalid"))?;
		let length = row
			.length
			.try_into()
			.map_err(|_| tg::error!("the log length is invalid"))?;
		let position = row
			.position
			.try_into()
			.map_err(|_| tg::error!("the log position is invalid"))?;
		let stream = crate::log::stream_for_kind(row.stream)?;
		let stream_position = row
			.stream_position
			.try_into()
			.map_err(|_| tg::error!("the stream position is invalid"))?;
		let timestamp = row.timestamp;
		let record = Self {
			bytes,
			combined_position,
			length,
			position,
			stream,
			stream_position,
			timestamp,
		};

		Ok(record)
	}
}

impl tangram_cache::log::Cache for Cache {
	async fn delete_log_cache_entry(
		&self,
		arg: tangram_cache::log::cache::delete::Arg,
	) -> tg::Result<()> {
		self.delete_log_cache_entry(arg).await
	}

	async fn get_log_cache_entries(
		&self,
		arg: tangram_cache::log::cache::get::Arg,
	) -> tg::Result<Vec<tangram_cache::log::cache::Entry>> {
		self.get_log_cache_entries(arg).await
	}

	async fn put_log_cache_entry(
		&self,
		arg: tangram_cache::log::cache::put::Arg,
	) -> tg::Result<()> {
		self.put_log_cache_entry(arg).await
	}

	async fn delete_log(&self, arg: tangram_cache::log::delete::Arg) -> tg::Result<()> {
		self.delete_log_inner(arg).await
	}

	async fn put_log(&self, arg: tangram_cache::log::put::Arg) -> tg::Result<()> {
		self.put_log_inner(arg).await
	}

	async fn put_log_batch(&self, args: Vec<tangram_cache::log::put::Arg>) -> tg::Result<()> {
		self.put_log_batch_inner(args).await
	}

	async fn put_log_end(&self, arg: tangram_cache::log::end::Arg) -> tg::Result<()> {
		self.put_log_end_inner(arg).await
	}

	async fn try_get_log_end(
		&self,
		process: &tg::process::Id,
	) -> tg::Result<Option<tg::process::log::End>> {
		self.try_get_log_end_inner(process).await
	}

	async fn try_get_log_length(
		&self,
		arg: tangram_cache::log::length::Arg,
	) -> tg::Result<Option<u64>> {
		self.try_get_log_length_inner(arg).await
	}

	async fn try_read_log(
		&self,
		arg: tangram_cache::log::read::Arg,
	) -> tg::Result<Vec<tangram_cache::log::read::Entry<'static>>> {
		self.try_read_log_inner(arg).await
	}
}

fn counterpart(kind: i8, record: &Record) -> tg::Result<(i8, i64)> {
	let key = if kind == crate::log::ENTRY_KIND {
		let kind = crate::log::kind_for_stream(record.stream)?;
		let position = i64::try_from(record.stream_position)
			.map_err(|_| tg::error!("the stream position is too large"))?;
		(kind, position)
	} else {
		let position = i64::try_from(record.combined_position)
			.map_err(|_| tg::error!("the combined position is too large"))?;
		(crate::log::ENTRY_KIND, position)
	};

	Ok(key)
}

fn kind_for_stream(stream: tg::process::stdio::Stream) -> tg::Result<i8> {
	let kind = match stream {
		tg::process::stdio::Stream::Stderr => STDERR_KIND,
		tg::process::stdio::Stream::Stdin => {
			return Err(tg::error!("invalid stdio stream"));
		},
		tg::process::stdio::Stream::Stdout => STDOUT_KIND,
	};

	Ok(kind)
}

fn kind_for_streams(streams: &BTreeSet<tg::process::stdio::Stream>) -> tg::Result<i8> {
	if streams.is_empty() || streams.len() > 2 {
		return Err(tg::error!("invalid log streams"));
	}
	if streams.contains(&tg::process::stdio::Stream::Stdin) {
		return Err(tg::error!("invalid stdio stream"));
	}
	if streams.len() == 2 {
		return Ok(ENTRY_KIND);
	}
	let stream = streams.iter().next().copied().unwrap();

	kind_for_stream(stream)
}

fn stream_for_kind(kind: i8) -> tg::Result<tg::process::stdio::Stream> {
	let stream = match kind {
		STDERR_KIND => tg::process::stdio::Stream::Stderr,
		STDOUT_KIND => tg::process::stdio::Stream::Stdout,
		_ => return Err(tg::error!(%kind, "invalid log kind")),
	};

	Ok(stream)
}
