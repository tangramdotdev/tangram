use {
	super::Store,
	crate::{Entry, ReadArg},
	bytes::Bytes,
	futures::TryStreamExt as _,
	std::{
		borrow::Cow,
		collections::{BTreeMap, BTreeSet},
	},
	tangram_client::prelude::*,
};

const GET_BY_POSITIONS_BATCH_SIZE: usize = 128;

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

impl Store {
	pub(super) async fn try_read(&self, arg: ReadArg) -> tg::Result<Vec<Entry<'static>>> {
		if arg.length == 0 {
			return Ok(Vec::new());
		}

		// Get the first record.
		let kind = super::kind_for_streams(&arg.streams)?;
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

		// Get the following records until the requested length is covered.
		if covered < arg.length {
			let start_position = i64::try_from(start_position)
				.map_err(|_| tg::error!("the log position is too large"))?;
			let pager = self
				.session
				.execute_iter(
					self.statements.get_after.clone(),
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
				covered = covered.saturating_add(record.length);
				records.push(record);
				if covered >= arg.length {
					break;
				}
			}
		}

		// Fetch payloads stored in the counterpart records.
		self.get_missing_bytes(&arg.process, &process, kind, &mut records)
			.await?;

		// Create the entries.
		let mut current: Option<Entry<'static>> = None;
		let mut output = Vec::new();
		let mut remaining = arg.length;
		for record in records {
			if remaining == 0 {
				break;
			}
			if !arg.streams.contains(&record.stream) {
				return Err(tg::error!(stream = %record.stream, "invalid log stream"));
			}
			let bytes = record
				.bytes
				.ok_or_else(|| tg::error!("the log payload is missing"))?;
			if bytes.len() != usize::try_from(record.length).unwrap() {
				return Err(tg::error!("the log payload length is invalid"));
			}
			let offset = arg.position.saturating_sub(record.position);
			let available = record.length.saturating_sub(offset);
			let take = remaining.min(available);
			if take == 0 {
				continue;
			}
			let start = usize::try_from(offset).unwrap();
			let end = usize::try_from(offset + take).unwrap();
			let bytes = Cow::Owned(bytes[start..end].to_vec());
			if let Some(entry) = &mut current {
				if entry.stream == record.stream {
					let mut combined = entry.bytes.to_vec();
					combined.extend_from_slice(&bytes);
					entry.bytes = Cow::Owned(combined);
				} else {
					output.push(current.take().unwrap());
					current = Some(Entry {
						bytes,
						position: record.combined_position + offset,
						stream: record.stream,
						stream_position: record.stream_position + offset,
						timestamp: record.timestamp,
					});
				}
			} else {
				current = Some(Entry {
					bytes,
					position: record.combined_position + offset,
					stream: record.stream,
					stream_position: record.stream_position + offset,
					timestamp: record.timestamp,
				});
			}
			remaining -= take;
		}
		if let Some(entry) = current {
			output.push(entry);
		}

		Ok(output)
	}

	pub(super) async fn try_get_length(
		&self,
		process: &tg::process::Id,
		streams: &BTreeSet<tg::process::stdio::Stream>,
	) -> tg::Result<Option<u64>> {
		let kind = super::kind_for_streams(streams)?;
		let process_bytes = process.to_bytes().to_vec();
		let result = self
			.session
			.execute_unpaged(&self.statements.get_last, (process_bytes, kind))
			.await
			.map_err(|error| tg::error!(!error, %process, "failed to execute the get last query"))?
			.into_rows_result()
			.map_err(|error| tg::error!(!error, %process, "failed to get the log rows"))?;
		let Some(row) = result
			.maybe_first_row::<LengthRow>()
			.map_err(|error| tg::error!(!error, %process, "failed to get the log row"))?
		else {
			return Ok(None);
		};
		let position = u64::try_from(row.position)
			.map_err(|_| tg::error!(%process, "the log position is invalid"))?;
		let entry_length = u64::try_from(row.length)
			.map_err(|_| tg::error!(%process, "the log length is invalid"))?;
		let length = position
			.checked_add(entry_length)
			.ok_or_else(|| tg::error!("the log length is too large"))?;

		Ok(Some(length))
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
				&self.statements.get_at_or_before,
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

	async fn get_missing_bytes(
		&self,
		process: &tg::process::Id,
		process_bytes: &[u8],
		kind: i8,
		records: &mut [Record],
	) -> tg::Result<()> {
		// Collect the counterpart positions by kind.
		let mut positions = BTreeMap::<i8, BTreeSet<i64>>::new();
		for record in records.iter().filter(|record| record.bytes.is_none()) {
			let (kind, position) = counterpart(kind, record)?;
			positions.entry(kind).or_default().insert(position);
		}

		// Fetch the counterpart payloads.
		let mut bytes = BTreeMap::new();
		for (kind, positions) in positions {
			let positions = positions.into_iter().collect::<Vec<_>>();
			for positions in positions.chunks(GET_BY_POSITIONS_BATCH_SIZE) {
				let result = self
					.session
					.execute_unpaged(
						&self.statements.get_by_positions,
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

		// Fill the missing payloads.
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
		let stream = super::stream_for_kind(row.stream)?;
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

fn counterpart(kind: i8, record: &Record) -> tg::Result<(i8, i64)> {
	let key = if kind == super::ENTRY_KIND {
		let kind = super::kind_for_stream(record.stream)?;
		let position = i64::try_from(record.stream_position)
			.map_err(|_| tg::error!("the stream position is too large"))?;
		(kind, position)
	} else {
		let position = i64::try_from(record.combined_position)
			.map_err(|_| tg::error!("the combined position is too large"))?;
		(super::ENTRY_KIND, position)
	};

	Ok(key)
}
