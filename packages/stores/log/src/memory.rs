use {
	crate::{DeleteProcessLogArg, ProcessLogEntry, PutProcessLogArg, ReadProcessLogArg},
	dashmap::DashMap,
	num::ToPrimitive as _,
	std::{
		borrow::Cow,
		collections::{BTreeMap, BTreeSet},
	},
	tangram_client::prelude::*,
};

pub struct Store {
	process_logs: DashMap<tg::process::Id, ProcessLogs, tg::id::BuildHasher>,
}

#[derive(Default)]
struct ProcessLogs {
	entries: BTreeMap<u64, ProcessLogEntry<'static>>,
	stream_positions: BTreeMap<(tg::process::stdio::Stream, u64), u64>,
}

impl Store {
	#[must_use]
	pub fn new() -> Self {
		Self {
			process_logs: DashMap::default(),
		}
	}

	#[must_use]
	#[expect(clippy::needless_pass_by_value)]
	fn try_read_process_log(&self, arg: ReadProcessLogArg) -> Vec<ProcessLogEntry<'static>> {
		let Some(logs) = self.process_logs.get(&arg.process) else {
			return Vec::new();
		};
		if arg.streams.is_empty() {
			return Vec::new();
		}
		let combined = arg.streams.len() > 1;

		// Find the starting position.
		let start_position = if combined {
			arg.position
		} else {
			let Some(stream) = arg.streams.iter().next().copied() else {
				return Vec::new();
			};
			let pointer = logs
				.stream_positions
				.range(..=(stream, arg.position))
				.next_back()
				.filter(|((s, _), _)| *s == stream)
				.map(|(_, &v)| v);
			let Some(position) = pointer else {
				return Vec::new();
			};
			position
		};

		// Collect all entries starting from the position.
		let entries: Vec<_> = logs
			.entries
			.range(start_position..)
			.map(|(_, v)| v.clone())
			.collect();

		let mut remaining = arg.length;
		let mut output = Vec::new();
		let mut current: Option<ProcessLogEntry<'static>> = None;

		for chunk in entries {
			if remaining == 0 {
				break;
			}

			// Skip chunks that do not match the stream filter.
			if !arg.streams.contains(&chunk.stream) {
				continue;
			}

			// Get the position based on the stream filter.
			let position = if combined {
				chunk.position
			} else {
				chunk.stream_position
			};

			let offset = arg.position.saturating_sub(position);

			let available = chunk.bytes.len().to_u64().unwrap().saturating_sub(offset);
			let take = remaining.min(available);

			if take == 0 {
				continue;
			}

			let start = offset.to_usize().unwrap();
			let end = (offset + take).to_usize().unwrap();
			let bytes: Cow<'static, [u8]> = Cow::Owned(chunk.bytes[start..end].to_vec());

			// Combine sequential entries from the same stream.
			if let Some(ref mut entry) = current {
				if entry.stream == chunk.stream {
					let mut combined = entry.bytes.to_vec();
					combined.extend_from_slice(&bytes);
					entry.bytes = Cow::Owned(combined);
				} else {
					output.push(current.take().unwrap());
					current = Some(ProcessLogEntry {
						bytes,
						position: chunk.position + offset,
						stream_position: chunk.stream_position + offset,
						stream: chunk.stream,
						timestamp: chunk.timestamp,
					});
				}
			} else {
				current = Some(ProcessLogEntry {
					bytes,
					position: chunk.position + offset,
					stream_position: chunk.stream_position + offset,
					stream: chunk.stream,
					timestamp: chunk.timestamp,
				});
			}

			remaining -= take;
		}

		// Push the last entry if any.
		if let Some(entry) = current {
			output.push(entry);
		}

		output
	}

	#[must_use]
	pub fn try_get_process_log_length(
		&self,
		id: &tg::process::Id,
		streams: &BTreeSet<tg::process::stdio::Stream>,
	) -> Option<u64> {
		if streams.is_empty() {
			return None;
		}
		let process_logs = self.process_logs.get(id)?;
		if streams.len() == 1 {
			let stream = streams.iter().next().copied()?;
			let last_position = process_logs
				.stream_positions
				.range((stream, 0)..(stream, u64::MAX))
				.next_back()
				.map(|(_, &v)| v)?;
			let entry = process_logs.entries.get(&last_position)?;
			Some(entry.stream_position + entry.bytes.len().to_u64().unwrap())
		} else {
			let entry = process_logs.entries.values().next_back()?;
			Some(entry.position + entry.bytes.len().to_u64().unwrap())
		}
	}

	pub fn put_process_log(&self, arg: PutProcessLogArg) {
		if arg.bytes.is_empty() {
			return;
		}

		let mut logs = self.process_logs.entry(arg.process).or_default();

		// Get the current position.
		let position = logs.entries.values().next_back().map_or(0, |entry| {
			entry.position + entry.bytes.len().to_u64().unwrap()
		});

		// Get the current stream position.
		let stream_position = logs
			.stream_positions
			.range((arg.stream, 0)..(arg.stream, u64::MAX))
			.next_back()
			.and_then(|(_, &position)| logs.entries.get(&position))
			.map_or(0, |entry| {
				entry.stream_position + entry.bytes.len().to_u64().unwrap()
			});

		// Create the entry.
		let entry = ProcessLogEntry {
			bytes: Cow::Owned(arg.bytes.to_vec()),
			position,
			stream_position,
			stream: arg.stream,
			timestamp: arg.timestamp,
		};

		// Store the entry.
		logs.entries.insert(position, entry);

		// Store the stream position pointer.
		logs.stream_positions
			.insert((arg.stream, stream_position), position);
	}

	pub fn put_process_log_batch(&self, args: Vec<PutProcessLogArg>) {
		for arg in args {
			self.put_process_log(arg);
		}
	}

	#[expect(clippy::needless_pass_by_value)]
	pub fn delete_process_log(&self, arg: DeleteProcessLogArg) {
		self.process_logs.remove(&arg.process);
	}

	pub fn delete_process_log_batch(&self, args: Vec<DeleteProcessLogArg>) {
		for arg in args {
			self.delete_process_log(arg);
		}
	}
}

impl Default for Store {
	fn default() -> Self {
		Self::new()
	}
}

impl crate::Store for Store {
	async fn try_read_process_log(
		&self,
		arg: ReadProcessLogArg,
	) -> tg::Result<Vec<ProcessLogEntry<'static>>> {
		Ok(self.try_read_process_log(arg))
	}

	async fn try_get_process_log_length(
		&self,
		id: &tg::process::Id,
		streams: &BTreeSet<tg::process::stdio::Stream>,
	) -> tg::Result<Option<u64>> {
		Ok(self.try_get_process_log_length(id, streams))
	}

	async fn put_process_log(&self, arg: PutProcessLogArg) -> tg::Result<()> {
		self.put_process_log(arg);
		Ok(())
	}

	async fn delete_process_log(&self, arg: DeleteProcessLogArg) -> tg::Result<()> {
		self.delete_process_log(arg);
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use bytes::Bytes;

	fn collect_bytes(entries: Vec<ProcessLogEntry>) -> Bytes {
		entries
			.into_iter()
			.flat_map(|entry| entry.bytes.to_vec())
			.collect::<Vec<_>>()
			.into()
	}

	#[test]
	fn test_put_and_read_log_single_chunk() {
		let store = Store::new();
		let process = tg::process::Id::new();

		// Insert a single chunk.
		store.put_process_log(PutProcessLogArg {
			bytes: Bytes::from("hello world"),
			process: process.clone(),
			stream: tg::process::stdio::Stream::Stdout,
			timestamp: 1000,
		});

		// Read the entire chunk.
		let result = store.try_read_process_log(ReadProcessLogArg {
			process: process.clone(),
			position: 0,
			length: 11,
			streams: BTreeSet::from([tg::process::stdio::Stream::Stdout]),
		});
		assert_eq!(collect_bytes(result), Bytes::from("hello world"));

		// Read a subset of the chunk.
		let result = store.try_read_process_log(ReadProcessLogArg {
			process: process.clone(),
			position: 6,
			length: 5,
			streams: BTreeSet::from([tg::process::stdio::Stream::Stdout]),
		});
		assert_eq!(collect_bytes(result), Bytes::from("world"));
	}

	#[test]
	fn test_put_and_read_log_multiple_chunks() {
		let store = Store::new();
		let process = tg::process::Id::new();

		// Insert multiple chunks.
		store.put_process_log(PutProcessLogArg {
			bytes: Bytes::from("hello"),
			process: process.clone(),
			stream: tg::process::stdio::Stream::Stdout,
			timestamp: 1000,
		});
		store.put_process_log(PutProcessLogArg {
			bytes: Bytes::from(" "),
			process: process.clone(),
			stream: tg::process::stdio::Stream::Stdout,
			timestamp: 1001,
		});
		store.put_process_log(PutProcessLogArg {
			bytes: Bytes::from("world"),
			process: process.clone(),
			stream: tg::process::stdio::Stream::Stdout,
			timestamp: 1002,
		});

		// Read across all chunks.
		let result = store.try_read_process_log(ReadProcessLogArg {
			process: process.clone(),
			position: 0,
			length: 11,
			streams: BTreeSet::from([tg::process::stdio::Stream::Stdout]),
		});
		assert_eq!(collect_bytes(result), Bytes::from("hello world"));
	}

	#[test]
	fn test_read_log_across_chunk_boundaries() {
		let store = Store::new();
		let process = tg::process::Id::new();

		// Insert chunks: "AAAA" (0-3), "BBBB" (4-7), "CCCC" (8-11).
		store.put_process_log(PutProcessLogArg {
			bytes: Bytes::from("AAAA"),
			process: process.clone(),
			stream: tg::process::stdio::Stream::Stdout,
			timestamp: 1000,
		});
		store.put_process_log(PutProcessLogArg {
			bytes: Bytes::from("BBBB"),
			process: process.clone(),
			stream: tg::process::stdio::Stream::Stdout,
			timestamp: 1001,
		});
		store.put_process_log(PutProcessLogArg {
			bytes: Bytes::from("CCCC"),
			process: process.clone(),
			stream: tg::process::stdio::Stream::Stdout,
			timestamp: 1002,
		});

		// Read starting in the middle of the first chunk, across into the second.
		let result = store.try_read_process_log(ReadProcessLogArg {
			process: process.clone(),
			position: 2,
			length: 4,
			streams: BTreeSet::from([tg::process::stdio::Stream::Stdout]),
		});
		assert_eq!(collect_bytes(result), Bytes::from("AABB"));

		// Read starting in the middle of the second chunk, across into the third.
		let result = store.try_read_process_log(ReadProcessLogArg {
			process: process.clone(),
			position: 6,
			length: 4,
			streams: BTreeSet::from([tg::process::stdio::Stream::Stdout]),
		});
		assert_eq!(collect_bytes(result), Bytes::from("BBCC"));

		// Read spanning all three chunks.
		let result = store.try_read_process_log(ReadProcessLogArg {
			process: process.clone(),
			position: 2,
			length: 8,
			streams: BTreeSet::from([tg::process::stdio::Stream::Stdout]),
		});
		assert_eq!(collect_bytes(result), Bytes::from("AABBBBCC"));
	}

	#[test]
	fn test_read_log_combined_stream() {
		let store = Store::new();
		let process = tg::process::Id::new();

		// Insert interleaved stdout and stderr chunks.
		store.put_process_log(PutProcessLogArg {
			bytes: Bytes::from("out1"),
			process: process.clone(),
			stream: tg::process::stdio::Stream::Stdout,
			timestamp: 1000,
		});
		store.put_process_log(PutProcessLogArg {
			bytes: Bytes::from("err1"),
			process: process.clone(),
			stream: tg::process::stdio::Stream::Stderr,
			timestamp: 1001,
		});
		store.put_process_log(PutProcessLogArg {
			bytes: Bytes::from("out2"),
			process: process.clone(),
			stream: tg::process::stdio::Stream::Stdout,
			timestamp: 1002,
		});

		// Read the combined stream.
		let result = store.try_read_process_log(ReadProcessLogArg {
			process: process.clone(),
			position: 0,
			length: 12,
			streams: BTreeSet::from([
				tg::process::stdio::Stream::Stdout,
				tg::process::stdio::Stream::Stderr,
			]),
		});
		assert_eq!(collect_bytes(result), Bytes::from("out1err1out2"));

		// Read only stdout.
		let result = store.try_read_process_log(ReadProcessLogArg {
			process: process.clone(),
			position: 0,
			length: 8,
			streams: BTreeSet::from([tg::process::stdio::Stream::Stdout]),
		});
		assert_eq!(collect_bytes(result), Bytes::from("out1out2"));

		// Read only stderr.
		let result = store.try_read_process_log(ReadProcessLogArg {
			process: process.clone(),
			position: 0,
			length: 4,
			streams: BTreeSet::from([tg::process::stdio::Stream::Stderr]),
		});
		assert_eq!(collect_bytes(result), Bytes::from("err1"));
	}

	#[test]
	fn test_delete_log_removes_all_chunks() {
		let store = Store::new();
		let process = tg::process::Id::new();

		// Insert some chunks.
		store.put_process_log(PutProcessLogArg {
			bytes: Bytes::from("hello"),
			process: process.clone(),
			stream: tg::process::stdio::Stream::Stdout,
			timestamp: 1000,
		});
		store.put_process_log(PutProcessLogArg {
			bytes: Bytes::from("world"),
			process: process.clone(),
			stream: tg::process::stdio::Stream::Stderr,
			timestamp: 1001,
		});

		// Verify the log exists.
		let result = store.try_read_process_log(ReadProcessLogArg {
			process: process.clone(),
			position: 0,
			length: 10,
			streams: BTreeSet::from([
				tg::process::stdio::Stream::Stdout,
				tg::process::stdio::Stream::Stderr,
			]),
		});
		assert_eq!(collect_bytes(result), Bytes::from("helloworld"));

		// Delete the log.
		store.delete_process_log(DeleteProcessLogArg {
			process: process.clone(),
		});

		// Verify the log no longer exists.
		let result = store.try_read_process_log(ReadProcessLogArg {
			process: process.clone(),
			position: 0,
			length: 10,
			streams: BTreeSet::from([
				tg::process::stdio::Stream::Stdout,
				tg::process::stdio::Stream::Stderr,
			]),
		});
		assert!(result.is_empty());
	}

	#[test]
	fn test_try_get_log_length() {
		let store = Store::new();
		let process = tg::process::Id::new();

		// Insert chunks.
		store.put_process_log(PutProcessLogArg {
			bytes: Bytes::from("hello"),
			process: process.clone(),
			stream: tg::process::stdio::Stream::Stdout,
			timestamp: 1000,
		});
		store.put_process_log(PutProcessLogArg {
			bytes: Bytes::from("err"),
			process: process.clone(),
			stream: tg::process::stdio::Stream::Stderr,
			timestamp: 1001,
		});
		store.put_process_log(PutProcessLogArg {
			bytes: Bytes::from("world"),
			process: process.clone(),
			stream: tg::process::stdio::Stream::Stdout,
			timestamp: 1002,
		});

		// Check lengths.
		assert_eq!(
			store.try_get_process_log_length(
				&process,
				&BTreeSet::from([
					tg::process::stdio::Stream::Stdout,
					tg::process::stdio::Stream::Stderr,
				])
			),
			Some(13)
		);
		assert_eq!(
			store.try_get_process_log_length(
				&process,
				&BTreeSet::from([tg::process::stdio::Stream::Stdout])
			),
			Some(10)
		);
		assert_eq!(
			store.try_get_process_log_length(
				&process,
				&BTreeSet::from([tg::process::stdio::Stream::Stderr])
			),
			Some(3)
		);
	}

	#[test]
	fn test_read_log_at_end_returns_empty() {
		let store = Store::new();
		let process = tg::process::Id::new();

		// Insert a chunk.
		store.put_process_log(PutProcessLogArg {
			bytes: Bytes::from("hello"),
			process: process.clone(),
			stream: tg::process::stdio::Stream::Stdout,
			timestamp: 1000,
		});

		// Read at the end position.
		let result = store.try_read_process_log(ReadProcessLogArg {
			process: process.clone(),
			position: 5,
			length: 10,
			streams: BTreeSet::from([tg::process::stdio::Stream::Stdout]),
		});
		assert_eq!(collect_bytes(result), Bytes::new());
	}

	#[test]
	fn test_read_log_nonexistent_process() {
		let store = Store::new();
		let process = tg::process::Id::new();

		// Try to read from a process that does not exist.
		let result = store.try_read_process_log(ReadProcessLogArg {
			process,
			position: 0,
			length: 10,
			streams: BTreeSet::from([
				tg::process::stdio::Stream::Stdout,
				tg::process::stdio::Stream::Stderr,
			]),
		});
		assert!(result.is_empty());
	}
}
