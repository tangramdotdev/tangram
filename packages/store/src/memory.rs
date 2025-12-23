use {
	crate::{CacheReference, DeleteArg, DeleteLogArg, PutArg, PutLogArg, ReadLogArg},
	bytes::Bytes,
	dashmap::DashMap,
	num::ToPrimitive as _,
	std::collections::BTreeMap,
	tangram_client::prelude::*,
};

pub struct Store {
	objects: DashMap<tg::object::Id, ObjectEntry, tg::id::BuildHasher>,
	logs: DashMap<tg::process::Id, LogEntry, tg::id::BuildHasher>,
}

struct ObjectEntry {
	bytes: Option<Bytes>,
	cache_reference: Option<CacheReference>,
	touched_at: i64,
}

#[derive(Default)]
struct LogEntry {
	counter: usize,
	chunks: BTreeMap<usize, crate::log::Chunk>,
	stderr: BTreeMap<u64, usize>,
	stdout: BTreeMap<u64, usize>,
	combined: BTreeMap<u64, usize>,
	stderr_position: u64,
	stdout_position: u64,
	combined_position: u64,
}

#[derive(Debug, derive_more::Display, derive_more::Error)]
pub enum Error {
	Other(Box<dyn std::error::Error + Send + Sync>),
}

impl Store {
	#[must_use]
	pub fn new() -> Self {
		Self {
			objects: DashMap::default(),
			logs: DashMap::default(),
		}
	}

	#[must_use]
	pub fn try_get(&self, id: &tg::object::Id) -> Option<Bytes> {
		let entry = self.objects.get(id)?;
		let bytes = entry.bytes.as_ref()?;
		Some(bytes.clone())
	}

	#[must_use]
	pub fn try_get_batch(&self, ids: &[tg::object::Id]) -> Vec<Option<Bytes>> {
		ids.iter().map(|id| self.try_get(id)).collect()
	}

	#[must_use]
	pub fn try_get_cache_reference(&self, id: &tg::object::Id) -> Option<CacheReference> {
		let entry = self.objects.get(id)?;
		let cache_reference = entry.cache_reference.clone()?;
		Some(cache_reference)
	}

	pub fn try_get_object_data(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<(u64, tg::object::Data)>> {
		let Some(entry) = self.objects.get(id) else {
			return Ok(None);
		};
		let Some(bytes) = &entry.bytes else {
			return Ok(None);
		};
		let size = bytes.len().to_u64().unwrap();
		let data = tg::object::Data::deserialize(id.kind(), bytes.as_ref())?;
		Ok(Some((size, data)))
	}

	pub fn put(&self, arg: PutArg) {
		let entry = ObjectEntry {
			bytes: arg.bytes,
			cache_reference: arg.cache_reference,
			touched_at: arg.touched_at,
		};
		self.objects.insert(arg.id, entry);
	}

	pub fn put_batch(&self, args: Vec<PutArg>) {
		for arg in args {
			self.put(arg);
		}
	}

	#[expect(clippy::needless_pass_by_value)]
	pub fn delete(&self, arg: DeleteArg) {
		self.objects.remove_if(&arg.id, |_, entry| {
			entry.touched_at >= arg.now - arg.ttl.to_i64().unwrap()
		});
	}

	pub fn delete_batch(&self, args: Vec<DeleteArg>) {
		for arg in args {
			self.delete(arg);
		}
	}

	#[must_use]
	#[expect(clippy::needless_pass_by_value)]
	pub fn try_read_log(&self, arg: ReadLogArg) -> Option<Bytes> {
		let entry = self.logs.get(&arg.process)?;
		let index = match arg.stream {
			Some(tg::process::log::Stream::Stderr) => {
				if arg.position >= entry.stderr_position {
					return Some(Bytes::new());
				}
				let (_, index) = entry.stderr.range(0..=arg.position).rev().next()?;
				*index
			},
			Some(tg::process::log::Stream::Stdout) => {
				if arg.position >= entry.stdout_position {
					return Some(Bytes::new());
				}
				let (_, index) = entry.stdout.range(0..=arg.position).rev().next()?;
				*index
			},
			None => {
				if arg.position == entry.combined_position {
					return Some(Bytes::new());
				}
				let (_, index) = entry.combined.range(0..=arg.position).rev().next()?;
				*index
			},
		};
		let data = entry
			.chunks
			.range(index..)
			.filter_map(|(_, chunk)| {
				if arg.stream.is_some_and(|stream| stream == chunk.stream) {
					Some((chunk.stream_position, &chunk.bytes))
				} else if arg.stream.is_none() {
					Some((chunk.combined_position, &chunk.bytes))
				} else {
					None
				}
			})
			.take_while(|(position, _)| *position <= (arg.position + arg.length))
			.fold(
				Vec::with_capacity(arg.length.to_usize().unwrap()),
				|mut output, (position, bytes)| {
					let offset = arg.position.saturating_sub(position).to_usize().unwrap();
					let length = bytes
						.len()
						.min(arg.length.to_usize().unwrap() - output.len());
					output.extend_from_slice(&bytes[offset..(offset + length).min(bytes.len())]);
					output
				},
			);
		Some(data.into())
	}

	#[must_use]
	pub fn try_get_log_length(
		&self,
		id: &tg::process::Id,
		stream: Option<tg::process::log::Stream>,
	) -> Option<u64> {
		let entry = self.logs.get(id)?;
		match stream {
			Some(tg::process::log::Stream::Stderr) => Some(entry.stderr_position),
			Some(tg::process::log::Stream::Stdout) => Some(entry.stdout_position),
			None => Some(entry.combined_position),
		}
	}

	pub fn put_log(&self, arg: PutLogArg) {
		let mut entry = self.logs.entry(arg.process).or_default();

		// Get the index we're inserting to.
		let index = entry.counter;

		// Get the stream positions.
		let combined_position = entry.combined_position;
		let stderr_position = entry.stderr_position;
		let stdout_position = entry.stdout_position;

		// Create the combined position.
		entry.combined.insert(combined_position, index);

		// Insert the chunk and update counters.
		match arg.stream {
			tg::process::log::Stream::Stderr => {
				let chunk = crate::log::Chunk {
					bytes: arg.bytes.clone(),
					combined_position,
					stream_position: stderr_position,
					stream: arg.stream,
					timestamp: arg.timestamp,
				};
				entry.chunks.insert(index, chunk);
				entry.stderr.insert(stderr_position, index);
				entry.combined_position += arg.bytes.len().to_u64().unwrap();
				entry.stderr_position += arg.bytes.len().to_u64().unwrap();
			},
			tg::process::log::Stream::Stdout => {
				let chunk = crate::log::Chunk {
					bytes: arg.bytes.clone(),
					combined_position,
					stream_position: stdout_position,
					stream: arg.stream,
					timestamp: arg.timestamp,
				};
				entry.chunks.insert(index, chunk);
				entry.stdout.insert(stdout_position, index);
				entry.combined_position += arg.bytes.len().to_u64().unwrap();
				entry.stdout_position += arg.bytes.len().to_u64().unwrap();
			},
		}

		entry.counter += 1;
	}

	pub fn put_log_batch(&self, args: Vec<PutLogArg>) {
		for arg in args {
			self.put_log(arg);
		}
	}

	#[expect(clippy::needless_pass_by_value)]
	pub fn delete_log(&self, arg: DeleteLogArg) {
		self.logs.remove(&arg.process);
	}

	pub fn delete_log_batch(&self, args: Vec<DeleteLogArg>) {
		for arg in args {
			self.delete_log(arg);
		}
	}

	pub fn flush(&self) {}
}

impl Default for Store {
	fn default() -> Self {
		Self::new()
	}
}

impl crate::Store for Store {
	type Error = Error;

	async fn try_get(&self, id: &tg::object::Id) -> Result<Option<Bytes>, Self::Error> {
		Ok(self.try_get(id))
	}

	async fn try_read_log(&self, arg: ReadLogArg) -> Result<Option<Bytes>, Self::Error> {
		Ok(self.try_read_log(arg))
	}

	async fn try_get_log_length(
		&self,
		id: &tg::process::Id,
		stream: Option<tg::process::log::Stream>,
	) -> Result<Option<u64>, Self::Error> {
		Ok(self.try_get_log_length(id, stream))
	}

	async fn try_get_batch(
		&self,
		ids: &[tg::object::Id],
	) -> Result<Vec<Option<Bytes>>, Self::Error> {
		Ok(self.try_get_batch(ids))
	}

	async fn try_get_cache_reference(
		&self,
		id: &tg::object::Id,
	) -> Result<Option<CacheReference>, Self::Error> {
		Ok(self.try_get_cache_reference(id))
	}

	async fn put(&self, arg: PutArg) -> Result<(), Self::Error> {
		self.put(arg);
		Ok(())
	}

	async fn put_log(&self, arg: PutLogArg) -> Result<(), Self::Error> {
		self.put_log(arg);
		Ok(())
	}

	async fn put_batch(&self, args: Vec<PutArg>) -> Result<(), Self::Error> {
		self.put_batch(args);
		Ok(())
	}

	async fn put_log_batch(&self, args: Vec<PutLogArg>) -> Result<(), Self::Error> {
		self.put_log_batch(args);
		Ok(())
	}

	async fn delete(&self, arg: DeleteArg) -> Result<(), Self::Error> {
		self.delete(arg);
		Ok(())
	}

	async fn delete_log(&self, arg: DeleteLogArg) -> Result<(), Self::Error> {
		self.delete_log(arg);
		Ok(())
	}

	async fn delete_batch(&self, args: Vec<DeleteArg>) -> Result<(), Self::Error> {
		self.delete_batch(args);
		Ok(())
	}

	async fn delete_log_batch(&self, args: Vec<DeleteLogArg>) -> Result<(), Self::Error> {
		self.delete_log_batch(args);
		Ok(())
	}

	async fn flush(&self) -> Result<(), Self::Error> {
		self.flush();
		Ok(())
	}
}

impl crate::Error for Error {
	fn other(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
		Self::Other(error.into())
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_put_and_read_log_single_chunk() {
		let store = Store::new();
		let process = tg::process::Id::new();

		// Insert a single chunk.
		store.put_log(PutLogArg {
			bytes: Bytes::from("hello world"),
			process: process.clone(),
			stream: tg::process::log::Stream::Stdout,
			timestamp: 1000,
		});

		// Read the entire chunk.
		let result = store.try_read_log(ReadLogArg {
			process: process.clone(),
			position: 0,
			length: 11,
			stream: Some(tg::process::log::Stream::Stdout),
		});
		assert_eq!(result, Some(Bytes::from("hello world")));

		// Read a subset of the chunk.
		let result = store.try_read_log(ReadLogArg {
			process: process.clone(),
			position: 6,
			length: 5,
			stream: Some(tg::process::log::Stream::Stdout),
		});
		assert_eq!(result, Some(Bytes::from("world")));
	}

	#[test]
	fn test_put_and_read_log_multiple_chunks() {
		let store = Store::new();
		let process = tg::process::Id::new();

		// Insert multiple chunks.
		store.put_log(PutLogArg {
			bytes: Bytes::from("hello"),
			process: process.clone(),
			stream: tg::process::log::Stream::Stdout,
			timestamp: 1000,
		});
		store.put_log(PutLogArg {
			bytes: Bytes::from(" "),
			process: process.clone(),
			stream: tg::process::log::Stream::Stdout,
			timestamp: 1001,
		});
		store.put_log(PutLogArg {
			bytes: Bytes::from("world"),
			process: process.clone(),
			stream: tg::process::log::Stream::Stdout,
			timestamp: 1002,
		});

		// Read across all chunks.
		let result = store.try_read_log(ReadLogArg {
			process: process.clone(),
			position: 0,
			length: 11,
			stream: Some(tg::process::log::Stream::Stdout),
		});
		assert_eq!(result, Some(Bytes::from("hello world")));
	}

	#[test]
	fn test_read_log_across_chunk_boundaries() {
		let store = Store::new();
		let process = tg::process::Id::new();

		// Insert chunks: "AAAA" (0-3), "BBBB" (4-7), "CCCC" (8-11).
		store.put_log(PutLogArg {
			bytes: Bytes::from("AAAA"),
			process: process.clone(),
			stream: tg::process::log::Stream::Stdout,
			timestamp: 1000,
		});
		store.put_log(PutLogArg {
			bytes: Bytes::from("BBBB"),
			process: process.clone(),
			stream: tg::process::log::Stream::Stdout,
			timestamp: 1001,
		});
		store.put_log(PutLogArg {
			bytes: Bytes::from("CCCC"),
			process: process.clone(),
			stream: tg::process::log::Stream::Stdout,
			timestamp: 1002,
		});

		// Read starting in the middle of the first chunk, across into the second.
		let result = store.try_read_log(ReadLogArg {
			process: process.clone(),
			position: 2,
			length: 4,
			stream: Some(tg::process::log::Stream::Stdout),
		});
		assert_eq!(result, Some(Bytes::from("AABB")));

		// Read starting in the middle of the second chunk, across into the third.
		let result = store.try_read_log(ReadLogArg {
			process: process.clone(),
			position: 6,
			length: 4,
			stream: Some(tg::process::log::Stream::Stdout),
		});
		assert_eq!(result, Some(Bytes::from("BBCC")));

		// Read spanning all three chunks.
		let result = store.try_read_log(ReadLogArg {
			process: process.clone(),
			position: 2,
			length: 8,
			stream: Some(tg::process::log::Stream::Stdout),
		});
		assert_eq!(result, Some(Bytes::from("AABBBBCC")));
	}

	#[test]
	fn test_read_log_combined_stream() {
		let store = Store::new();
		let process = tg::process::Id::new();

		// Insert interleaved stdout and stderr chunks.
		store.put_log(PutLogArg {
			bytes: Bytes::from("out1"),
			process: process.clone(),
			stream: tg::process::log::Stream::Stdout,
			timestamp: 1000,
		});
		store.put_log(PutLogArg {
			bytes: Bytes::from("err1"),
			process: process.clone(),
			stream: tg::process::log::Stream::Stderr,
			timestamp: 1001,
		});
		store.put_log(PutLogArg {
			bytes: Bytes::from("out2"),
			process: process.clone(),
			stream: tg::process::log::Stream::Stdout,
			timestamp: 1002,
		});

		// Read the combined stream.
		let result = store.try_read_log(ReadLogArg {
			process: process.clone(),
			position: 0,
			length: 12,
			stream: None,
		});
		assert_eq!(result, Some(Bytes::from("out1err1out2")));

		// Read only stdout.
		let result = store.try_read_log(ReadLogArg {
			process: process.clone(),
			position: 0,
			length: 8,
			stream: Some(tg::process::log::Stream::Stdout),
		});
		assert_eq!(result, Some(Bytes::from("out1out2")));

		// Read only stderr.
		let result = store.try_read_log(ReadLogArg {
			process: process.clone(),
			position: 0,
			length: 4,
			stream: Some(tg::process::log::Stream::Stderr),
		});
		assert_eq!(result, Some(Bytes::from("err1")));
	}

	#[test]
	fn test_delete_log_removes_all_chunks() {
		let store = Store::new();
		let process = tg::process::Id::new();

		// Insert some chunks.
		store.put_log(PutLogArg {
			bytes: Bytes::from("hello"),
			process: process.clone(),
			stream: tg::process::log::Stream::Stdout,
			timestamp: 1000,
		});
		store.put_log(PutLogArg {
			bytes: Bytes::from("world"),
			process: process.clone(),
			stream: tg::process::log::Stream::Stderr,
			timestamp: 1001,
		});

		// Verify the log exists.
		let result = store.try_read_log(ReadLogArg {
			process: process.clone(),
			position: 0,
			length: 10,
			stream: None,
		});
		assert_eq!(result, Some(Bytes::from("helloworld")));

		// Delete the log.
		store.delete_log(DeleteLogArg {
			process: process.clone(),
			stream: tg::process::log::Stream::Stdout,
			stream_position: 0,
		});

		// Verify the log no longer exists.
		let result = store.try_read_log(ReadLogArg {
			process: process.clone(),
			position: 0,
			length: 10,
			stream: None,
		});
		assert_eq!(result, None);
	}

	#[test]
	fn test_try_get_log_length() {
		let store = Store::new();
		let process = tg::process::Id::new();

		// Insert chunks.
		store.put_log(PutLogArg {
			bytes: Bytes::from("hello"),
			process: process.clone(),
			stream: tg::process::log::Stream::Stdout,
			timestamp: 1000,
		});
		store.put_log(PutLogArg {
			bytes: Bytes::from("err"),
			process: process.clone(),
			stream: tg::process::log::Stream::Stderr,
			timestamp: 1001,
		});
		store.put_log(PutLogArg {
			bytes: Bytes::from("world"),
			process: process.clone(),
			stream: tg::process::log::Stream::Stdout,
			timestamp: 1002,
		});

		// Check lengths.
		assert_eq!(store.try_get_log_length(&process, None), Some(13)); // 5 + 3 + 5
		assert_eq!(
			store.try_get_log_length(&process, Some(tg::process::log::Stream::Stdout)),
			Some(10)
		); // 5 + 5
		assert_eq!(
			store.try_get_log_length(&process, Some(tg::process::log::Stream::Stderr)),
			Some(3)
		);
	}

	#[test]
	fn test_read_log_at_end_returns_empty() {
		let store = Store::new();
		let process = tg::process::Id::new();

		// Insert a chunk.
		store.put_log(PutLogArg {
			bytes: Bytes::from("hello"),
			process: process.clone(),
			stream: tg::process::log::Stream::Stdout,
			timestamp: 1000,
		});

		// Read at the end position.
		let result = store.try_read_log(ReadLogArg {
			process: process.clone(),
			position: 5,
			length: 10,
			stream: Some(tg::process::log::Stream::Stdout),
		});
		assert_eq!(result, Some(Bytes::new()));
	}

	#[test]
	fn test_read_log_nonexistent_process() {
		let store = Store::new();
		let process = tg::process::Id::new();

		// Try to read from a process that does not exist.
		let result = store.try_read_log(ReadLogArg {
			process,
			position: 0,
			length: 10,
			stream: None,
		});
		assert_eq!(result, None);
	}
}
