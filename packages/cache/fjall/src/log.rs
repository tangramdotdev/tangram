use {
	crate::{Cache, Key as CacheKey, Kind},
	foundationdb_tuple::{self as fdbt, TuplePack as _},
	num::ToPrimitive as _,
	std::borrow::Cow,
	tangram_cache::log,
	tangram_client::prelude::*,
};

mod key;
#[cfg(test)]
mod tests;

pub(super) use key::Key;

#[derive(Clone, Copy, Debug)]
struct StreamPointer {
	combined_position: u64,
	length: u64,
	stream_position: u64,
}

impl Cache {
	pub(super) async fn delete_log(&self, arg: log::delete::Arg) -> tg::Result<()> {
		self.send_write_request(super::request::Request::DeleteLog(arg))
			.await?;
		Ok(())
	}

	pub(super) fn delete_log_with_transaction(
		transaction: &mut crate::transaction::Transaction<'_>,
		arg: &log::delete::Arg,
	) -> tg::Result<()> {
		let process = &arg.process;
		let start = CacheKey::Log(Key::End {
			position: 0,
			process,
		})
		.pack_to_vec();
		let end = CacheKey::Log(Key::End {
			position: u64::MAX,
			process,
		})
		.pack_to_vec();
		Self::delete_log_range_with_transaction(transaction, &start, &end)?;
		let start = CacheKey::Log(Key::Entry {
			position: 0,
			process,
		})
		.pack_to_vec();
		let end = CacheKey::Log(Key::Entry {
			position: u64::MAX,
			process,
		})
		.pack_to_vec();
		Self::delete_log_range_with_transaction(transaction, &start, &end)?;
		for stream in [
			tg::process::stdio::Stream::Stderr,
			tg::process::stdio::Stream::Stdout,
		] {
			let start = CacheKey::Log(Key::StreamPosition {
				position: 0,
				process,
				stream,
			})
			.pack_to_vec();
			let end = CacheKey::Log(Key::StreamPosition {
				position: u64::MAX,
				process,
				stream,
			})
			.pack_to_vec();
			Self::delete_log_range_with_transaction(transaction, &start, &end)?;
		}

		Ok(())
	}

	fn delete_log_range_with_transaction(
		transaction: &mut crate::transaction::Transaction<'_>,
		start: &[u8],
		end: &[u8],
	) -> tg::Result<()> {
		let mut current = start.to_vec();
		let mut keys = Vec::new();
		loop {
			let Some((key, _)) = transaction
				.get_greater_than_or_equal_to(&current)
				.map_err(|error| tg::error!(!error, "failed to iterate the log entries"))?
			else {
				break;
			};
			if key.as_ref() > end {
				break;
			}
			keys.push(key.to_vec());
			current = key.to_vec();
			current.push(0);
		}
		for key in keys {
			transaction
				.delete(&key)
				.map_err(|error| tg::error!(!error, "failed to delete the log entry"))?;
		}

		Ok(())
	}

	pub(super) async fn put_log(&self, arg: log::put::Arg) -> tg::Result<()> {
		self.put_log_batch(vec![arg]).await?;
		Ok(())
	}

	pub(super) async fn put_log_batch(&self, mut args: Vec<log::put::Arg>) -> tg::Result<()> {
		args.retain(|arg| !arg.bytes.is_empty());
		if args.is_empty() {
			return Ok(());
		}
		self.send_write_request(super::request::Request::PutLogBatch(args))
			.await?;
		Ok(())
	}

	pub(super) fn put_log_with_transaction(
		transaction: &mut crate::transaction::Transaction<'_>,
		arg: &log::put::Arg,
	) -> tg::Result<()> {
		if arg.bytes.is_empty() {
			return Ok(());
		}
		let length = arg.bytes.len().to_u64().unwrap();
		let entry = log::read::Entry {
			bytes: Cow::Owned(arg.bytes.to_vec()),
			position: arg.position,
			stream: arg.stream,
			stream_position: arg.stream_position,
			timestamp: arg.timestamp,
		};
		let key = CacheKey::Log(Key::Entry {
			position: arg.position,
			process: &arg.process,
		});
		let value = tangram_serialize::to_vec(&entry)
			.map_err(|error| tg::error!(!error, "failed to serialize the log entry"))?;
		transaction
			.put(&key.pack_to_vec(), &value)
			.map_err(|error| tg::error!(!error, "failed to cache the log entry"))?;
		let key = CacheKey::Log(Key::StreamPosition {
			position: arg.stream_position,
			process: &arg.process,
			stream: arg.stream,
		});
		let pointer = StreamPointer {
			combined_position: arg.position,
			length,
			stream_position: arg.stream_position,
		};
		let value = pointer.to_bytes();
		transaction
			.put(&key.pack_to_vec(), &value)
			.map_err(|error| tg::error!(!error, "failed to cache the log stream position"))?;

		Ok(())
	}

	fn validate_log_streams(
		streams: &std::collections::BTreeSet<tg::process::stdio::Stream>,
	) -> tg::Result<()> {
		if streams.is_empty() {
			return Err(tg::error!("expected at least one log stream"));
		}
		if streams.len() > 2 {
			return Err(tg::error!("the log streams is invalid"));
		}
		if streams.contains(&tg::process::stdio::Stream::Stdin) {
			return Err(tg::error!("the log streams is invalid"));
		}

		Ok(())
	}

	pub(super) async fn put_log_end(&self, arg: log::end::Arg) -> tg::Result<()> {
		self.send_write_request(super::request::Request::PutLogEnd(arg))
			.await?;
		Ok(())
	}

	pub(super) fn put_log_end_with_transaction(
		transaction: &mut crate::transaction::Transaction<'_>,
		arg: &log::end::Arg,
	) -> tg::Result<()> {
		let key = CacheKey::Log(Key::End {
			position: arg.end.position,
			process: &arg.process,
		})
		.pack_to_vec();
		let value = tangram_serialize::to_vec(&arg.end)
			.map_err(|error| tg::error!(!error, "failed to serialize the log end"))?;
		transaction
			.put(&key, &value)
			.map_err(|error| tg::error!(!error, "failed to cache the log end"))?;
		Ok(())
	}

	pub(super) async fn try_get_log_end(
		&self,
		process: &tg::process::Id,
	) -> tg::Result<Option<tg::process::log::End>> {
		let request = crate::read::Request::TryGetLogEnd(process.clone());
		let response = self.send_read_request(request).await?;
		let crate::read::Response::TryGetLogEnd(output) = response else {
			return Err(tg::error!("received an unexpected read response"));
		};
		Ok(output)
	}

	pub(super) fn try_get_log_end_with_transaction(
		transaction: &crate::transaction::Transaction<'_>,
		process: &tg::process::Id,
	) -> tg::Result<Option<tg::process::log::End>> {
		let start = CacheKey::Log(Key::End {
			position: 0,
			process,
		})
		.pack_to_vec();
		let end = CacheKey::Log(Key::End {
			position: u64::MAX,
			process,
		})
		.pack_to_vec();
		let Some((key, value)) = transaction
			.get_lower_than_or_equal_to(&end)
			.map_err(|error| tg::error!(!error, "failed to get the log end"))?
		else {
			return Ok(None);
		};
		if key.as_ref() < start.as_slice() {
			return Ok(None);
		}
		let output = tangram_serialize::from_slice(&value)
			.map_err(|error| tg::error!(!error, "failed to deserialize the log end"))?;
		Ok(Some(output))
	}

	pub(super) async fn try_get_log_length(
		&self,
		arg: log::length::Arg,
	) -> tg::Result<Option<u64>> {
		let request = crate::read::Request::TryGetLogLength(arg);
		let response = self.send_read_request(request).await?;
		let crate::read::Response::TryGetLogLength(output) = response else {
			return Err(tg::error!("received an unexpected read response"));
		};
		Ok(output)
	}

	pub(super) fn try_get_log_length_with_transaction(
		transaction: &crate::transaction::Transaction<'_>,
		arg: &log::length::Arg,
	) -> tg::Result<Option<u64>> {
		Self::validate_log_streams(&arg.streams)?;
		let mut pointers = Vec::new();
		for &stream in &arg.streams {
			if let Some(pointer) = Self::try_get_last_log_stream_pointer_with_transaction(
				transaction,
				&arg.process,
				stream,
			)? {
				pointers.push(pointer);
			}
		}
		let Some(pointer) = pointers
			.into_iter()
			.max_by_key(|pointer| pointer.combined_position)
		else {
			return Ok(None);
		};
		let position = if arg.streams.len() == 1 {
			pointer.stream_position
		} else {
			pointer.combined_position
		};
		let length = position
			.checked_add(pointer.length)
			.ok_or_else(|| tg::error!("the log length is too large"))?;

		Ok(Some(length))
	}

	fn try_get_last_log_stream_pointer_with_transaction(
		transaction: &crate::transaction::Transaction<'_>,
		process: &tg::process::Id,
		stream: tg::process::stdio::Stream,
	) -> tg::Result<Option<StreamPointer>> {
		let start = CacheKey::Log(Key::StreamPosition {
			position: 0,
			process,
			stream,
		})
		.pack_to_vec();
		let end = CacheKey::Log(Key::StreamPosition {
			position: u64::MAX,
			process,
			stream,
		})
		.pack_to_vec();
		let Some((key, value)) = transaction
			.get_lower_than_or_equal_to(&end)
			.map_err(|error| tg::error!(!error, "failed to get the last log stream position"))?
		else {
			return Ok(None);
		};
		if key.as_ref() < start.as_slice() {
			return Ok(None);
		}
		let pointer = StreamPointer::from_slice(&value)?;

		Ok(Some(pointer))
	}

	pub(super) async fn try_read_log(
		&self,
		arg: log::read::Arg,
	) -> tg::Result<Vec<log::read::Entry<'static>>> {
		let request = crate::read::Request::TryReadLog(arg);
		let response = self.send_read_request(request).await?;
		let crate::read::Response::TryReadLog(output) = response else {
			return Err(tg::error!("received an unexpected read response"));
		};
		Ok(output)
	}

	pub(super) fn try_read_log_with_transaction(
		transaction: &crate::transaction::Transaction<'_>,
		arg: &log::read::Arg,
	) -> tg::Result<Vec<log::read::Entry<'static>>> {
		Self::validate_log_streams(&arg.streams)?;
		let combined = arg.streams.len() > 1;
		let start_position = if combined {
			arg.position
		} else {
			let stream = arg.streams.iter().next().copied().unwrap();
			let start = CacheKey::Log(Key::StreamPosition {
				position: 0,
				process: &arg.process,
				stream,
			})
			.pack_to_vec();
			let key = CacheKey::Log(Key::StreamPosition {
				position: arg.position,
				process: &arg.process,
				stream,
			})
			.pack_to_vec();
			let value = if arg.position == 0 {
				transaction
					.get(&key)
					.map_err(|error| tg::error!(!error, "failed to get the log stream position"))?
			} else {
				transaction
					.get_lower_than_or_equal_to(&key)
					.map_err(|error| tg::error!(!error, "failed to get the log stream position"))?
					.and_then(|(key, value)| {
						(key.as_ref() >= start.as_slice()).then_some(value.into_vec())
					})
			};
			let Some(value) = value else {
				return Ok(Vec::new());
			};
			StreamPointer::from_slice(&value)?.combined_position
		};
		let start = CacheKey::Log(Key::Entry {
			position: 0,
			process: &arg.process,
		})
		.pack_to_vec();
		let key = CacheKey::Log(Key::Entry {
			position: start_position,
			process: &arg.process,
		})
		.pack_to_vec();
		let entry = if start_position == 0 {
			transaction
				.get(&key)
				.map_err(|error| tg::error!(!error, "failed to get the log entry"))?
				.map(|value| (key.clone(), value.into_boxed_slice()))
		} else {
			transaction
				.get_lower_than_or_equal_to(&key)
				.map_err(|error| tg::error!(!error, "failed to get the log entry"))?
				.and_then(|(key, value)| {
					(key.as_ref() >= start.as_slice()).then(|| (key.to_vec(), value))
				})
		};
		let Some((mut current_key, first_value)) = entry else {
			return Ok(Vec::new());
		};
		let end = CacheKey::Log(Key::Entry {
			position: u64::MAX,
			process: &arg.process,
		})
		.pack_to_vec();
		let mut builder = log::read::Builder::new(arg);
		let mut value = Some(first_value);
		loop {
			let value = if let Some(value) = value.take() {
				value
			} else {
				let Some((key, value)) = transaction
					.get_greater_than(&current_key)
					.map_err(|error| tg::error!(!error, "failed to get the next log entry"))?
				else {
					break;
				};
				if key.as_ref() > end.as_slice() {
					break;
				}
				current_key = key.to_vec();
				value
			};
			let chunk = tangram_serialize::from_slice::<log::read::Entry<'_>>(&value)
				.map_err(|error| tg::error!(!error, "failed to deserialize the log entry"))?;
			if !builder.push(&chunk) {
				break;
			}
		}
		let output = builder.finish();

		Ok(output)
	}

	pub async fn delete_log_cache_entry(&self, arg: log::cache::delete::Arg) -> tg::Result<()> {
		let request = crate::request::Request::DeleteLogCacheEntry(arg);
		self.send_write_request(request).await?;
		Ok(())
	}

	pub async fn get_log_cache_entries(
		&self,
		arg: log::cache::get::Arg,
	) -> tg::Result<Vec<log::cache::Entry>> {
		let request = crate::read::Request::GetLogCacheEntries(arg);
		let response = self.send_read_request(request).await?;
		let crate::read::Response::GetLogCacheEntries(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};
		Ok(output)
	}

	pub async fn put_log_cache_entry(&self, arg: log::cache::put::Arg) -> tg::Result<()> {
		let request = crate::request::Request::PutLogCacheEntry(arg);
		self.send_write_request(request).await?;
		Ok(())
	}

	pub(crate) fn delete_log_cache_entry_with_transaction(
		transaction: &mut crate::transaction::Transaction<'_>,
		arg: log::cache::delete::Arg,
	) -> tg::Result<()> {
		let entry = arg.entry;
		let arg = log::delete::Arg {
			process: entry.process.clone(),
		};
		Self::delete_log_with_transaction(transaction, &arg)?;
		let key = CacheKey::LogCache(entry).pack_to_vec();
		transaction
			.delete(&key)
			.map_err(|error| tg::error!(!error, "failed to delete a log cache entry"))?;
		Ok(())
	}

	pub(crate) fn get_log_cache_entries_with_transaction(
		transaction: &crate::transaction::Transaction<'_>,
		arg: &log::cache::get::Arg,
	) -> tg::Result<Vec<log::cache::Entry>> {
		let prefix = fdbt::pack(&(Kind::LogCache.to_i32().unwrap(), arg.partition));
		let entries = transaction.prefix_iter(&prefix);
		let mut output = Vec::new();
		for entry in entries.take(arg.batch_size) {
			let (key, _) =
				entry.map_err(|error| tg::error!(!error, "failed to get a log cache entry"))?;
			let (_, partition, expires_at, process): (i32, u64, i64, Vec<u8>) = fdbt::unpack(&key)
				.map_err(|error| tg::error!(!error, "failed to unpack a log cache key"))?;
			if expires_at > arg.now {
				break;
			}
			let process = tg::process::Id::from_slice(&process)?;
			let entry = log::cache::Entry {
				expires_at,
				partition,
				process,
			};
			output.push(entry);
		}

		Ok(output)
	}

	pub(crate) fn put_log_cache_entry_with_transaction(
		transaction: &mut crate::transaction::Transaction<'_>,
		arg: log::cache::put::Arg,
	) -> tg::Result<()> {
		let key = CacheKey::LogCache(arg.entry).pack_to_vec();
		transaction
			.put(&key, &[])
			.map_err(|error| tg::error!(!error, "failed to put a log cache entry"))?;
		Ok(())
	}
}

impl StreamPointer {
	fn from_slice(value: &[u8]) -> tg::Result<Self> {
		let value: &[u8; 24] = value
			.try_into()
			.map_err(|_| tg::error!("the log stream pointer is invalid"))?;
		let combined_position = u64::from_le_bytes(value[0..8].try_into().unwrap());
		let length = u64::from_le_bytes(value[8..16].try_into().unwrap());
		let stream_position = u64::from_le_bytes(value[16..24].try_into().unwrap());
		let pointer = Self {
			combined_position,
			length,
			stream_position,
		};

		Ok(pointer)
	}

	#[must_use]
	fn to_bytes(self) -> [u8; 24] {
		let mut value = [0; 24];
		value[0..8].copy_from_slice(&self.combined_position.to_le_bytes());
		value[8..16].copy_from_slice(&self.length.to_le_bytes());
		value[16..24].copy_from_slice(&self.stream_position.to_le_bytes());

		value
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
		self.delete_log(arg).await?;
		Ok(())
	}

	async fn put_log(&self, arg: tangram_cache::log::put::Arg) -> tg::Result<()> {
		self.put_log(arg).await?;
		Ok(())
	}

	async fn put_log_batch(&self, args: Vec<tangram_cache::log::put::Arg>) -> tg::Result<()> {
		self.put_log_batch(args).await?;
		Ok(())
	}

	async fn put_log_end(&self, arg: tangram_cache::log::end::Arg) -> tg::Result<()> {
		self.put_log_end(arg).await?;
		Ok(())
	}

	async fn try_get_log_end(
		&self,
		process: &tg::process::Id,
	) -> tg::Result<Option<tg::process::log::End>> {
		self.try_get_log_end(process).await
	}

	async fn try_get_log_length(
		&self,
		arg: tangram_cache::log::length::Arg,
	) -> tg::Result<Option<u64>> {
		self.try_get_log_length(arg).await
	}

	async fn try_read_log(
		&self,
		arg: tangram_cache::log::read::Arg,
	) -> tg::Result<Vec<tangram_cache::log::read::Entry<'static>>> {
		self.try_read_log(arg).await
	}
}
