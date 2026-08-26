use {
	super::{Db, Key as StoreKey, Store},
	crate::log,
	foundationdb_tuple::TuplePack as _,
	heed as lmdb,
	num::ToPrimitive as _,
	std::borrow::Cow,
	tangram_client::prelude::*,
};

mod key;

pub(super) use key::Key;

#[derive(Clone, Copy, Debug)]
struct StreamPointer {
	combined_position: u64,
	length: u64,
	stream_position: u64,
}

impl StreamPointer {
	fn from_slice(value: &[u8]) -> tg::Result<Self> {
		let value: &[u8; 24] = value
			.try_into()
			.map_err(|_| tg::error!("invalid log stream pointer"))?;
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

impl Store {
	pub(super) async fn delete_log(&self, arg: log::delete::Arg) -> tg::Result<()> {
		self.send_write_request(super::request::Request::DeleteLog(arg))
			.await
	}

	pub(super) async fn put_log(&self, arg: log::put::Arg) -> tg::Result<()> {
		self.put_log_batch(vec![arg]).await
	}

	pub(super) async fn put_log_batch(&self, mut args: Vec<log::put::Arg>) -> tg::Result<()> {
		args.retain(|arg| !arg.bytes.is_empty());
		if args.is_empty() {
			return Ok(());
		}
		self.send_write_request(super::request::Request::PutLogBatch(args))
			.await
	}

	pub(super) async fn try_get_log_length(
		&self,
		arg: log::length::Arg,
	) -> tg::Result<Option<u64>> {
		let request = crate::read::Request::TryGetLogLength(arg);
		let response = self.send_read_request(request).await?;
		let crate::read::Response::TryGetLogLength(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub(super) async fn try_read_log(
		&self,
		arg: log::read::Arg,
	) -> tg::Result<Vec<log::read::Entry<'static>>> {
		let request = crate::read::Request::TryReadLog(arg);
		let response = self.send_read_request(request).await?;
		let crate::read::Response::TryReadLog(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub(super) fn delete_log_with_transaction(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &log::delete::Arg,
	) -> tg::Result<()> {
		let process = &arg.process;
		let start = StoreKey::Log(Key::Entry {
			position: 0,
			process,
		})
		.pack_to_vec();
		let end = StoreKey::Log(Key::Entry {
			position: u64::MAX,
			process,
		})
		.pack_to_vec();
		Self::delete_log_range_with_transaction(db, transaction, &start, &end)?;
		for stream in [
			tg::process::stdio::Stream::Stderr,
			tg::process::stdio::Stream::Stdout,
		] {
			let start = StoreKey::Log(Key::StreamPosition {
				position: 0,
				process,
				stream,
			})
			.pack_to_vec();
			let end = StoreKey::Log(Key::StreamPosition {
				position: u64::MAX,
				process,
				stream,
			})
			.pack_to_vec();
			Self::delete_log_range_with_transaction(db, transaction, &start, &end)?;
		}

		Ok(())
	}

	pub(super) fn put_log_with_transaction(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
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
		let key = StoreKey::Log(Key::Entry {
			position: arg.position,
			process: &arg.process,
		});
		let value = tangram_serialize::to_vec(&entry)
			.map_err(|error| tg::error!(!error, "failed to serialize the log entry"))?;
		db.put(transaction, &key.pack_to_vec(), &value)
			.map_err(|error| tg::error!(!error, "failed to store the log entry"))?;
		let key = StoreKey::Log(Key::StreamPosition {
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
		db.put(transaction, &key.pack_to_vec(), &value)
			.map_err(|error| tg::error!(!error, "failed to store the log stream position"))?;

		Ok(())
	}

	pub(super) fn try_get_log_length_with_transaction(
		db: &Db,
		transaction: &lmdb::RoTxn<'_>,
		arg: &log::length::Arg,
	) -> tg::Result<Option<u64>> {
		Self::validate_log_streams(&arg.streams)?;
		let mut pointers = Vec::new();
		for &stream in &arg.streams {
			if let Some(pointer) = Self::try_get_last_log_stream_pointer_with_transaction(
				db,
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
		db: &Db,
		transaction: &lmdb::RoTxn<'_>,
		process: &tg::process::Id,
		stream: tg::process::stdio::Stream,
	) -> tg::Result<Option<StreamPointer>> {
		let start = StoreKey::Log(Key::StreamPosition {
			position: 0,
			process,
			stream,
		})
		.pack_to_vec();
		let end = StoreKey::Log(Key::StreamPosition {
			position: u64::MAX,
			process,
			stream,
		})
		.pack_to_vec();
		let Some((key, value)) = db
			.get_lower_than_or_equal_to(transaction, &end)
			.map_err(|error| tg::error!(!error, "failed to get the last log stream position"))?
		else {
			return Ok(None);
		};
		if key < start.as_slice() {
			return Ok(None);
		}
		let pointer = StreamPointer::from_slice(value)?;

		Ok(Some(pointer))
	}

	pub(super) fn try_read_log_with_transaction(
		db: &Db,
		transaction: &lmdb::RoTxn<'_>,
		arg: &log::read::Arg,
	) -> tg::Result<Vec<log::read::Entry<'static>>> {
		Self::validate_log_streams(&arg.streams)?;
		let combined = arg.streams.len() > 1;
		let start_position = if combined {
			arg.position
		} else {
			let stream = arg.streams.iter().next().copied().unwrap();
			let start = StoreKey::Log(Key::StreamPosition {
				position: 0,
				process: &arg.process,
				stream,
			})
			.pack_to_vec();
			let key = StoreKey::Log(Key::StreamPosition {
				position: arg.position,
				process: &arg.process,
				stream,
			})
			.pack_to_vec();
			let value = if arg.position == 0 {
				db.get(transaction, &key)
					.map_err(|error| tg::error!(!error, "failed to get the log stream position"))?
			} else {
				db.get_lower_than_or_equal_to(transaction, &key)
					.map_err(|error| tg::error!(!error, "failed to get the log stream position"))?
					.and_then(|(key, value)| (key >= start.as_slice()).then_some(value))
			};
			let Some(value) = value else {
				return Ok(Vec::new());
			};
			StreamPointer::from_slice(value)?.combined_position
		};
		let start = StoreKey::Log(Key::Entry {
			position: 0,
			process: &arg.process,
		})
		.pack_to_vec();
		let key = StoreKey::Log(Key::Entry {
			position: start_position,
			process: &arg.process,
		})
		.pack_to_vec();
		let entry = if start_position == 0 {
			db.get(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to get the log entry"))?
				.map(|value| (key.clone(), value))
		} else {
			db.get_lower_than_or_equal_to(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to get the log entry"))?
				.and_then(|(key, value)| (key >= start.as_slice()).then(|| (key.to_vec(), value)))
		};
		let Some((mut current_key, first_value)) = entry else {
			return Ok(Vec::new());
		};
		let end = StoreKey::Log(Key::Entry {
			position: u64::MAX,
			process: &arg.process,
		})
		.pack_to_vec();
		let mut current: Option<log::read::Entry<'static>> = None;
		let mut output = Vec::new();
		let mut remaining = arg.length;
		let mut value = Some(first_value);
		while remaining > 0 {
			let value = if let Some(value) = value.take() {
				value
			} else {
				let Some((key, value)) = db
					.get_greater_than(transaction, &current_key)
					.map_err(|error| tg::error!(!error, "failed to get the next log entry"))?
				else {
					break;
				};
				if key > end.as_slice() {
					break;
				}
				current_key = key.to_vec();
				value
			};
			let chunk = tangram_serialize::from_slice::<log::read::Entry<'_>>(value)
				.map_err(|error| tg::error!(!error, "failed to deserialize the log entry"))?;
			if !arg.streams.contains(&chunk.stream) {
				continue;
			}
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
			let bytes = if offset > 0 || take < chunk.bytes.len().to_u64().unwrap() {
				let start = offset.to_usize().unwrap();
				let end = (offset + take).to_usize().unwrap();
				chunk.bytes[start..end].to_vec()
			} else {
				chunk.bytes.into_owned()
			};
			if let Some(entry) = &mut current {
				if entry.stream == chunk.stream {
					entry.bytes.to_mut().extend_from_slice(&bytes);
				} else {
					output.push(current.take().unwrap());
					current = Some(log::read::Entry {
						bytes: Cow::Owned(bytes),
						position: chunk.position + offset,
						stream: chunk.stream,
						stream_position: chunk.stream_position + offset,
						timestamp: chunk.timestamp,
					});
				}
			} else {
				current = Some(log::read::Entry {
					bytes: Cow::Owned(bytes),
					position: chunk.position + offset,
					stream: chunk.stream,
					stream_position: chunk.stream_position + offset,
					timestamp: chunk.timestamp,
				});
			}
			remaining -= take;
		}
		if let Some(entry) = current {
			output.push(entry);
		}

		Ok(output)
	}

	fn delete_log_range_with_transaction(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		start: &[u8],
		end: &[u8],
	) -> tg::Result<()> {
		let mut current = start.to_vec();
		let mut keys = Vec::new();
		loop {
			let Some((key, _)) = db
				.get_greater_than_or_equal_to(transaction, &current)
				.map_err(|error| tg::error!(!error, "failed to iterate the log entries"))?
			else {
				break;
			};
			if key > end {
				break;
			}
			keys.push(key.to_vec());
			current = key.to_vec();
			current.push(0);
		}
		for key in keys {
			db.delete(transaction, &key)
				.map_err(|error| tg::error!(!error, "failed to delete the log entry"))?;
		}

		Ok(())
	}

	fn validate_log_streams(
		streams: &std::collections::BTreeSet<tg::process::stdio::Stream>,
	) -> tg::Result<()> {
		if streams.is_empty() {
			return Err(tg::error!("expected at least one log stream"));
		}
		if streams.len() > 2 {
			return Err(tg::error!("invalid log streams"));
		}
		if streams.contains(&tg::process::stdio::Stream::Stdin) {
			return Err(tg::error!("invalid log streams"));
		}

		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use {
		super::*,
		bytes::Bytes,
		std::{collections::BTreeSet, path::Path},
	};

	fn store(path: &Path) -> Store {
		let config = super::super::Config {
			map_size: 10 * 1024 * 1024,
			path: path.join("test.lmdb"),
			posix_sem_prefix: None,
			read_batch_size: 64,
			read_concurrency: 4,
			write_batch_size: 8_000,
		};
		Store::new(&config).unwrap()
	}

	fn streams(
		streams: impl IntoIterator<Item = tg::process::stdio::Stream>,
	) -> BTreeSet<tg::process::stdio::Stream> {
		streams.into_iter().collect()
	}

	async fn put(
		store: &Store,
		process: &tg::process::Id,
		bytes: &'static [u8],
		position: u64,
		stream: tg::process::stdio::Stream,
		stream_position: u64,
	) {
		let arg = log::put::Arg {
			bytes: Bytes::from_static(bytes),
			position,
			process: process.clone(),
			stream,
			stream_position,
			timestamp: i64::try_from(position).unwrap(),
		};
		store.put_log(arg).await.unwrap();
	}

	fn bytes(entries: &[log::read::Entry<'_>]) -> Bytes {
		entries
			.iter()
			.flat_map(|entry| entry.bytes.iter().copied())
			.collect::<Vec<_>>()
			.into()
	}

	#[tokio::test]
	async fn read_and_length() {
		let temp = tangram_util::fs::Temp::new().unwrap();
		std::fs::create_dir(temp.path()).unwrap();
		let store = store(temp.path());
		let process = tg::process::Id::new();
		put(
			&store,
			&process,
			b"abc",
			0,
			tg::process::stdio::Stream::Stdout,
			0,
		)
		.await;
		put(
			&store,
			&process,
			b"de",
			3,
			tg::process::stdio::Stream::Stderr,
			0,
		)
		.await;
		put(
			&store,
			&process,
			b"fghi",
			5,
			tg::process::stdio::Stream::Stdout,
			3,
		)
		.await;

		let combined_streams = streams([
			tg::process::stdio::Stream::Stderr,
			tg::process::stdio::Stream::Stdout,
		]);
		let entries = store
			.try_read_log(log::read::Arg {
				length: 6,
				position: 1,
				process: process.clone(),
				streams: combined_streams.clone(),
			})
			.await
			.unwrap();
		assert_eq!(bytes(&entries), Bytes::from_static(b"bcdefg"));

		let stdout_streams = streams([tg::process::stdio::Stream::Stdout]);
		let entries = store
			.try_read_log(log::read::Arg {
				length: 4,
				position: 2,
				process: process.clone(),
				streams: stdout_streams.clone(),
			})
			.await
			.unwrap();
		assert_eq!(bytes(&entries), Bytes::from_static(b"cfgh"));

		let length = store
			.try_get_log_length(log::length::Arg {
				process: process.clone(),
				streams: combined_streams,
			})
			.await
			.unwrap();
		assert_eq!(length, Some(9));
		let length = store
			.try_get_log_length(log::length::Arg {
				process,
				streams: stdout_streams,
			})
			.await
			.unwrap();
		assert_eq!(length, Some(7));
	}

	#[tokio::test]
	async fn retry_and_delete() {
		let temp = tangram_util::fs::Temp::new().unwrap();
		std::fs::create_dir(temp.path()).unwrap();
		let store = store(temp.path());
		let process = tg::process::Id::new();
		for _ in 0..2 {
			put(
				&store,
				&process,
				b"hello",
				0,
				tg::process::stdio::Stream::Stdout,
				0,
			)
			.await;
		}
		let streams = streams([tg::process::stdio::Stream::Stdout]);
		let entries = store
			.try_read_log(log::read::Arg {
				length: u64::MAX,
				position: 0,
				process: process.clone(),
				streams: streams.clone(),
			})
			.await
			.unwrap();
		assert_eq!(bytes(&entries), Bytes::from_static(b"hello"));

		store
			.delete_log(log::delete::Arg {
				process: process.clone(),
			})
			.await
			.unwrap();
		let entries = store
			.try_read_log(log::read::Arg {
				length: u64::MAX,
				position: 0,
				process: process.clone(),
				streams: streams.clone(),
			})
			.await
			.unwrap();
		assert!(entries.is_empty());
		let length = store
			.try_get_log_length(log::length::Arg { process, streams })
			.await
			.unwrap();
		assert_eq!(length, None);
	}
}
