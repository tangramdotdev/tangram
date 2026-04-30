use {
	crate::{DeleteArg, Entry, PutArg, ReadArg},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	futures::StreamExt as _,
	num::ToPrimitive as _,
	std::{borrow::Cow, collections::BTreeSet, path::PathBuf, sync::Arc},
	tangram_client::prelude::*,
};

#[derive(Clone, Debug)]
pub struct Options {
	pub cluster: PathBuf,
	pub prefix: Option<String>,
}

pub struct Store {
	database: Arc<fdb::Database>,
	sender: RequestSender,
	subspace: fdbt::Subspace,
}

type RequestSender = tokio::sync::mpsc::UnboundedSender<(Request, ResponseSender)>;
type RequestReceiver = tokio::sync::mpsc::UnboundedReceiver<(Request, ResponseSender)>;
type ResponseSender = tokio::sync::oneshot::Sender<tg::Result<()>>;

#[derive(Clone)]
enum Request {
	Delete(DeleteArg),
	Put(PutArg),
}

#[derive(Debug)]
enum Key<'a> {
	Entry(&'a tg::process::Id, u64),
	StreamPosition(&'a tg::process::Id, tg::process::stdio::Stream, u64),
}

impl Store {
	pub fn new(options: &Options) -> tg::Result<Self> {
		let database = fdb::Database::new(Some(options.cluster.to_str().unwrap()))
			.map_err(|source| tg::error!(!source, "failed to open the foundationdb cluster"))?;
		let database = Arc::new(database);

		let subspace = match &options.prefix {
			Some(prefix) => fdbt::Subspace::from_bytes(prefix.clone().into_bytes()),
			None => fdbt::Subspace::all(),
		};

		let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
		tokio::spawn({
			let database = database.clone();
			let subspace = subspace.clone();
			async move {
				Self::task(database, subspace, receiver).await;
			}
		});

		Ok(Self {
			database,
			sender,
			subspace,
		})
	}

	fn pack<T: fdbt::TuplePack>(subspace: &fdbt::Subspace, key: &T) -> Vec<u8> {
		subspace.pack(key)
	}

	fn entries_subspace(subspace: &fdbt::Subspace, id: &tg::process::Id) -> fdbt::Subspace {
		let id_bytes = id.to_bytes();
		let prefix = Self::pack(subspace, &(id_bytes.as_ref(), 0));
		fdbt::Subspace::from_bytes(prefix)
	}

	fn stream_positions_subspace(
		subspace: &fdbt::Subspace,
		id: &tg::process::Id,
		stream: tg::process::stdio::Stream,
	) -> tg::Result<fdbt::Subspace> {
		let kind = match stream {
			tg::process::stdio::Stream::Stdout => 1,
			tg::process::stdio::Stream::Stderr => 2,
			tg::process::stdio::Stream::Stdin => {
				return Err(tg::error!("invalid stdio stream"));
			},
		};
		let id_bytes = id.to_bytes();
		let prefix = Self::pack(subspace, &(id_bytes.as_ref(), kind));
		Ok(fdbt::Subspace::from_bytes(prefix))
	}

	fn entry_key(subspace: &fdbt::Subspace, id: &tg::process::Id, position: u64) -> Vec<u8> {
		Self::pack(subspace, &Key::Entry(id, position))
	}

	fn stream_position_key(
		subspace: &fdbt::Subspace,
		id: &tg::process::Id,
		stream: tg::process::stdio::Stream,
		position: u64,
	) -> tg::Result<Vec<u8>> {
		if stream == tg::process::stdio::Stream::Stdin {
			return Err(tg::error!("invalid stdio stream"));
		}
		Ok(Self::pack(
			subspace,
			&Key::StreamPosition(id, stream, position),
		))
	}

	async fn task(
		database: Arc<fdb::Database>,
		subspace: fdbt::Subspace,
		mut receiver: RequestReceiver,
	) {
		while let Some((request, sender)) = receiver.recv().await {
			let result = match request {
				Request::Delete(arg) => {
					Self::execute_delete_request(&database, &subspace, arg).await
				},
				Request::Put(arg) => Self::execute_put_request(&database, &subspace, arg).await,
			};
			sender.send(result).ok();
		}
	}

	async fn execute_delete_request(
		database: &fdb::Database,
		subspace: &fdbt::Subspace,
		arg: DeleteArg,
	) -> tg::Result<()> {
		database
			.run(|txn, _maybe_committed| {
				let arg = arg.clone();
				let subspace = subspace.clone();
				async move {
					Self::task_delete(&txn, &subspace, &arg)
						.map_err(|source| fdb::FdbBindingError::CustomError(source.into()))?;
					Ok(())
				}
			})
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the request"))?;
		Ok(())
	}

	async fn execute_put_request(
		database: &fdb::Database,
		subspace: &fdbt::Subspace,
		arg: PutArg,
	) -> tg::Result<()> {
		database
			.run(|txn, _maybe_committed| {
				let arg = arg.clone();
				let subspace = subspace.clone();
				async move {
					Self::task_put(&txn, &subspace, &arg)
						.await
						.map_err(|source| fdb::FdbBindingError::CustomError(source.into()))?;
					Ok(())
				}
			})
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the request"))?;
		Ok(())
	}

	async fn get_entry_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		id: &tg::process::Id,
		position: u64,
	) -> tg::Result<Option<Entry<'static>>> {
		let key = Self::entry_key(subspace, id, position);
		let bytes = txn
			.get(&key, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the log entry"))?;
		let Some(bytes) = bytes else {
			return Ok(None);
		};
		let entry = tangram_serialize::from_slice::<Entry>(&bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize the log entry"))?;
		Ok(Some(entry.into_static()))
	}

	async fn get_last_entry_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		id: &tg::process::Id,
	) -> tg::Result<Option<Entry<'static>>> {
		let range_subspace = Self::entries_subspace(subspace, id);
		let range = fdb::RangeOption {
			limit: Some(1),
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace).rev()
		};
		let mut entries = txn.get_ranges_keyvalues(range, false);
		let Some(entry) = entries
			.next()
			.await
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to get the last log entry"))?
		else {
			return Ok(None);
		};
		let entry = tangram_serialize::from_slice::<Entry>(entry.value())
			.map_err(|source| tg::error!(!source, "failed to deserialize the log entry"))?;
		Ok(Some(entry.into_static()))
	}

	async fn get_entry_at_or_before_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		id: &tg::process::Id,
		position: u64,
	) -> tg::Result<Option<Entry<'static>>> {
		let range_subspace = Self::entries_subspace(subspace, id);
		let (begin, _) = range_subspace.range();
		let end = Self::entry_key(subspace, id, position);
		let range = fdb::RangeOption {
			begin: fdb::KeySelector::first_greater_or_equal(begin),
			end: fdb::KeySelector::first_greater_than(end),
			limit: Some(1),
			mode: fdb::options::StreamingMode::WantAll,
			reverse: true,
			..fdb::RangeOption::default()
		};
		let mut entries = txn.get_ranges_keyvalues(range, false);
		let Some(entry) = entries
			.next()
			.await
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to get the log entry"))?
		else {
			return Ok(None);
		};
		let entry = tangram_serialize::from_slice::<Entry>(entry.value())
			.map_err(|source| tg::error!(!source, "failed to deserialize the log entry"))?;
		Ok(Some(entry.into_static()))
	}

	async fn get_stream_position_at_or_before_with_transaction(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		id: &tg::process::Id,
		stream: tg::process::stdio::Stream,
		position: u64,
	) -> tg::Result<Option<u64>> {
		let range_subspace = Self::stream_positions_subspace(subspace, id, stream)?;
		let (begin, _) = range_subspace.range();
		let end = Self::stream_position_key(subspace, id, stream, position)?;
		let range = fdb::RangeOption {
			begin: fdb::KeySelector::first_greater_or_equal(begin),
			end: fdb::KeySelector::first_greater_than(end),
			limit: Some(1),
			mode: fdb::options::StreamingMode::WantAll,
			reverse: true,
			..fdb::RangeOption::default()
		};
		let mut entries = txn.get_ranges_keyvalues(range, false);
		let Some(entry) = entries
			.next()
			.await
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to get the stream index"))?
		else {
			return Ok(None);
		};
		let position = u64::from_le_bytes(
			entry
				.value()
				.try_into()
				.map_err(|_| tg::error!("expected 8 bytes"))?,
		);
		Ok(Some(position))
	}

	pub async fn try_read(&self, arg: ReadArg) -> tg::Result<Vec<Entry<'static>>> {
		if arg.streams.is_empty() {
			return Err(tg::error!("expected at least one log stream"));
		}
		if arg.streams.len() > 2 {
			return Err(tg::error!("invalid log streams"));
		}
		if arg.streams.contains(&tg::process::stdio::Stream::Stdin) {
			return Err(tg::error!("invalid stdio stream"));
		}

		let txn = self
			.database
			.create_trx()
			.map_err(|source| tg::error!(!source, "failed to create the transaction"))?;
		let combined = arg.streams.len() > 1;

		let start_position = if combined {
			arg.position
		} else {
			let stream = arg.streams.iter().next().copied().unwrap();
			let Some(position) = Self::get_stream_position_at_or_before_with_transaction(
				&txn,
				&self.subspace,
				&arg.process,
				stream,
				arg.position,
			)
			.await?
			else {
				return Ok(Vec::new());
			};
			position
		};

		let Some(start_entry) = Self::get_entry_at_or_before_with_transaction(
			&txn,
			&self.subspace,
			&arg.process,
			start_position,
		)
		.await?
		else {
			return Ok(Vec::new());
		};
		let start_key = Self::entry_key(&self.subspace, &arg.process, start_entry.position);
		let range_subspace = Self::entries_subspace(&self.subspace, &arg.process);
		let (_, end) = range_subspace.range();
		let range = fdb::RangeOption {
			begin: fdb::KeySelector::first_greater_or_equal(start_key),
			end: fdb::KeySelector::first_greater_or_equal(end),
			mode: fdb::options::StreamingMode::Iterator,
			..fdb::RangeOption::default()
		};
		let mut entries = txn.get_ranges_keyvalues(range, false);

		let mut remaining = arg.length;
		let mut output = Vec::new();
		let mut current: Option<Entry<'static>> = None;

		while remaining > 0 {
			let Some(entry) = entries
				.next()
				.await
				.transpose()
				.map_err(|source| tg::error!(!source, "failed to get the next entry"))?
			else {
				break;
			};

			let chunk = tangram_serialize::from_slice::<Entry>(entry.value())
				.map_err(|source| tg::error!(!source, "failed to deserialize the log entry"))?;

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

			let bytes: Cow<'static, [u8]> =
				if offset > 0 || take < chunk.bytes.len().to_u64().unwrap() {
					let start = offset.to_usize().unwrap();
					let end = (offset + take).to_usize().unwrap();
					Cow::Owned(chunk.bytes[start..end].to_vec())
				} else {
					Cow::Owned(chunk.bytes.to_vec())
				};

			if let Some(ref mut entry) = current {
				if entry.stream == chunk.stream {
					let mut combined = entry.bytes.to_vec();
					combined.extend_from_slice(&bytes);
					entry.bytes = Cow::Owned(combined);
				} else {
					output.push(current.take().unwrap());
					current = Some(Entry {
						bytes,
						position: chunk.position + offset,
						stream_position: chunk.stream_position + offset,
						stream: chunk.stream,
						timestamp: chunk.timestamp,
					});
				}
			} else {
				current = Some(Entry {
					bytes,
					position: chunk.position + offset,
					stream_position: chunk.stream_position + offset,
					stream: chunk.stream,
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

	pub async fn try_get_length(
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

		let txn = self
			.database
			.create_trx()
			.map_err(|source| tg::error!(!source, "failed to create the transaction"))?;

		let length = if streams.len() == 1 {
			let stream = streams.iter().next().copied().unwrap();
			let Some(position) = Self::get_stream_position_at_or_before_with_transaction(
				&txn,
				&self.subspace,
				id,
				stream,
				u64::MAX,
			)
			.await?
			else {
				return Ok(None);
			};
			let Some(entry) =
				Self::get_entry_with_transaction(&txn, &self.subspace, id, position).await?
			else {
				return Ok(None);
			};
			entry.stream_position + entry.bytes.len().to_u64().unwrap()
		} else {
			let Some(entry) =
				Self::get_last_entry_with_transaction(&txn, &self.subspace, id).await?
			else {
				return Ok(None);
			};
			entry.position + entry.bytes.len().to_u64().unwrap()
		};

		Ok(Some(length))
	}

	pub async fn put(&self, arg: PutArg) -> tg::Result<()> {
		if arg.bytes.is_empty() {
			return Ok(());
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		self.sender
			.send((Request::Put(arg), sender))
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))?
	}

	pub async fn delete(&self, arg: DeleteArg) -> tg::Result<()> {
		let (sender, receiver) = tokio::sync::oneshot::channel();
		self.sender
			.send((Request::Delete(arg), sender))
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))?
	}

	async fn task_put(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &PutArg,
	) -> tg::Result<()> {
		let id = &arg.process;

		let position = Self::get_last_entry_with_transaction(txn, subspace, id)
			.await?
			.map_or(0, |entry| {
				entry.position + entry.bytes.len().to_u64().unwrap()
			});

		let stream_position = match Self::get_stream_position_at_or_before_with_transaction(
			txn,
			subspace,
			id,
			arg.stream,
			u64::MAX,
		)
		.await?
		{
			Some(position) => {
				match Self::get_entry_with_transaction(txn, subspace, id, position).await? {
					Some(entry) => entry.stream_position + entry.bytes.len().to_u64().unwrap(),
					None => 0,
				}
			},
			None => 0,
		};

		let entry = Entry {
			bytes: Cow::Owned(arg.bytes.to_vec()),
			position,
			stream_position,
			stream: arg.stream,
			timestamp: arg.timestamp,
		};

		let key = Self::entry_key(subspace, id, position);
		let value = tangram_serialize::to_vec(&entry).unwrap();
		txn.set(&key, &value);

		let key = Self::stream_position_key(subspace, id, arg.stream, stream_position)?;
		let value = position.to_le_bytes();
		txn.set(&key, &value);

		Ok(())
	}

	fn task_delete(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &DeleteArg,
	) -> tg::Result<()> {
		let entries = Self::entries_subspace(subspace, &arg.process);
		txn.clear_subspace_range(&entries);

		for stream in [
			tg::process::stdio::Stream::Stdout,
			tg::process::stdio::Stream::Stderr,
		] {
			let pointers = Self::stream_positions_subspace(subspace, &arg.process, stream)?;
			txn.clear_subspace_range(&pointers);
		}

		Ok(())
	}
}

impl crate::Store for Store {
	async fn try_read(&self, arg: ReadArg) -> tg::Result<Vec<Entry<'static>>> {
		self.try_read(arg).await
	}

	async fn try_get_length(
		&self,
		id: &tg::process::Id,
		streams: &BTreeSet<tg::process::stdio::Stream>,
	) -> tg::Result<Option<u64>> {
		self.try_get_length(id, streams).await
	}

	async fn put(&self, arg: PutArg) -> tg::Result<()> {
		self.put(arg).await
	}

	async fn delete(&self, arg: DeleteArg) -> tg::Result<()> {
		self.delete(arg).await
	}
}

impl fdbt::TuplePack for Key<'_> {
	fn pack<W: std::io::Write>(
		&self,
		w: &mut W,
		tuple_depth: fdbt::TupleDepth,
	) -> std::io::Result<fdbt::VersionstampOffset> {
		match self {
			Key::Entry(id, position) => (id.to_bytes().as_ref(), 0, *position).pack(w, tuple_depth),
			Key::StreamPosition(id, tg::process::stdio::Stream::Stdin, position) => {
				let _ = (id, position);
				Err(std::io::Error::new(
					std::io::ErrorKind::InvalidInput,
					"invalid stdio stream",
				))
			},
			Key::StreamPosition(id, tg::process::stdio::Stream::Stdout, position) => {
				(id.to_bytes().as_ref(), 1, *position).pack(w, tuple_depth)
			},
			Key::StreamPosition(id, tg::process::stdio::Stream::Stderr, position) => {
				(id.to_bytes().as_ref(), 2, *position).pack(w, tuple_depth)
			},
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use foundationdb_tuple::TuplePack as _;

	#[test]
	fn test_key_pack_matches_lmdb_layout() {
		let subspace = fdbt::Subspace::all();
		let id = tg::process::Id::new();
		let id_bytes = id.to_bytes();

		assert_eq!(
			Store::pack(&subspace, &Key::Entry(&id, 42)),
			(id_bytes.as_ref(), 0, 42u64).pack_to_vec(),
		);
		assert_eq!(
			Store::pack(
				&subspace,
				&Key::StreamPosition(&id, tg::process::stdio::Stream::Stdout, 7),
			),
			(id_bytes.as_ref(), 1, 7u64).pack_to_vec(),
		);
		assert_eq!(
			Store::pack(
				&subspace,
				&Key::StreamPosition(&id, tg::process::stdio::Stream::Stderr, 9),
			),
			(id_bytes.as_ref(), 2, 9u64).pack_to_vec(),
		);
	}

	#[test]
	fn test_prefix_is_applied_to_keys() {
		let subspace = fdbt::Subspace::from_bytes(b"logs_".to_vec());
		let id = tg::process::Id::new();
		let id_bytes = id.to_bytes();

		let mut expected = b"logs_".to_vec();
		expected.extend((id_bytes.as_ref(), 0, 1u64).pack_to_vec());

		assert_eq!(Store::pack(&subspace, &Key::Entry(&id, 1)), expected,);
	}
}
