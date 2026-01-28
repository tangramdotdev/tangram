use {
	super::{
		CacheEntryCoreField, CacheEntryField, Index, ItemKind, Key, Kind, ObjectCoreField,
		ObjectField, ProcessCoreField, ProcessField,
	},
	crate::CleanOutput,
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	num_traits::ToPrimitive as _,
	tangram_client::prelude::*,
	tangram_util::varint,
};

struct Candidate {
	touched_at: i64,
	item: Item,
}

#[derive(Clone)]
enum Item {
	CacheEntry(tg::artifact::Id),
	Object(tg::object::Id),
	Process(tg::process::Id),
}

impl Index {
	pub async fn clean(&self, max_touched_at: i64, batch_size: usize) -> tg::Result<CleanOutput> {
		self.database
			.run(|txn, _| {
				let this = self.clone();
				async move {
					this.clean_inner(&txn, max_touched_at, batch_size)
						.await
						.map_err(|source| fdb::FdbBindingError::CustomError(source.into()))
				}
			})
			.await
			.map_err(|source| tg::error!(!source, "failed to run clean transaction"))
	}

	async fn clean_inner(
		&self,
		txn: &fdb::Transaction,
		max_touched_at: i64,
		batch_size: usize,
	) -> tg::Result<CleanOutput> {
		txn.set_option(fdb::options::TransactionOption::PriorityBatch)
			.unwrap();

		let mut output = CleanOutput::default();

		let begin = self.pack(&(Kind::Clean.to_i32().unwrap(), 0));
		let end = self.pack(&(Kind::Clean.to_i32().unwrap(), max_touched_at + 1));
		let range = fdb::RangeOption {
			begin: fdb::KeySelector::first_greater_or_equal(&begin),
			end: fdb::KeySelector::first_greater_or_equal(&end),
			limit: Some(batch_size),
			mode: fdb::options::StreamingMode::WantAll,
			..Default::default()
		};
		let entries = txn
			.get_range(&range, 1, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get range"))?;
		let candidates = entries
			.iter()
			.map(|entry| {
				let key = self
					.unpack(entry.key())
					.map_err(|source| tg::error!(!source, "failed to unpack key"))?;
				let Key::Clean {
					touched_at,
					kind,
					id,
				} = key
				else {
					return Err(tg::error!("expected clean key"));
				};
				let item = match kind {
					ItemKind::CacheEntry => {
						let tg::Either::Left(object_id) = id else {
							return Err(tg::error!("expected object id for cache entry"));
						};
						let artifact_id = tg::artifact::Id::try_from(object_id)
							.map_err(|source| tg::error!(!source, "invalid artifact id"))?;
						Item::CacheEntry(artifact_id)
					},
					ItemKind::Object => {
						let tg::Either::Left(object_id) = id else {
							return Err(tg::error!("expected object id"));
						};
						Item::Object(object_id)
					},
					ItemKind::Process => {
						let tg::Either::Right(process_id) = id else {
							return Err(tg::error!("expected process id"));
						};
						Item::Process(process_id)
					},
				};
				Ok(Candidate { touched_at, item })
			})
			.collect::<tg::Result<Vec<_>>>()?;

		for candidate in &candidates {
			let reference_count = match &candidate.item {
				Item::CacheEntry(id) => self.compute_cache_entry_reference_count(txn, id).await?,
				Item::Object(id) => self.compute_object_reference_count(txn, id).await?,
				Item::Process(id) => self.compute_process_reference_count(txn, id).await?,
			};

			if reference_count > 0 {
				self.set_reference_count(txn, &candidate.item, reference_count);
			} else {
				self.delete_item(txn, &candidate.item).await?;
				match &candidate.item {
					Item::CacheEntry(id) => output.cache_entries.push(id.clone()),
					Item::Object(id) => output.objects.push(id.clone()),
					Item::Process(id) => output.processes.push(id.clone()),
				}
			}

			let (kind, id) = match &candidate.item {
				Item::CacheEntry(id) => (ItemKind::CacheEntry, tg::Either::Left(id.clone().into())),
				Item::Object(id) => (ItemKind::Object, tg::Either::Left(id.clone())),
				Item::Process(id) => (ItemKind::Process, tg::Either::Right(id.clone())),
			};
			let key = Key::Clean {
				touched_at: candidate.touched_at,
				kind,
				id,
			};
			let key = self.pack(&key);
			txn.clear(&key);
		}

		output.done = candidates.is_empty();

		Ok(output)
	}

	async fn compute_cache_entry_reference_count(
		&self,
		txn: &fdb::Transaction,
		id: &tg::artifact::Id,
	) -> tg::Result<u64> {
		let id = id.to_bytes();
		let prefix = (Kind::CacheEntryObject.to_i32().unwrap(), id.as_ref());
		let prefix = self.pack(&prefix);
		let subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&subspace)
		};
		let count = txn
			.get_range(&range, 1, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get range"))?
			.len()
			.to_u64()
			.unwrap();
		Ok(count)
	}

	async fn compute_object_reference_count(
		&self,
		txn: &fdb::Transaction,
		id: &tg::object::Id,
	) -> tg::Result<u64> {
		let child_object_future = async {
			let id = id.to_bytes();
			let prefix = (Kind::ChildObject.to_i32().unwrap(), id.as_ref());
			let prefix = self.pack(&prefix);
			let subspace = Subspace::from_bytes(prefix);
			let range = fdb::RangeOption {
				mode: fdb::options::StreamingMode::WantAll,
				..fdb::RangeOption::from(&subspace)
			};
			let count = txn
				.get_range(&range, 1, false)
				.await
				.map_err(|source| tg::error!(!source, "failed to get range"))?
				.len()
				.to_u64()
				.unwrap();
			Ok::<_, tg::Error>(count)
		};
		let object_process_future = async {
			let id = id.to_bytes();
			let prefix = (Kind::ObjectProcess.to_i32().unwrap(), id.as_ref());
			let prefix = self.pack(&prefix);
			let subspace = Subspace::from_bytes(prefix);
			let range = fdb::RangeOption {
				mode: fdb::options::StreamingMode::WantAll,
				..fdb::RangeOption::from(&subspace)
			};
			let count = txn
				.get_range(&range, 1, false)
				.await
				.map_err(|source| tg::error!(!source, "failed to get range"))?
				.len()
				.to_u64()
				.unwrap();
			Ok::<_, tg::Error>(count)
		};
		let item_tag_future = async {
			let id = id.to_bytes();
			let prefix = (Kind::ItemTag.to_i32().unwrap(), id.as_ref());
			let prefix = self.pack(&prefix);
			let subspace = Subspace::from_bytes(prefix);
			let range = fdb::RangeOption {
				mode: fdb::options::StreamingMode::WantAll,
				..fdb::RangeOption::from(&subspace)
			};
			let count = txn
				.get_range(&range, 1, false)
				.await
				.map_err(|source| tg::error!(!source, "failed to get range"))?
				.len()
				.to_u64()
				.unwrap();
			Ok::<_, tg::Error>(count)
		};
		let (child_object_count, object_process_count, item_tag_count) =
			futures::future::try_join3(child_object_future, object_process_future, item_tag_future)
				.await?;
		let count = child_object_count + object_process_count + item_tag_count;
		Ok(count)
	}

	async fn compute_process_reference_count(
		&self,
		txn: &fdb::Transaction,
		id: &tg::process::Id,
	) -> tg::Result<u64> {
		let child_process_future = async {
			let id = id.to_bytes();
			let prefix = (Kind::ChildProcess.to_i32().unwrap(), id.as_ref());
			let prefix = self.pack(&prefix);
			let subspace = Subspace::from_bytes(prefix);
			let range = fdb::RangeOption {
				mode: fdb::options::StreamingMode::WantAll,
				..fdb::RangeOption::from(&subspace)
			};
			let count = txn
				.get_range(&range, 1, false)
				.await
				.map_err(|source| tg::error!(!source, "failed to get range"))?
				.len()
				.to_u64()
				.unwrap();
			Ok::<_, tg::Error>(count)
		};
		let item_tag_future = async {
			let id = id.to_bytes();
			let prefix = (Kind::ItemTag.to_i32().unwrap(), id.as_ref());
			let prefix = self.pack(&prefix);
			let subspace = Subspace::from_bytes(prefix);
			let range = fdb::RangeOption {
				mode: fdb::options::StreamingMode::WantAll,
				..fdb::RangeOption::from(&subspace)
			};
			let count = txn
				.get_range(&range, 1, false)
				.await
				.map_err(|source| tg::error!(!source, "failed to get range"))?
				.len()
				.to_u64()
				.unwrap();
			Ok::<_, tg::Error>(count)
		};
		let (child_process_count, item_tag_count) =
			futures::future::try_join(child_process_future, item_tag_future).await?;
		let count = child_process_count + item_tag_count;
		Ok(count)
	}

	fn set_reference_count(&self, txn: &fdb::Transaction, item: &Item, reference_count: u64) {
		let key = match item {
			Item::CacheEntry(id) => Key::CacheEntry {
				id: id.clone(),
				field: CacheEntryField::Core(CacheEntryCoreField::ReferenceCount),
			},
			Item::Object(id) => Key::Object {
				id: id.clone(),
				field: ObjectField::Core(ObjectCoreField::ReferenceCount),
			},
			Item::Process(id) => Key::Process {
				id: id.clone(),
				field: ProcessField::Core(ProcessCoreField::ReferenceCount),
			},
		};
		let key = self.pack(&key);
		txn.set(&key, &varint::encode_uvarint(reference_count));
	}

	async fn delete_item(&self, txn: &fdb::Transaction, item: &Item) -> tg::Result<()> {
		match item {
			Item::CacheEntry(id) => self.delete_cache_entry(txn, id).await,
			Item::Object(id) => self.delete_object(txn, id).await,
			Item::Process(id) => self.delete_process(txn, id).await,
		}
	}

	async fn delete_cache_entry(
		&self,
		txn: &fdb::Transaction,
		id: &tg::artifact::Id,
	) -> tg::Result<()> {
		let id_bytes = id.to_bytes();

		let prefix = (Kind::CacheEntry.to_i32().unwrap(), id_bytes.as_ref());
		let prefix = self.pack(&prefix);
		let subspace = Subspace::from_bytes(prefix);
		let (begin, end) = subspace.range();
		txn.clear_range(&begin, &end);

		Ok(())
	}

	async fn delete_object(&self, txn: &fdb::Transaction, id: &tg::object::Id) -> tg::Result<()> {
		let key = Key::Object {
			id: id.clone(),
			field: ObjectField::Core(ObjectCoreField::CacheEntry),
		};
		let key = self.pack(&key);
		let cache_entry = txn
			.get(&key, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get cache entry"))?
			.map(|bytes| {
				tg::artifact::Id::from_slice(&bytes)
					.map_err(|source| tg::error!(!source, "invalid artifact id"))
			})
			.transpose()?;

		let id_bytes = id.to_bytes();

		let prefix = (Kind::Object.to_i32().unwrap(), id_bytes.as_ref());
		let prefix = self.pack(&prefix);
		let subspace = Subspace::from_bytes(prefix);
		let (begin, end) = subspace.range();
		txn.clear_range(&begin, &end);

		let prefix = (Kind::ObjectChild.to_i32().unwrap(), id_bytes.as_ref());
		let prefix = self.pack(&prefix);
		let subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&subspace)
		};
		let entries = txn
			.get_range(&range, 1, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get range"))?;
		let children = entries
			.iter()
			.map(|entry| {
				let key = self
					.unpack(entry.key())
					.map_err(|source| tg::error!(!source, "failed to unpack key"))?;
				let Key::ObjectChild { child, .. } = key else {
					return Err(tg::error!("expected object child key"));
				};
				Ok(child)
			})
			.collect::<tg::Result<Vec<_>>>()?;
		let (begin, end) = subspace.range();
		txn.clear_range(&begin, &end);
		for child in &children {
			let key = Key::ChildObject {
				child: child.clone(),
				object: id.clone(),
			};
			let key = self.pack(&key);
			txn.clear(&key);
		}
		for child in children {
			self.decrement_object_reference_count(txn, &child).await?;
		}

		if let Some(cache_entry) = &cache_entry {
			let key = Key::ObjectCacheEntry {
				object: id.clone(),
				cache_entry: cache_entry.clone(),
			};
			let key = self.pack(&key);
			txn.clear(&key);

			let key = Key::CacheEntryObject {
				cache_entry: cache_entry.clone(),
				object: id.clone(),
			};
			let key = self.pack(&key);
			txn.clear(&key);

			self.decrement_cache_entry_reference_count(txn, cache_entry)
				.await?;
		}

		Ok(())
	}

	async fn delete_process(&self, txn: &fdb::Transaction, id: &tg::process::Id) -> tg::Result<()> {
		let id_bytes = id.to_bytes();

		let prefix = (Kind::Process.to_i32().unwrap(), id_bytes.as_ref());
		let prefix = self.pack(&prefix);
		let subspace = Subspace::from_bytes(prefix);
		let (begin, end) = subspace.range();
		txn.clear_range(&begin, &end);

		let prefix = (Kind::ProcessChild.to_i32().unwrap(), id_bytes.as_ref());
		let prefix = self.pack(&prefix);
		let subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&subspace)
		};
		let entries = txn
			.get_range(&range, 1, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get range"))?;
		let children = entries
			.iter()
			.map(|entry| {
				let key = self
					.unpack(entry.key())
					.map_err(|source| tg::error!(!source, "failed to unpack key"))?;
				let Key::ProcessChild { child, .. } = key else {
					return Err(tg::error!("expected process child key"));
				};
				Ok(child)
			})
			.collect::<tg::Result<Vec<_>>>()?;
		let (begin, end) = subspace.range();
		txn.clear_range(&begin, &end);
		for child in &children {
			let key = Key::ChildProcess {
				child: child.clone(),
				parent: id.clone(),
			};
			let key = self.pack(&key);
			txn.clear(&key);
		}
		for child in children {
			self.decrement_process_reference_count(txn, &child).await?;
		}

		let prefix = (Kind::ProcessObject.to_i32().unwrap(), id_bytes.as_ref());
		let prefix = self.pack(&prefix);
		let subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&subspace)
		};
		let entries = txn
			.get_range(&range, 1, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get range"))?;
		let object_processes = entries
			.iter()
			.map(|entry| {
				let key = self
					.unpack(entry.key())
					.map_err(|source| tg::error!(!source, "failed to unpack key"))?;
				let Key::ProcessObject { kind, object, .. } = key else {
					return Err(tg::error!("expected process object key"));
				};
				Ok((object, kind))
			})
			.collect::<tg::Result<Vec<_>>>()?;
		let (begin, end) = subspace.range();
		txn.clear_range(&begin, &end);
		for (object, kind) in &object_processes {
			let key = Key::ObjectProcess {
				object: object.clone(),
				kind: *kind,
				process: id.clone(),
			};
			let key = self.pack(&key);
			txn.clear(&key);
		}
		for (object, _) in object_processes {
			self.decrement_object_reference_count(txn, &object).await?;
		}

		Ok(())
	}

	async fn decrement_cache_entry_reference_count(
		&self,
		txn: &fdb::Transaction,
		id: &tg::artifact::Id,
	) -> tg::Result<()> {
		let key = Key::CacheEntry {
			id: id.clone(),
			field: CacheEntryField::Core(CacheEntryCoreField::ReferenceCount),
		};
		let key = self.pack(&key);
		let reference_count = txn
			.get(&key, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get reference count"))?
			.map(|bytes| varint::decode_uvarint(&bytes).ok_or_else(|| tg::error!("invalid value")))
			.transpose()?
			.unwrap_or(0);
		if reference_count > 1 {
			txn.set(&key, &varint::encode_uvarint(reference_count - 1));
		} else {
			txn.clear(&key);
			let key = Key::CacheEntry {
				id: id.clone(),
				field: CacheEntryField::Core(CacheEntryCoreField::TouchedAt),
			};
			let key = self.pack(&key);
			let touched_at = txn
				.get(&key, false)
				.await
				.map_err(|source| tg::error!(!source, "failed to get touched at"))?
				.map(|bytes| {
					let bytes = bytes
						.as_ref()
						.try_into()
						.map_err(|_| tg::error!("invalid value"))?;
					let value = i64::from_le_bytes(bytes);
					Ok::<_, tg::Error>(value)
				})
				.transpose()?
				.unwrap_or(0);

			let key = Key::Clean {
				touched_at,
				kind: ItemKind::CacheEntry,
				id: tg::Either::Left(id.clone().into()),
			};
			let key = self.pack(&key);
			txn.set(&key, &[]);
		}
		Ok(())
	}

	pub(super) async fn decrement_object_reference_count(
		&self,
		txn: &fdb::Transaction,
		id: &tg::object::Id,
	) -> tg::Result<()> {
		let key = Key::Object {
			id: id.clone(),
			field: ObjectField::Core(ObjectCoreField::ReferenceCount),
		};
		let key = self.pack(&key);
		let reference_count = txn
			.get(&key, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get reference count"))?
			.map(|bytes| varint::decode_uvarint(&bytes).ok_or_else(|| tg::error!("invalid value")))
			.transpose()?
			.unwrap_or(0);
		if reference_count > 1 {
			txn.set(&key, &varint::encode_uvarint(reference_count - 1));
		} else {
			txn.clear(&key);
			let key = Key::Object {
				id: id.clone(),
				field: ObjectField::Core(ObjectCoreField::TouchedAt),
			};
			let key = self.pack(&key);
			let touched_at = txn
				.get(&key, false)
				.await
				.map_err(|source| tg::error!(!source, "failed to get touched at"))?
				.map(|bytes| {
					let bytes = bytes
						.as_ref()
						.try_into()
						.map_err(|_| tg::error!("invalid value"))?;
					let value = i64::from_le_bytes(bytes);
					Ok::<_, tg::Error>(value)
				})
				.transpose()?
				.unwrap_or(0);

			let key = Key::Clean {
				touched_at,
				kind: ItemKind::Object,
				id: tg::Either::Left(id.clone()),
			};
			let key = self.pack(&key);
			txn.set(&key, &[]);
		}
		Ok(())
	}

	pub(super) async fn decrement_process_reference_count(
		&self,
		txn: &fdb::Transaction,
		id: &tg::process::Id,
	) -> tg::Result<()> {
		let key = Key::Process {
			id: id.clone(),
			field: ProcessField::Core(ProcessCoreField::ReferenceCount),
		};
		let key = self.pack(&key);
		let reference_count = txn
			.get(&key, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get reference count"))?
			.map(|bytes| varint::decode_uvarint(&bytes).ok_or_else(|| tg::error!("invalid value")))
			.transpose()?
			.unwrap_or(0);
		if reference_count > 1 {
			txn.set(&key, &varint::encode_uvarint(reference_count - 1));
		} else {
			txn.clear(&key);
			let key = Key::Process {
				id: id.clone(),
				field: ProcessField::Core(ProcessCoreField::TouchedAt),
			};
			let key = self.pack(&key);
			let touched_at = txn
				.get(&key, false)
				.await
				.map_err(|source| tg::error!(!source, "failed to get touched at"))?
				.map(|bytes| {
					let bytes = bytes
						.as_ref()
						.try_into()
						.map_err(|_| tg::error!("invalid value"))?;
					let value = i64::from_le_bytes(bytes);
					Ok::<_, tg::Error>(value)
				})
				.transpose()?
				.unwrap_or(0);

			let key = Key::Clean {
				touched_at,
				kind: ItemKind::Process,
				id: tg::Either::Right(id.clone()),
			};
			let key = self.pack(&key);
			txn.set(&key, &[]);
		}
		Ok(())
	}
}
