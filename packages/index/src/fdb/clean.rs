use {
	super::{
		CacheEntryCoreField, CacheEntryField, Index, ItemKind, Key, KeyKind, ObjectCoreField,
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
	partition: u64,
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
	pub async fn clean(
		&self,
		max_touched_at: i64,
		batch_size: usize,
		partition_start: u64,
		partition_count: u64,
	) -> tg::Result<CleanOutput> {
		let partition_total = self.partition_total;
		let result = self
			.database
			.run(|txn, _| {
				let subspace = self.subspace.clone();
				async move {
					Self::clean_inner(
						&txn,
						&subspace,
						max_touched_at,
						batch_size,
						partition_start,
						partition_count,
						partition_total,
					)
					.await
					.map_err(|source| fdb::FdbBindingError::CustomError(source.into()))
				}
			})
			.await;

		let output = match result {
			Ok(output) => output,
			Err(fdb::FdbBindingError::NonRetryableFdbError(error))
				if error.code() == 2101 && batch_size > 1 =>
			{
				let half = batch_size / 2;
				let first =
					Box::pin(self.clean(max_touched_at, half, partition_start, partition_count))
						.await?;
				let second =
					Box::pin(self.clean(max_touched_at, half, partition_start, partition_count))
						.await?;
				CleanOutput {
					bytes: first.bytes + second.bytes,
					cache_entries: first
						.cache_entries
						.into_iter()
						.chain(second.cache_entries)
						.collect(),
					objects: first.objects.into_iter().chain(second.objects).collect(),
					processes: first
						.processes
						.into_iter()
						.chain(second.processes)
						.collect(),
					done: first.done && second.done,
				}
			},
			Err(error) => {
				return Err(tg::error!(!error, "failed to run clean transaction"));
			},
		};

		Ok(output)
	}

	#[allow(clippy::too_many_arguments)]
	async fn clean_inner(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		max_touched_at: i64,
		batch_size: usize,
		partition_start: u64,
		partition_count: u64,
		partition_total: u64,
	) -> tg::Result<CleanOutput> {
		txn.set_option(fdb::options::TransactionOption::PriorityBatch)
			.unwrap();

		let mut output = CleanOutput::default();
		let mut candidates = Vec::new();

		let key_kind = KeyKind::Clean.to_i32().unwrap();
		let partition_end = partition_start.saturating_add(partition_count);
		for partition in partition_start..partition_end {
			let begin = Self::pack(subspace, &(key_kind, partition, 0i64));
			let end = Self::pack(subspace, &(key_kind, partition, max_touched_at + 1));
			let remaining = batch_size.saturating_sub(candidates.len());
			if remaining == 0 {
				break;
			}
			let range = fdb::RangeOption {
				begin: fdb::KeySelector::first_greater_or_equal(&begin),
				end: fdb::KeySelector::first_greater_or_equal(&end),
				limit: Some(remaining),
				mode: fdb::options::StreamingMode::WantAll,
				..Default::default()
			};
			let entries = txn
				.get_range(&range, 1, false)
				.await
				.map_err(|source| tg::error!(!source, "failed to get range"))?;
			for entry in &entries {
				let key = Self::unpack(subspace, entry.key())
					.map_err(|source| tg::error!(!source, "failed to unpack key"))?;
				let Key::Clean {
					partition,
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
				candidates.push(Candidate {
					partition,
					touched_at,
					item,
				});
			}
		}

		for candidate in &candidates {
			let reference_count = match &candidate.item {
				Item::CacheEntry(id) => {
					Self::compute_cache_entry_reference_count(txn, subspace, id).await?
				},
				Item::Object(id) => Self::compute_object_reference_count(txn, subspace, id).await?,
				Item::Process(id) => {
					Self::compute_process_reference_count(txn, subspace, id).await?
				},
			};

			if reference_count > 0 {
				Self::set_reference_count(txn, subspace, &candidate.item, reference_count);
			} else {
				Self::delete_item(txn, subspace, &candidate.item, partition_total).await?;
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
				partition: candidate.partition,
				touched_at: candidate.touched_at,
				kind,
				id,
			};
			let key = Self::pack(subspace, &key);
			txn.clear(&key);
		}

		output.done = candidates.is_empty();

		Ok(output)
	}

	async fn compute_cache_entry_reference_count(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::artifact::Id,
	) -> tg::Result<u64> {
		let id = id.to_bytes();
		let prefix = (KeyKind::CacheEntryObject.to_i32().unwrap(), id.as_ref());
		let prefix = Self::pack(subspace, &prefix);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace)
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
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::object::Id,
	) -> tg::Result<u64> {
		let child_object_future = async {
			let id = id.to_bytes();
			let prefix = (KeyKind::ChildObject.to_i32().unwrap(), id.as_ref());
			let prefix = Self::pack(subspace, &prefix);
			let range_subspace = Subspace::from_bytes(prefix);
			let range = fdb::RangeOption {
				mode: fdb::options::StreamingMode::WantAll,
				..fdb::RangeOption::from(&range_subspace)
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
			let prefix = (KeyKind::ObjectProcess.to_i32().unwrap(), id.as_ref());
			let prefix = Self::pack(subspace, &prefix);
			let range_subspace = Subspace::from_bytes(prefix);
			let range = fdb::RangeOption {
				mode: fdb::options::StreamingMode::WantAll,
				..fdb::RangeOption::from(&range_subspace)
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
			let prefix = (KeyKind::ItemTag.to_i32().unwrap(), id.as_ref());
			let prefix = Self::pack(subspace, &prefix);
			let range_subspace = Subspace::from_bytes(prefix);
			let range = fdb::RangeOption {
				mode: fdb::options::StreamingMode::WantAll,
				..fdb::RangeOption::from(&range_subspace)
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
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::process::Id,
	) -> tg::Result<u64> {
		let child_process_future = async {
			let id = id.to_bytes();
			let prefix = (KeyKind::ChildProcess.to_i32().unwrap(), id.as_ref());
			let prefix = Self::pack(subspace, &prefix);
			let range_subspace = Subspace::from_bytes(prefix);
			let range = fdb::RangeOption {
				mode: fdb::options::StreamingMode::WantAll,
				..fdb::RangeOption::from(&range_subspace)
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
			let prefix = (KeyKind::ItemTag.to_i32().unwrap(), id.as_ref());
			let prefix = Self::pack(subspace, &prefix);
			let range_subspace = Subspace::from_bytes(prefix);
			let range = fdb::RangeOption {
				mode: fdb::options::StreamingMode::WantAll,
				..fdb::RangeOption::from(&range_subspace)
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

	fn set_reference_count(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		item: &Item,
		reference_count: u64,
	) {
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
		let key = Self::pack(subspace, &key);
		txn.set(&key, &varint::encode_uvarint(reference_count));
	}

	async fn delete_item(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		item: &Item,
		partition_total: u64,
	) -> tg::Result<()> {
		match item {
			Item::CacheEntry(id) => Self::delete_cache_entry(txn, subspace, id).await,
			Item::Object(id) => Self::delete_object(txn, subspace, id, partition_total).await,
			Item::Process(id) => Self::delete_process(txn, subspace, id, partition_total).await,
		}
	}

	async fn delete_cache_entry(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::artifact::Id,
	) -> tg::Result<()> {
		let id_bytes = id.to_bytes();

		let prefix = (KeyKind::CacheEntry.to_i32().unwrap(), id_bytes.as_ref());
		let prefix = Self::pack(subspace, &prefix);
		let range_subspace = Subspace::from_bytes(prefix);
		let (begin, end) = range_subspace.range();
		txn.clear_range(&begin, &end);

		Ok(())
	}

	async fn delete_object(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::object::Id,
		partition_total: u64,
	) -> tg::Result<()> {
		let key = Key::Object {
			id: id.clone(),
			field: ObjectField::Core(ObjectCoreField::CacheEntry),
		};
		let key = Self::pack(subspace, &key);
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

		let prefix = (KeyKind::Object.to_i32().unwrap(), id_bytes.as_ref());
		let prefix = Self::pack(subspace, &prefix);
		let range_subspace = Subspace::from_bytes(prefix);
		let (begin, end) = range_subspace.range();
		txn.clear_range(&begin, &end);

		let prefix = (KeyKind::ObjectChild.to_i32().unwrap(), id_bytes.as_ref());
		let prefix = Self::pack(subspace, &prefix);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace)
		};
		let entries = txn
			.get_range(&range, 1, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get range"))?;
		let children = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())
					.map_err(|source| tg::error!(!source, "failed to unpack key"))?;
				let Key::ObjectChild { child, .. } = key else {
					return Err(tg::error!("expected object child key"));
				};
				Ok(child)
			})
			.collect::<tg::Result<Vec<_>>>()?;
		let (begin, end) = range_subspace.range();
		txn.clear_range(&begin, &end);
		for child in &children {
			let key = Key::ChildObject {
				child: child.clone(),
				object: id.clone(),
			};
			let key = Self::pack(subspace, &key);
			txn.clear(&key);
		}
		for child in children {
			Self::decrement_object_reference_count(txn, subspace, &child, partition_total).await?;
		}

		if let Some(cache_entry) = &cache_entry {
			let key = Key::ObjectCacheEntry {
				object: id.clone(),
				cache_entry: cache_entry.clone(),
			};
			let key = Self::pack(subspace, &key);
			txn.clear(&key);

			let key = Key::CacheEntryObject {
				cache_entry: cache_entry.clone(),
				object: id.clone(),
			};
			let key = Self::pack(subspace, &key);
			txn.clear(&key);

			Self::decrement_cache_entry_reference_count(
				txn,
				subspace,
				cache_entry,
				partition_total,
			)
			.await?;
		}

		Ok(())
	}

	async fn delete_process(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::process::Id,
		partition_total: u64,
	) -> tg::Result<()> {
		let id_bytes = id.to_bytes();

		let prefix = (KeyKind::Process.to_i32().unwrap(), id_bytes.as_ref());
		let prefix = Self::pack(subspace, &prefix);
		let range_subspace = Subspace::from_bytes(prefix);
		let (begin, end) = range_subspace.range();
		txn.clear_range(&begin, &end);

		let prefix = (KeyKind::ProcessChild.to_i32().unwrap(), id_bytes.as_ref());
		let prefix = Self::pack(subspace, &prefix);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace)
		};
		let entries = txn
			.get_range(&range, 1, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get range"))?;
		let children = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())
					.map_err(|source| tg::error!(!source, "failed to unpack key"))?;
				let Key::ProcessChild { child, .. } = key else {
					return Err(tg::error!("expected process child key"));
				};
				Ok(child)
			})
			.collect::<tg::Result<Vec<_>>>()?;
		let (begin, end) = range_subspace.range();
		txn.clear_range(&begin, &end);
		for child in &children {
			let key = Key::ChildProcess {
				child: child.clone(),
				parent: id.clone(),
			};
			let key = Self::pack(subspace, &key);
			txn.clear(&key);
		}
		for child in children {
			Self::decrement_process_reference_count(txn, subspace, &child, partition_total).await?;
		}

		let prefix = (KeyKind::ProcessObject.to_i32().unwrap(), id_bytes.as_ref());
		let prefix = Self::pack(subspace, &prefix);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace)
		};
		let entries = txn
			.get_range(&range, 1, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get range"))?;
		let object_processes = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())
					.map_err(|source| tg::error!(!source, "failed to unpack key"))?;
				let Key::ProcessObject { kind, object, .. } = key else {
					return Err(tg::error!("expected process object key"));
				};
				Ok((object, kind))
			})
			.collect::<tg::Result<Vec<_>>>()?;
		let (begin, end) = range_subspace.range();
		txn.clear_range(&begin, &end);
		for (object, kind) in &object_processes {
			let key = Key::ObjectProcess {
				object: object.clone(),
				kind: *kind,
				process: id.clone(),
			};
			let key = Self::pack(subspace, &key);
			txn.clear(&key);
		}
		for (object, _) in object_processes {
			Self::decrement_object_reference_count(txn, subspace, &object, partition_total).await?;
		}

		Ok(())
	}

	async fn decrement_cache_entry_reference_count(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::artifact::Id,
		partition_total: u64,
	) -> tg::Result<()> {
		let key = Key::CacheEntry {
			id: id.clone(),
			field: CacheEntryField::Core(CacheEntryCoreField::ReferenceCount),
		};
		let key = Self::pack(subspace, &key);
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
			let key = Self::pack(subspace, &key);
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

			let id_bytes = id.to_bytes();
			let partition = Self::partition_for_id(id_bytes.as_ref(), partition_total);
			let key = Key::Clean {
				partition,
				touched_at,
				kind: ItemKind::CacheEntry,
				id: tg::Either::Left(id.clone().into()),
			};
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);
		}
		Ok(())
	}

	pub(super) async fn decrement_object_reference_count(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::object::Id,
		partition_total: u64,
	) -> tg::Result<()> {
		let key = Key::Object {
			id: id.clone(),
			field: ObjectField::Core(ObjectCoreField::ReferenceCount),
		};
		let key = Self::pack(subspace, &key);
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
			let key = Self::pack(subspace, &key);
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

			let id_bytes = id.to_bytes();
			let partition = Self::partition_for_id(id_bytes.as_ref(), partition_total);
			let key = Key::Clean {
				partition,
				touched_at,
				kind: ItemKind::Object,
				id: tg::Either::Left(id.clone()),
			};
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);
		}
		Ok(())
	}

	pub(super) async fn decrement_process_reference_count(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::process::Id,
		partition_total: u64,
	) -> tg::Result<()> {
		let key = Key::Process {
			id: id.clone(),
			field: ProcessField::Core(ProcessCoreField::ReferenceCount),
		};
		let key = Self::pack(subspace, &key);
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
			let key = Self::pack(subspace, &key);
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

			let id_bytes = id.to_bytes();
			let partition = Self::partition_for_id(id_bytes.as_ref(), partition_total);
			let key = Key::Clean {
				partition,
				touched_at,
				kind: ItemKind::Process,
				id: tg::Either::Right(id.clone()),
			};
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);
		}
		Ok(())
	}
}
