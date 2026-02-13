use {
	super::{Index, ItemKind, Key, KeyKind, Request, Response},
	crate::{CacheEntry, CleanOutput, Object, Process},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	num_traits::ToPrimitive as _,
	tangram_client::prelude::*,
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
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::Clean {
			max_touched_at,
			batch_size,
			partition_start,
			partition_count,
		};
		self.sender_low
			.send((request, sender))
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		let response = receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))??;
		let Response::CleanOutput(output) = response else {
			return Err(tg::error!("unexpected response"));
		};
		Ok(output)
	}

	#[allow(clippy::too_many_arguments)]
	pub(super) async fn task_clean(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		max_touched_at: i64,
		batch_size: usize,
		partition_start: u64,
		partition_count: u64,
		partition_total: u64,
	) -> tg::Result<CleanOutput> {
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
				Self::set_reference_count(txn, subspace, &candidate.item, reference_count).await?;
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
		let id_bytes = id.to_bytes();

		let cache_entry_object_future = async {
			let prefix = (
				KeyKind::CacheEntryObject.to_i32().unwrap(),
				id_bytes.as_ref(),
			);
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

		let dependency_cache_entry_future = async {
			let prefix = (
				KeyKind::DependencyCacheEntry.to_i32().unwrap(),
				id_bytes.as_ref(),
			);
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

		let (cache_entry_object_count, dependency_cache_entry_count) =
			futures::future::try_join(cache_entry_object_future, dependency_cache_entry_future)
				.await?;
		let count = cache_entry_object_count + dependency_cache_entry_count;
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

	async fn set_reference_count(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		item: &Item,
		reference_count: u64,
	) -> tg::Result<()> {
		match item {
			Item::CacheEntry(id) => {
				let key = Key::CacheEntry(id.clone());
				let key = Self::pack(subspace, &key);
				if let Some(bytes) = txn
					.get(&key, false)
					.await
					.map_err(|source| tg::error!(!source, "failed to get cache entry"))?
				{
					let mut entry = CacheEntry::deserialize(&bytes)?;
					entry.reference_count = reference_count;
					let bytes = entry.serialize()?;
					txn.set(&key, &bytes);
				}
			},
			Item::Object(id) => {
				let key = Key::Object(id.clone());
				let key = Self::pack(subspace, &key);
				if let Some(bytes) = txn
					.get(&key, false)
					.await
					.map_err(|source| tg::error!(!source, "failed to get object"))?
				{
					let mut object = Object::deserialize(&bytes)?;
					object.reference_count = reference_count;
					let bytes = object.serialize()?;
					txn.set(&key, &bytes);
				}
			},
			Item::Process(id) => {
				let key = Key::Process(id.clone());
				let key = Self::pack(subspace, &key);
				if let Some(bytes) = txn
					.get(&key, false)
					.await
					.map_err(|source| tg::error!(!source, "failed to get process"))?
				{
					let mut process = Process::deserialize(&bytes)?;
					process.reference_count = reference_count;
					let bytes = process.serialize()?;
					txn.set(&key, &bytes);
				}
			},
		}
		Ok(())
	}

	async fn delete_item(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		item: &Item,
		partition_total: u64,
	) -> tg::Result<()> {
		match item {
			Item::CacheEntry(id) => {
				Self::delete_cache_entry(txn, subspace, id, partition_total).await
			},
			Item::Object(id) => Self::delete_object(txn, subspace, id, partition_total).await,
			Item::Process(id) => Self::delete_process(txn, subspace, id, partition_total).await,
		}
	}

	async fn delete_cache_entry(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::artifact::Id,
		partition_total: u64,
	) -> tg::Result<()> {
		let key = Key::CacheEntry(id.clone());
		let key = Self::pack(subspace, &key);
		txn.clear(&key);

		let id_bytes = id.to_bytes();
		let prefix = (
			KeyKind::CacheEntryDependency.to_i32().unwrap(),
			id_bytes.as_ref(),
		);
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
		let dependencies = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())
					.map_err(|source| tg::error!(!source, "failed to unpack key"))?;
				let Key::CacheEntryDependency { dependency, .. } = key else {
					return Err(tg::error!("expected cache entry dependency key"));
				};
				Ok(dependency)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		let (begin, end) = range_subspace.range();
		txn.clear_range(&begin, &end);

		for dependency in dependencies {
			let key = Key::DependencyCacheEntry {
				dependency: dependency.clone(),
				cache_entry: id.clone(),
			};
			let key = Self::pack(subspace, &key);
			txn.clear(&key);

			Self::decrement_cache_entry_reference_count(
				txn,
				subspace,
				&dependency,
				partition_total,
			)
			.await?;
		}

		Ok(())
	}

	async fn delete_object(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::object::Id,
		partition_total: u64,
	) -> tg::Result<()> {
		let key = Key::Object(id.clone());
		let key = Self::pack(subspace, &key);
		let cache_entry = txn
			.get(&key, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get object"))?
			.and_then(|bytes| Object::deserialize(&bytes).ok())
			.and_then(|obj| obj.cache_entry);

		txn.clear(&key);

		let id_bytes = id.to_bytes();

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
		let key = Key::Process(id.clone());
		let key = Self::pack(subspace, &key);
		txn.clear(&key);

		let id_bytes = id.to_bytes();

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
		let key = Key::CacheEntry(id.clone());
		let key = Self::pack(subspace, &key);
		let Some(bytes) = txn
			.get(&key, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get cache entry"))?
		else {
			return Ok(());
		};
		let mut entry = CacheEntry::deserialize(&bytes)?;
		let reference_count = entry.reference_count;
		if reference_count > 1 {
			entry.reference_count = reference_count - 1;
			let bytes = entry.serialize()?;
			txn.set(&key, &bytes);
		} else {
			entry.reference_count = 0;
			let bytes = entry.serialize()?;
			txn.set(&key, &bytes);

			let id_bytes = id.to_bytes();
			let partition = Self::partition_for_id(id_bytes.as_ref(), partition_total);
			let key = Key::Clean {
				partition,
				touched_at: entry.touched_at,
				kind: ItemKind::CacheEntry,
				id: tg::Either::Left(id.clone().into()),
			};
			let clean_key = Self::pack(subspace, &key);
			txn.set(&clean_key, &[]);
		}
		Ok(())
	}

	pub(super) async fn decrement_object_reference_count(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::object::Id,
		partition_total: u64,
	) -> tg::Result<()> {
		let key = Key::Object(id.clone());
		let key = Self::pack(subspace, &key);
		let Some(bytes) = txn
			.get(&key, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get object"))?
		else {
			return Ok(());
		};
		let mut object = Object::deserialize(&bytes)?;
		let reference_count = object.reference_count;
		if reference_count > 1 {
			object.reference_count = reference_count - 1;
			let bytes = object.serialize()?;
			txn.set(&key, &bytes);
		} else {
			object.reference_count = 0;
			let bytes = object.serialize()?;
			txn.set(&key, &bytes);

			let id_bytes = id.to_bytes();
			let partition = Self::partition_for_id(id_bytes.as_ref(), partition_total);
			let key = Key::Clean {
				partition,
				touched_at: object.touched_at,
				kind: ItemKind::Object,
				id: tg::Either::Left(id.clone()),
			};
			let clean_key = Self::pack(subspace, &key);
			txn.set(&clean_key, &[]);
		}
		Ok(())
	}

	pub(super) async fn decrement_process_reference_count(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::process::Id,
		partition_total: u64,
	) -> tg::Result<()> {
		let key = Key::Process(id.clone());
		let key = Self::pack(subspace, &key);
		let Some(bytes) = txn
			.get(&key, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get process"))?
		else {
			return Ok(());
		};
		let mut process = Process::deserialize(&bytes)?;
		let reference_count = process.reference_count;
		if reference_count > 1 {
			process.reference_count = reference_count - 1;
			let bytes = process.serialize()?;
			txn.set(&key, &bytes);
		} else {
			process.reference_count = 0;
			let bytes = process.serialize()?;
			txn.set(&key, &bytes);

			let id_bytes = id.to_bytes();
			let partition = Self::partition_for_id(id_bytes.as_ref(), partition_total);
			let key = Key::Clean {
				partition,
				touched_at: process.touched_at,
				kind: ItemKind::Process,
				id: tg::Either::Right(id.clone()),
			};
			let clean_key = Self::pack(subspace, &key);
			txn.set(&clean_key, &[]);
		}
		Ok(())
	}
}
