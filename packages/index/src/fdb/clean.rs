mod key;

pub(super) use key::{ItemKind, Key};

use {
	super::{Index, Kind, Request, Response},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	futures::StreamExt as _,
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

pub(super) struct TaskCleanArg<'a> {
	pub txn: &'a fdb::Transaction,
	pub subspace: &'a Subspace,
	pub now: i64,
	pub max_object_touched_at: i64,
	pub max_process_touched_at: i64,
	pub batch_size: usize,
	pub partition_start: u64,
	pub partition_count: u64,
	pub partition_total: u64,
}

impl Index {
	pub async fn clean(
		&self,
		now: i64,
		max_object_touched_at: i64,
		max_process_touched_at: i64,
		batch_size: usize,
		partition_start: u64,
		partition_count: u64,
	) -> tg::Result<crate::clean::Output> {
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::Clean(crate::fdb::Clean {
			batch_size,
			max_object_touched_at,
			max_process_touched_at,
			now,
			partition_count,
			partition_start,
		});
		self.sender_low
			.send((request, sender))
			.map_err(|error| tg::error!(!error, "failed to send the request"))?;
		let response = receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))??;
		let Response::CleanOutput(output) = response else {
			return Err(tg::error!("unexpected response"));
		};
		Ok(output)
	}

	pub(super) async fn task_clean(arg: TaskCleanArg<'_>) -> tg::Result<crate::clean::Output> {
		let TaskCleanArg {
			txn,
			subspace,
			now,
			max_object_touched_at,
			max_process_touched_at,
			batch_size,
			partition_start,
			partition_count,
			partition_total,
		} = arg;
		let grants = Self::delete_expired_grants(
			txn,
			subspace,
			now,
			batch_size,
			partition_start,
			partition_count,
			partition_total,
		)
		.await?;
		let mut output = crate::clean::Output {
			grants,
			..Default::default()
		};
		let remaining_batch_size = batch_size.saturating_sub(grants);
		let mut candidates = Vec::new();

		let key_kind = Kind::Clean.to_i32().unwrap();
		let max_touched_at = max_object_touched_at.max(max_process_touched_at);
		let partition_end = partition_start.saturating_add(partition_count);
		for partition in partition_start..partition_end {
			let begin = Self::pack(subspace, &(key_kind, partition, 0i64));
			let end = Self::pack(subspace, &(key_kind, partition, max_touched_at + 1));
			if candidates.len() >= remaining_batch_size {
				break;
			}
			let range = fdb::RangeOption {
				begin: fdb::KeySelector::first_greater_or_equal(&begin),
				end: fdb::KeySelector::first_greater_or_equal(&end),
				mode: fdb::options::StreamingMode::Iterator,
				..Default::default()
			};
			let mut entries = txn.get_ranges_keyvalues(range, false);
			while candidates.len() < remaining_batch_size {
				let Some(entry) = entries
					.next()
					.await
					.transpose()
					.map_err(|error| tg::error!(!error, "failed to get the next entry"))?
				else {
					break;
				};
				let key = Self::unpack(subspace, entry.key())
					.map_err(|error| tg::error!(!error, "failed to unpack key"))?;
				let crate::fdb::Key::Clean(crate::fdb::clean::Key::Clean {
					partition,
					touched_at,
					kind,
					id,
				}) = key
				else {
					return Err(tg::error!("expected clean key"));
				};
				let max_touched_at = match kind {
					ItemKind::CacheEntry | ItemKind::Object => max_object_touched_at,
					ItemKind::Process => max_process_touched_at,
				};
				if touched_at > max_touched_at {
					continue;
				}
				let item = match kind {
					ItemKind::CacheEntry => {
						let tg::Either::Left(object_id) = id else {
							return Err(tg::error!("expected object id for cache entry"));
						};
						let artifact_id = tg::artifact::Id::try_from(object_id)
							.map_err(|error| tg::error!(!error, "invalid artifact id"))?;
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
			let touched_at = Self::get_touched_at(txn, subspace, &candidate.item).await?;
			if touched_at != candidate.touched_at {
				Self::delete_clean_key(txn, subspace, candidate);
				continue;
			}

			let reference_count = match &candidate.item {
				Item::CacheEntry(id) => {
					Self::compute_cache_entry_reference_count(txn, subspace, id).await?
				},
				Item::Object(id) => Self::compute_object_reference_count(txn, subspace, id).await?,
				Item::Process(id) => {
					Self::compute_process_reference_count(txn, subspace, id).await?
				},
			};

			let item = if reference_count > 0 {
				Self::set_reference_count(txn, subspace, &candidate.item, reference_count).await?;
				None
			} else {
				Self::delete_item(txn, subspace, &candidate.item, partition_total).await?;
				Some(candidate.item.clone())
			};

			Self::delete_clean_key(txn, subspace, candidate);

			if let Some(item) = item {
				match item {
					Item::CacheEntry(id) => output.cache_entries.push(id),
					Item::Object(id) => output.objects.push(id),
					Item::Process(id) => output.processes.push(id),
				}
			}
		}

		output.done = grants == 0 && candidates.is_empty();

		Ok(output)
	}

	async fn delete_expired_grants(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		now: i64,
		batch_size: usize,
		partition_start: u64,
		partition_count: u64,
		partition_total: u64,
	) -> tg::Result<usize> {
		let key_kind = Kind::GrantExpiresAt.to_i32().unwrap();
		let partition_end = partition_start.saturating_add(partition_count);
		let mut args = Vec::new();
		for partition in partition_start..partition_end {
			if args.len() >= batch_size {
				break;
			}
			let begin = Self::pack(subspace, &(key_kind, partition, i64::MIN));
			let end = Self::pack(subspace, &(key_kind, partition, now + 1));
			let range = fdb::RangeOption {
				begin: fdb::KeySelector::first_greater_or_equal(&begin),
				end: fdb::KeySelector::first_greater_or_equal(&end),
				mode: fdb::options::StreamingMode::Iterator,
				..Default::default()
			};
			let mut entries = txn.get_ranges_keyvalues(range, false);
			while args.len() < batch_size {
				let Some(entry) = entries
					.next()
					.await
					.transpose()
					.map_err(|error| tg::error!(!error, "failed to get the next entry"))?
				else {
					break;
				};
				let key = Self::unpack(subspace, entry.key())
					.map_err(|error| tg::error!(!error, "failed to unpack key"))?;
				let crate::fdb::Key::Grant(crate::fdb::grant::Key::GrantExpiresAt {
					expires_at,
					resource,
					principal,
					creator,
					permission,
					..
				}) = key
				else {
					return Err(tg::error!("expected a grant expiration key"));
				};
				args.push(crate::grant::delete::Arg {
					creator,
					expires_at: Some(expires_at),
					permissions: permission.into(),
					principal,
					resource,
				});
			}
		}
		let count = args.len();
		for arg in args {
			for permission in arg.permissions.iter() {
				Self::delete_grant_index_entry(
					txn,
					subspace,
					&crate::fdb::grant::GrantIndexEntry {
						creator: arg.creator.as_ref(),
						expires_at: arg.expires_at,
						permission,
						principal: &arg.principal,
						resource: &arg.resource,
					},
					crate::fdb::grant::GrantSource::All,
					partition_total,
				)
				.await?;
				Self::enqueue_grant_update(
					txn,
					subspace,
					&arg.resource,
					&arg.principal,
					permission,
					partition_total,
				);
			}
		}
		Ok(count)
	}

	async fn get_touched_at(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		item: &Item,
	) -> tg::Result<i64> {
		match item {
			Item::CacheEntry(id) => {
				let entry = Self::try_get_cache_entry_with_transaction(txn, subspace, id)
					.await?
					.ok_or_else(
						|| tg::error!(%id, "the clean key referenced a missing cache entry"),
					)?;
				Ok(entry.touched_at)
			},
			Item::Object(id) => {
				let object = Self::try_get_object_with_transaction(txn, subspace, id)
					.await?
					.ok_or_else(|| tg::error!(%id, "the clean key referenced a missing object"))?;
				Ok(object.touched_at)
			},
			Item::Process(id) => {
				let process = Self::try_get_process_with_transaction(txn, subspace, id)
					.await?
					.ok_or_else(|| tg::error!(%id, "the clean key referenced a missing process"))?;
				Ok(process.touched_at)
			},
		}
	}

	fn delete_clean_key(txn: &fdb::Transaction, subspace: &Subspace, candidate: &Candidate) {
		let key = match &candidate.item {
			Item::CacheEntry(id) => crate::fdb::Key::Clean(crate::fdb::clean::Key::Clean {
				partition: candidate.partition,
				touched_at: candidate.touched_at,
				kind: ItemKind::CacheEntry,
				id: tg::Either::Left(id.clone().into()),
			}),
			Item::Object(id) => crate::fdb::Key::Clean(crate::fdb::clean::Key::Clean {
				partition: candidate.partition,
				touched_at: candidate.touched_at,
				kind: ItemKind::Object,
				id: tg::Either::Left(id.clone()),
			}),
			Item::Process(id) => crate::fdb::Key::Clean(crate::fdb::clean::Key::Clean {
				partition: candidate.partition,
				touched_at: candidate.touched_at,
				kind: ItemKind::Process,
				id: tg::Either::Right(id.clone()),
			}),
		};
		let key = Self::pack(subspace, &key);
		txn.clear(&key);
	}

	async fn compute_cache_entry_reference_count(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::artifact::Id,
	) -> tg::Result<u64> {
		let id_bytes = id.to_bytes();

		let cache_entry_object_future = async {
			let prefix = (Kind::CacheEntryObject.to_i32().unwrap(), id_bytes.as_ref());
			let prefix = Self::pack(subspace, &prefix);
			let range_subspace = Subspace::from_bytes(prefix);
			let range = fdb::RangeOption {
				mode: fdb::options::StreamingMode::WantAll,
				..fdb::RangeOption::from(&range_subspace)
			};
			let count = txn
				.get_range(&range, 1, false)
				.await
				.map_err(|error| tg::error!(!error, "failed to get range"))?
				.len()
				.to_u64()
				.unwrap();
			Ok::<_, tg::Error>(count)
		};

		let dependency_cache_entry_future = async {
			let prefix = (
				Kind::DependencyCacheEntry.to_i32().unwrap(),
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
				.map_err(|error| tg::error!(!error, "failed to get range"))?
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
			let prefix = (Kind::ChildObject.to_i32().unwrap(), id.as_ref());
			let prefix = Self::pack(subspace, &prefix);
			let range_subspace = Subspace::from_bytes(prefix);
			let range = fdb::RangeOption {
				mode: fdb::options::StreamingMode::WantAll,
				..fdb::RangeOption::from(&range_subspace)
			};
			let count = txn
				.get_range(&range, 1, false)
				.await
				.map_err(|error| tg::error!(!error, "failed to get range"))?
				.len()
				.to_u64()
				.unwrap();
			Ok::<_, tg::Error>(count)
		};
		let object_process_future = async {
			let id = id.to_bytes();
			let prefix = (Kind::ObjectProcess.to_i32().unwrap(), id.as_ref());
			let prefix = Self::pack(subspace, &prefix);
			let range_subspace = Subspace::from_bytes(prefix);
			let range = fdb::RangeOption {
				mode: fdb::options::StreamingMode::WantAll,
				..fdb::RangeOption::from(&range_subspace)
			};
			let count = txn
				.get_range(&range, 1, false)
				.await
				.map_err(|error| tg::error!(!error, "failed to get range"))?
				.len()
				.to_u64()
				.unwrap();
			Ok::<_, tg::Error>(count)
		};
		let item_tag_future = async {
			let id = id.to_bytes();
			let prefix = (Kind::ItemTag.to_i32().unwrap(), id.as_ref());
			let prefix = Self::pack(subspace, &prefix);
			let range_subspace = Subspace::from_bytes(prefix);
			let range = fdb::RangeOption {
				mode: fdb::options::StreamingMode::WantAll,
				..fdb::RangeOption::from(&range_subspace)
			};
			let count = txn
				.get_range(&range, 1, false)
				.await
				.map_err(|error| tg::error!(!error, "failed to get range"))?
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
			let prefix = (Kind::ChildProcess.to_i32().unwrap(), id.as_ref());
			let prefix = Self::pack(subspace, &prefix);
			let range_subspace = Subspace::from_bytes(prefix);
			let range = fdb::RangeOption {
				mode: fdb::options::StreamingMode::WantAll,
				..fdb::RangeOption::from(&range_subspace)
			};
			let count = txn
				.get_range(&range, 1, false)
				.await
				.map_err(|error| tg::error!(!error, "failed to get range"))?
				.len()
				.to_u64()
				.unwrap();
			Ok::<_, tg::Error>(count)
		};
		let item_tag_future = async {
			let id = id.to_bytes();
			let prefix = (Kind::ItemTag.to_i32().unwrap(), id.as_ref());
			let prefix = Self::pack(subspace, &prefix);
			let range_subspace = Subspace::from_bytes(prefix);
			let range = fdb::RangeOption {
				mode: fdb::options::StreamingMode::WantAll,
				..fdb::RangeOption::from(&range_subspace)
			};
			let count = txn
				.get_range(&range, 1, false)
				.await
				.map_err(|error| tg::error!(!error, "failed to get range"))?
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
				let key = crate::fdb::Key::Cache(crate::fdb::cache::Key::CacheEntry(id.clone()));
				let key = Self::pack(subspace, &key);
				if let Some(bytes) = txn
					.get(&key, false)
					.await
					.map_err(|error| tg::error!(!error, "failed to get cache entry"))?
				{
					let mut entry = crate::cache::Entry::deserialize(&bytes)?;
					entry.reference_count = reference_count;
					let bytes = entry.serialize()?;
					txn.set(&key, &bytes);
				}
			},
			Item::Object(id) => {
				let key = crate::fdb::Key::Object(crate::fdb::object::Key::Object(id.clone()));
				let key = Self::pack(subspace, &key);
				if let Some(bytes) = txn
					.get(&key, false)
					.await
					.map_err(|error| tg::error!(!error, "failed to get object"))?
				{
					let mut object = crate::object::Object::deserialize(&bytes)?;
					object.reference_count = reference_count;
					let bytes = object.serialize()?;
					txn.set(&key, &bytes);
				}
			},
			Item::Process(id) => {
				let key = crate::fdb::Key::Process(crate::fdb::process::Key::Process(id.clone()));
				let key = Self::pack(subspace, &key);
				if let Some(bytes) = txn
					.get(&key, false)
					.await
					.map_err(|error| tg::error!(!error, "failed to get process"))?
				{
					let mut process = crate::process::Process::deserialize(&bytes)?;
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
		let key = crate::fdb::Key::Cache(crate::fdb::cache::Key::CacheEntry(id.clone()));
		let key = Self::pack(subspace, &key);
		txn.clear(&key);

		let id_bytes = id.to_bytes();
		let prefix = (
			Kind::CacheEntryDependency.to_i32().unwrap(),
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
			.map_err(|error| tg::error!(!error, "failed to get range"))?;
		let dependencies = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())
					.map_err(|error| tg::error!(!error, "failed to unpack key"))?;
				let crate::fdb::Key::Cache(crate::fdb::cache::Key::CacheEntryDependency {
					dependency,
					..
				}) = key
				else {
					return Err(tg::error!("expected cache entry dependency key"));
				};
				Ok(dependency)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		let (begin, end) = range_subspace.range();
		txn.clear_range(&begin, &end);

		for dependency in dependencies {
			let key = crate::fdb::Key::Cache(crate::fdb::cache::Key::DependencyCacheEntry {
				dependency: dependency.clone(),
				cache_entry: id.clone(),
			});
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
		let key = crate::fdb::Key::Object(crate::fdb::object::Key::Object(id.clone()));
		let key = Self::pack(subspace, &key);
		let cache_entry = txn
			.get(&key, false)
			.await
			.map_err(|error| tg::error!(!error, "failed to get object"))?
			.and_then(|bytes| crate::object::Object::deserialize(&bytes).ok())
			.and_then(|obj| obj.cache_entry);

		txn.clear(&key);

		let id_bytes = id.to_bytes();

		let prefix = (Kind::ObjectChild.to_i32().unwrap(), id_bytes.as_ref());
		let prefix = Self::pack(subspace, &prefix);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace)
		};
		let entries = txn
			.get_range(&range, 1, false)
			.await
			.map_err(|error| tg::error!(!error, "failed to get range"))?;
		let children = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())
					.map_err(|error| tg::error!(!error, "failed to unpack key"))?;
				let crate::fdb::Key::Object(crate::fdb::object::Key::ObjectChild { child, .. }) =
					key
				else {
					return Err(tg::error!("expected object child key"));
				};
				Ok(child)
			})
			.collect::<tg::Result<Vec<_>>>()?;
		let (begin, end) = range_subspace.range();
		txn.clear_range(&begin, &end);
		for child in &children {
			let key = crate::fdb::Key::Object(crate::fdb::object::Key::ChildObject {
				child: child.clone(),
				object: id.clone(),
			});
			let key = Self::pack(subspace, &key);
			txn.clear(&key);
		}
		for child in children {
			Self::decrement_object_reference_count(txn, subspace, &child, partition_total).await?;
		}

		if let Some(cache_entry) = &cache_entry {
			let key = crate::fdb::Key::Object(crate::fdb::object::Key::ObjectCacheEntry {
				object: id.clone(),
				cache_entry: cache_entry.clone(),
			});
			let key = Self::pack(subspace, &key);
			txn.clear(&key);

			let key = crate::fdb::Key::Object(crate::fdb::object::Key::CacheEntryObject {
				cache_entry: cache_entry.clone(),
				object: id.clone(),
			});
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
		let key = crate::fdb::Key::Process(crate::fdb::process::Key::Process(id.clone()));
		let key = Self::pack(subspace, &key);
		txn.clear(&key);

		let id_bytes = id.to_bytes();

		let prefix = (Kind::ProcessChild.to_i32().unwrap(), id_bytes.as_ref());
		let prefix = Self::pack(subspace, &prefix);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace)
		};
		let entries = txn
			.get_range(&range, 1, false)
			.await
			.map_err(|error| tg::error!(!error, "failed to get range"))?;
		let children = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())
					.map_err(|error| tg::error!(!error, "failed to unpack key"))?;
				let crate::fdb::Key::Process(crate::fdb::process::Key::ProcessChild {
					child, ..
				}) = key
				else {
					return Err(tg::error!("expected process child key"));
				};
				Ok(child)
			})
			.collect::<tg::Result<Vec<_>>>()?;
		let (begin, end) = range_subspace.range();
		txn.clear_range(&begin, &end);
		for child in &children {
			let key = crate::fdb::Key::Process(crate::fdb::process::Key::ChildProcess {
				child: child.clone(),
				parent: id.clone(),
			});
			let key = Self::pack(subspace, &key);
			txn.clear(&key);
		}
		for child in children {
			Self::decrement_process_reference_count(txn, subspace, &child, partition_total).await?;
		}

		let prefix = (Kind::ProcessObject.to_i32().unwrap(), id_bytes.as_ref());
		let prefix = Self::pack(subspace, &prefix);
		let range_subspace = Subspace::from_bytes(prefix);
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace)
		};
		let entries = txn
			.get_range(&range, 1, false)
			.await
			.map_err(|error| tg::error!(!error, "failed to get range"))?;
		let object_processes = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())
					.map_err(|error| tg::error!(!error, "failed to unpack key"))?;
				let crate::fdb::Key::Process(crate::fdb::process::Key::ProcessObject {
					kind,
					object,
					..
				}) = key
				else {
					return Err(tg::error!("expected process object key"));
				};
				Ok((object, kind))
			})
			.collect::<tg::Result<Vec<_>>>()?;
		let (begin, end) = range_subspace.range();
		txn.clear_range(&begin, &end);
		for (object, kind) in &object_processes {
			let key = crate::fdb::Key::Object(crate::fdb::object::Key::ObjectProcess {
				object: object.clone(),
				kind: *kind,
				process: id.clone(),
			});
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
		let key = crate::fdb::Key::Cache(crate::fdb::cache::Key::CacheEntry(id.clone()));
		let key = Self::pack(subspace, &key);
		let Some(bytes) = txn
			.get(&key, false)
			.await
			.map_err(|error| tg::error!(!error, "failed to get cache entry"))?
		else {
			return Ok(());
		};
		let mut entry = crate::cache::Entry::deserialize(&bytes)?;
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
			let key = crate::fdb::Key::Clean(crate::fdb::clean::Key::Clean {
				partition,
				touched_at: entry.touched_at,
				kind: ItemKind::CacheEntry,
				id: tg::Either::Left(id.clone().into()),
			});
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
		let key = crate::fdb::Key::Object(crate::fdb::object::Key::Object(id.clone()));
		let key = Self::pack(subspace, &key);
		let Some(bytes) = txn
			.get(&key, false)
			.await
			.map_err(|error| tg::error!(!error, "failed to get object"))?
		else {
			return Ok(());
		};
		let mut object = crate::object::Object::deserialize(&bytes)?;
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
			let key = crate::fdb::Key::Clean(crate::fdb::clean::Key::Clean {
				partition,
				touched_at: object.touched_at,
				kind: ItemKind::Object,
				id: tg::Either::Left(id.clone()),
			});
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
		let key = crate::fdb::Key::Process(crate::fdb::process::Key::Process(id.clone()));
		let key = Self::pack(subspace, &key);
		let Some(bytes) = txn
			.get(&key, false)
			.await
			.map_err(|error| tg::error!(!error, "failed to get process"))?
		else {
			return Ok(());
		};
		let mut process = crate::process::Process::deserialize(&bytes)?;
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
			let key = crate::fdb::Key::Clean(crate::fdb::clean::Key::Clean {
				partition,
				touched_at: process.touched_at,
				kind: ItemKind::Process,
				id: tg::Either::Right(id.clone()),
			});
			let clean_key = Self::pack(subspace, &key);
			txn.set(&clean_key, &[]);
		}
		Ok(())
	}
}
