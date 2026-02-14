use {
	super::{Index, ItemKind, Key, KeyKind, Request, Response, Update},
	crate::{
		CacheEntry, Object, ObjectStored, Process, PutArg, PutCacheEntryArg, PutObjectArg,
		PutProcessArg,
	},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	num_traits::ToPrimitive as _,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn put(&self, arg: PutArg) -> tg::Result<()> {
		if arg.cache_entries.is_empty() && arg.objects.is_empty() && arg.processes.is_empty() {
			return Ok(());
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::Put(arg);
		self.sender_medium
			.send((request, sender))
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		let response = receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))??;
		let Response::Unit = response else {
			return Err(tg::error!("unexpected response"));
		};
		Ok(())
	}

	pub(super) async fn task_put(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &PutArg,
		partition_total: u64,
	) -> tg::Result<()> {
		for cache_entry in &arg.cache_entries {
			Self::put_cache_entry(txn, subspace, cache_entry, partition_total)
				.map_err(|source| tg::error!(!source, "failed to put the cache entry"))?;
		}
		for object in &arg.objects {
			Self::put_object(txn, subspace, object, partition_total)
				.await
				.map_err(|source| tg::error!(!source, "failed to put the object"))?;
		}
		for process in &arg.processes {
			Self::put_process(txn, subspace, process, partition_total)
				.await
				.map_err(|source| tg::error!(!source, "failed to put the process"))?;
		}
		Ok(())
	}

	fn put_cache_entry(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &PutCacheEntryArg,
		partition_total: u64,
	) -> Result<(), fdb::FdbBindingError> {
		let id = &arg.id;

		let key = Key::CacheEntry(id.clone());
		let key = Self::pack(subspace, &key);
		let value = CacheEntry {
			reference_count: 0,
			touched_at: arg.touched_at,
		}
		.serialize()
		.map_err(|error| fdb::FdbBindingError::CustomError(error.into()))?;
		txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
			.unwrap();
		txn.set(&key, &value);

		for dependency in &arg.dependencies {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = Key::CacheEntryDependency {
				cache_entry: id.clone(),
				dependency: dependency.clone(),
			};
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);

			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = Key::DependencyCacheEntry {
				dependency: dependency.clone(),
				cache_entry: id.clone(),
			};
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);
		}

		let id_bytes = id.to_bytes();
		let partition = Self::partition_for_id(id_bytes.as_ref(), partition_total);
		let key = Key::Clean {
			partition,
			touched_at: arg.touched_at,
			kind: ItemKind::CacheEntry,
			id: tg::Either::Left(arg.id.clone().into()),
		};
		let key = Self::pack(subspace, &key);
		txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
			.unwrap();
		txn.set(&key, &[]);

		Ok(())
	}

	async fn put_object(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &PutObjectArg,
		partition_total: u64,
	) -> Result<(), fdb::FdbBindingError> {
		let id = &arg.id;
		let key = Key::Object(id.clone());
		let key = Self::pack(subspace, &key);

		let existing = if arg.complete() {
			None
		} else {
			txn.get(&key, false)
				.await?
				.and_then(|bytes| Object::deserialize(&bytes).ok())
		};

		let touched_at = existing.as_ref().map_or(arg.touched_at, |existing| {
			existing.touched_at.max(arg.touched_at)
		});

		let cache_entry = arg.cache_entry.clone().or_else(|| {
			existing
				.as_ref()
				.and_then(|existing| existing.cache_entry.clone())
		});

		let stored = ObjectStored {
			subtree: arg.stored.subtree
				|| existing
					.as_ref()
					.is_some_and(|existing| existing.stored.subtree),
		};

		let mut metadata = arg.metadata.clone();
		if let Some(ref existing) = existing {
			metadata.merge(&existing.metadata);
		}

		let value = Object {
			cache_entry,
			metadata,
			reference_count: 0,
			stored,
			touched_at,
		}
		.serialize()
		.map_err(|error| fdb::FdbBindingError::CustomError(error.into()))?;

		if existing.is_none() {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
		}
		txn.set(&key, &value);

		for child in &arg.children {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = Key::ObjectChild {
				object: id.clone(),
				child: child.clone(),
			};
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);

			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = Key::ChildObject {
				child: child.clone(),
				object: id.clone(),
			};
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);
		}

		if let Some(cache_entry) = &arg.cache_entry {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = Key::ObjectCacheEntry {
				object: id.clone(),
				cache_entry: cache_entry.clone(),
			};
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);

			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = Key::CacheEntryObject {
				cache_entry: cache_entry.clone(),
				object: id.clone(),
			};
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);
		}

		let id_bytes = id.to_bytes();
		let partition = Self::partition_for_id(id_bytes.as_ref(), partition_total);
		txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
			.unwrap();
		let key = Key::Clean {
			partition,
			touched_at,
			kind: ItemKind::Object,
			id: tg::Either::Left(id.clone()),
		};
		let key = Self::pack(subspace, &key);
		txn.set(&key, &[]);

		Self::enqueue_update(
			txn,
			subspace,
			&tg::Either::Left(id.clone()),
			partition_total,
		);

		Ok(())
	}

	async fn put_process(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &PutProcessArg,
		partition_total: u64,
	) -> Result<(), fdb::FdbBindingError> {
		let id = &arg.id;
		let key = Key::Process(id.clone());
		let key = Self::pack(subspace, &key);

		let existing = if arg.complete() {
			None
		} else {
			txn.get(&key, false)
				.await?
				.and_then(|bytes| Process::deserialize(&bytes).ok())
		};

		let touched_at = existing.as_ref().map_or(arg.touched_at, |existing| {
			existing.touched_at.max(arg.touched_at)
		});

		let mut stored = arg.stored.clone();
		if let Some(ref existing) = existing {
			stored.merge(&existing.stored);
		}

		let mut metadata = arg.metadata.clone();
		if let Some(ref existing) = existing {
			metadata.merge(&existing.metadata);
		}

		let value = Process {
			metadata,
			reference_count: 0,
			stored,
			touched_at,
		}
		.serialize()
		.map_err(|error| fdb::FdbBindingError::CustomError(error.into()))?;
		txn.set(&key, &value);

		for child in &arg.children {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = Key::ProcessChild {
				process: id.clone(),
				child: child.clone(),
			};
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);

			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = Key::ChildProcess {
				child: child.clone(),
				parent: id.clone(),
			};
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);
		}

		for (object, kind) in &arg.objects {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = Key::ProcessObject {
				process: id.clone(),
				kind: *kind,
				object: object.clone(),
			};
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);

			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = Key::ObjectProcess {
				object: object.clone(),
				kind: *kind,
				process: id.clone(),
			};
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);
		}

		let id_bytes = id.to_bytes();
		let partition = Self::partition_for_id(id_bytes.as_ref(), partition_total);
		txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
			.unwrap();
		let key = Key::Clean {
			partition,
			touched_at,
			kind: ItemKind::Process,
			id: tg::Either::Right(id.clone()),
		};
		let key = Self::pack(subspace, &key);
		txn.set(&key, &[]);

		Self::enqueue_update(
			txn,
			subspace,
			&tg::Either::Right(id.clone()),
			partition_total,
		);

		Ok(())
	}

	pub(super) fn enqueue_update(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		id: &tg::Either<tg::object::Id, tg::process::Id>,
		partition_total: u64,
	) {
		let key = Self::pack(subspace, &Key::Update { id: id.clone() });
		let value = [Update::Put.to_u8().unwrap()];
		txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
			.unwrap();
		txn.set(&key, &value);

		let id_bytes = match &id {
			tg::Either::Left(id) => id.to_bytes(),
			tg::Either::Right(id) => id.to_bytes(),
		};
		let partition = Self::partition_for_id(id_bytes.as_ref(), partition_total);
		let key = Self::pack_with_versionstamp(
			subspace,
			&(
				KeyKind::UpdateVersion.to_i32().unwrap(),
				partition,
				fdbt::Versionstamp::incomplete(0),
				id_bytes.as_ref(),
			),
		);
		txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
			.unwrap();
		txn.atomic_op(&key, &[], fdb::options::MutationType::SetVersionstampedKey);
	}
}
