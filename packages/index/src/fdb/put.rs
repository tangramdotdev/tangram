use {
	super::{Index, ItemKind, Key, KeyKind, Metrics, Update},
	crate::{
		CacheEntry, Object, ObjectStored, Process, PutArg, PutCacheEntryArg, PutObjectArg,
		PutProcessArg,
	},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	futures::{StreamExt as _, stream},
	num_traits::ToPrimitive as _,
	std::{
		collections::HashSet,
		sync::{
			Arc, Mutex,
			atomic::{AtomicU64, Ordering},
		},
	},
	tangram_client::prelude::*,
};

pub(super) struct Request {
	arg: PutArg,
	sender: tokio::sync::oneshot::Sender<tg::Result<()>>,
}

struct RequestState {
	remaining: usize,
	result: tg::Result<()>,
	sender: Option<tokio::sync::oneshot::Sender<tg::Result<()>>>,
}

struct Batch {
	arg: PutArg,
	trackers: Vec<Arc<Mutex<RequestState>>>,
}

enum Item {
	CacheEntry(PutCacheEntryArg),
	Object(PutObjectArg),
	Process(PutProcessArg),
}

impl Index {
	pub async fn put(&self, arg: PutArg) -> tg::Result<()> {
		if arg.cache_entries.is_empty() && arg.objects.is_empty() && arg.processes.is_empty() {
			return Ok(());
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		self.put_sender
			.send(Request { arg, sender })
			.map_err(|_| tg::error!("failed to send the request to the put task"))?;
		receiver
			.await
			.map_err(|_| tg::error!("the put task failed"))??;
		Ok(())
	}

	pub(super) async fn put_task(
		database: Arc<fdb::Database>,
		subspace: fdbt::Subspace,
		mut receiver: tokio::sync::mpsc::UnboundedReceiver<Request>,
		concurrency: usize,
		max_items_per_transaction: usize,
		partition_total: u64,
		metrics: Metrics,
	) {
		futures::stream::unfold(&mut receiver, |receiver| async move {
			let request = receiver.recv().await?;
			let mut requests = vec![request];
			while let Ok(request) = receiver.try_recv() {
				requests.push(request);
			}
			let batches = Self::put_task_create_batches(requests, max_items_per_transaction);
			Some((batches, receiver))
		})
		.flat_map(stream::iter)
		.for_each_concurrent(concurrency, |batch| {
			let database = database.clone();
			let subspace = subspace.clone();
			let metrics = metrics.clone();
			async move {
				let result = Self::put_task_put_batch(
					&database,
					&subspace,
					&batch.arg,
					partition_total,
					&metrics,
				)
				.await;
				for tracker in batch.trackers {
					let mut state = tracker.lock().unwrap();
					if let Err(error) = &result {
						state.result = Err(error.clone());
					}
					state.remaining -= 1;
					if state.remaining == 0
						&& let Some(sender) = state.sender.take()
					{
						sender.send(state.result.clone()).ok();
					}
				}
			}
		})
		.await;
	}

	fn put_task_create_batches(requests: Vec<Request>, max_item_count: usize) -> Vec<Batch> {
		let mut items = Vec::new();

		for request in requests {
			let tracker = Arc::new(Mutex::new(RequestState {
				remaining: 0,
				result: Ok(()),
				sender: Some(request.sender),
			}));

			for item in request.arg.cache_entries {
				items.push((Item::CacheEntry(item), tracker.clone()));
			}
			for item in request.arg.objects {
				items.push((Item::Object(item), tracker.clone()));
			}
			for item in request.arg.processes {
				items.push((Item::Process(item), tracker.clone()));
			}
		}

		let mut batches = Vec::new();
		let mut current_arg = PutArg::default();
		let mut current_trackers: Vec<Arc<Mutex<RequestState>>> = Vec::new();
		let mut current_tracker_set: HashSet<*const Mutex<RequestState>> = HashSet::new();
		let mut current_item_count = 0;

		for (item, tracker) in items {
			if current_item_count + 1 > max_item_count && current_item_count > 0 {
				for tracker in &current_trackers {
					tracker.lock().unwrap().remaining += 1;
				}
				batches.push(Batch {
					arg: std::mem::take(&mut current_arg),
					trackers: std::mem::take(&mut current_trackers),
				});
				current_tracker_set.clear();
				current_item_count = 0;
			}

			match item {
				Item::CacheEntry(c) => current_arg.cache_entries.push(c),
				Item::Object(o) => current_arg.objects.push(o),
				Item::Process(p) => current_arg.processes.push(p),
			}
			current_item_count += 1;

			let ptr = Arc::as_ptr(&tracker);
			if current_tracker_set.insert(ptr) {
				current_trackers.push(tracker);
			}
		}

		if current_item_count > 0 {
			for tracker in &current_trackers {
				tracker.lock().unwrap().remaining += 1;
			}
			batches.push(Batch {
				arg: current_arg,
				trackers: current_trackers,
			});
		}

		batches
	}

	async fn put_task_put_batch(
		database: &fdb::Database,
		subspace: &fdbt::Subspace,
		arg: &PutArg,
		partition_total: u64,
		metrics: &Metrics,
	) -> tg::Result<()> {
		let start = std::time::Instant::now();
		let retry_count = AtomicU64::new(0);

		let result = database
			.run(|txn, _maybe_committed| {
				retry_count.fetch_add(1, Ordering::Relaxed);
				let subspace = subspace.clone();
				let arg = arg.clone();
				async move {
					txn.set_option(fdb::options::TransactionOption::PriorityBatch)
						.unwrap();
					for cache_entry in &arg.cache_entries {
						Self::put_cache_entry(&txn, &subspace, cache_entry, partition_total)
							.await?;
					}
					for object in &arg.objects {
						Self::put_object(&txn, &subspace, object, partition_total).await?;
					}
					for process in &arg.processes {
						Self::put_process(&txn, &subspace, process, partition_total).await?;
					}
					Ok::<_, fdb::FdbBindingError>(())
				}
			})
			.await;

		let duration = start.elapsed().as_secs_f64();
		metrics.put_commit_duration.record(duration, &[]);
		metrics.put_transactions.add(1, &[]);

		let attempts = retry_count.load(Ordering::Relaxed);
		if attempts > 1 {
			metrics
				.put_transaction_conflict_retry
				.add(attempts - 1, &[]);
		}

		match result {
			Ok(()) => (),
			Err(fdb::FdbBindingError::NonRetryableFdbError(error)) if error.code() == 2101 => {
				metrics.put_transaction_too_large.add(1, &[]);
				let count = arg.cache_entries.len() + arg.objects.len() + arg.processes.len();
				if count <= 1 {
					return Err(tg::error!(
						source = fdb::FdbBindingError::NonRetryableFdbError(error),
						"single item exceeds transaction size limit"
					));
				}
				let left = PutArg {
					cache_entries: arg.cache_entries[..arg.cache_entries.len() / 2].to_vec(),
					objects: arg.objects[..arg.objects.len() / 2].to_vec(),
					processes: arg.processes[..arg.processes.len() / 2].to_vec(),
				};
				let right = PutArg {
					cache_entries: arg.cache_entries[arg.cache_entries.len() / 2..].to_vec(),
					objects: arg.objects[arg.objects.len() / 2..].to_vec(),
					processes: arg.processes[arg.processes.len() / 2..].to_vec(),
				};
				Box::pin(Self::put_task_put_batch(
					database,
					subspace,
					&left,
					partition_total,
					metrics,
				))
				.await?;
				Box::pin(Self::put_task_put_batch(
					database,
					subspace,
					&right,
					partition_total,
					metrics,
				))
				.await?;
			},
			Err(error) => {
				return Err(tg::error!(!error, "failed to put"));
			},
		}

		Ok(())
	}

	async fn put_cache_entry(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &PutCacheEntryArg,
		partition_total: u64,
	) -> Result<(), fdb::FdbBindingError> {
		let id = &arg.id;
		let key = Key::CacheEntry(id.clone());
		let key = Self::pack(subspace, &key);

		let existing = txn
			.get(&key, false)
			.await?
			.and_then(|bytes| CacheEntry::deserialize(&bytes).ok());

		let touched_at = existing.as_ref().map_or(arg.touched_at, |existing| {
			existing.touched_at.max(arg.touched_at)
		});

		let value = CacheEntry {
			reference_count: 0,
			touched_at,
		}
		.serialize()
		.map_err(|error| fdb::FdbBindingError::CustomError(error.into()))?;
		txn.set(&key, &value);

		let id_bytes = id.to_bytes();
		let partition = Self::partition_for_id(id_bytes.as_ref(), partition_total);
		txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
			.unwrap();
		let key = Key::Clean {
			partition,
			touched_at,
			kind: ItemKind::CacheEntry,
			id: tg::Either::Left(arg.id.clone().into()),
		};
		let key = Self::pack(subspace, &key);
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

		let merge = !arg.complete();
		let existing = if merge {
			txn.get(&key, false)
				.await?
				.and_then(|bytes| Object::deserialize(&bytes).ok())
		} else {
			None
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

		let merge = !arg.complete();
		let existing = if merge {
			txn.get(&key, false)
				.await?
				.and_then(|bytes| Process::deserialize(&bytes).ok())
		} else {
			None
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
