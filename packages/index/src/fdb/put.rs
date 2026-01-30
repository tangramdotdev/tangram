use {
	super::{
		CacheEntryCoreField, CacheEntryField, Index, ItemKind, Key, KeyKind, ObjectCoreField,
		ObjectField, ObjectMetadataField, ObjectStoredField, ProcessCoreField, ProcessField,
		ProcessMetadataField, ProcessStoredField, Update,
	},
	crate::{ProcessObjectKind, PutArg, PutCacheEntryArg, PutObjectArg, PutProcessArg},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	futures::{StreamExt as _, stream},
	num_traits::ToPrimitive as _,
	std::{
		collections::HashSet,
		sync::{Arc, Mutex},
	},
	tangram_client::prelude::*,
	tangram_util::varint,
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

enum ObjectSubtreeMetadataField {
	Count,
	Depth,
	Size,
	Solvable,
	Solved,
}

impl Index {
	pub async fn put(&self, arg: PutArg) -> tg::Result<()> {
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
		max_keys_per_transaction: usize,
		partition_total: u64,
	) {
		futures::stream::unfold(&mut receiver, |receiver| async move {
			let request = receiver.recv().await?;
			let mut requests = vec![request];
			while let Ok(request) = receiver.try_recv() {
				requests.push(request);
			}
			let batches = Self::put_task_create_batches(requests, max_keys_per_transaction);
			Some((batches, receiver))
		})
		.flat_map(stream::iter)
		.for_each_concurrent(concurrency, |batch| {
			let database = database.clone();
			let subspace = subspace.clone();
			async move {
				let result =
					Self::put_task_put_batch(&database, &subspace, &batch.arg, partition_total)
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

	fn put_task_create_batches(requests: Vec<Request>, max_key_count: usize) -> Vec<Batch> {
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
		let mut current_key_count = 0;

		for (item, tracker) in items {
			let key_count = match &item {
				Item::CacheEntry(_) => 3,
				Item::Object(o) => Self::put_task_object_key_count(o),
				Item::Process(p) => Self::put_task_process_key_count(p),
			};

			if current_key_count + key_count > max_key_count && current_key_count > 0 {
				for tracker in &current_trackers {
					tracker.lock().unwrap().remaining += 1;
				}
				batches.push(Batch {
					arg: std::mem::take(&mut current_arg),
					trackers: std::mem::take(&mut current_trackers),
				});
				current_tracker_set.clear();
				current_key_count = 0;
			}

			match item {
				Item::CacheEntry(c) => current_arg.cache_entries.push(c),
				Item::Object(o) => current_arg.objects.push(o),
				Item::Process(p) => current_arg.processes.push(p),
			}
			current_key_count += key_count;

			let ptr = Arc::as_ptr(&tracker);
			if current_tracker_set.insert(ptr) {
				current_trackers.push(tracker);
			}
		}

		if current_key_count > 0 {
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

	fn put_task_object_key_count(arg: &PutObjectArg) -> usize {
		let base = 8;
		let cache_entry_field = usize::from(arg.cache_entry.is_some());
		let subtree_count = usize::from(arg.metadata.subtree.count.is_some());
		let subtree_depth = usize::from(arg.metadata.subtree.depth.is_some());
		let subtree_size = usize::from(arg.metadata.subtree.size.is_some());
		let subtree_solvable = usize::from(arg.metadata.subtree.solvable.is_some());
		let subtree_solved = usize::from(arg.metadata.subtree.solved.is_some());
		let subtree_fields =
			subtree_count + subtree_depth + subtree_size + subtree_solvable + subtree_solved;
		let stored_subtree = usize::from(arg.stored.subtree);
		let children = arg.children.len() * 2;
		let cache_entry_relations = if arg.cache_entry.is_some() { 2 } else { 0 };
		base + cache_entry_field
			+ subtree_fields
			+ stored_subtree
			+ children
			+ cache_entry_relations
	}

	fn put_task_process_key_count(arg: &PutProcessArg) -> usize {
		let base = 5;

		let node_metadata = Self::put_task_subtree_field_count(&arg.metadata.node.command)
			+ Self::put_task_subtree_field_count(&arg.metadata.node.error)
			+ Self::put_task_subtree_field_count(&arg.metadata.node.log)
			+ Self::put_task_subtree_field_count(&arg.metadata.node.output);

		let subtree_count = usize::from(arg.metadata.subtree.count.is_some());

		let subtree_metadata = Self::put_task_subtree_field_count(&arg.metadata.subtree.command)
			+ Self::put_task_subtree_field_count(&arg.metadata.subtree.error)
			+ Self::put_task_subtree_field_count(&arg.metadata.subtree.log)
			+ Self::put_task_subtree_field_count(&arg.metadata.subtree.output);

		let node_command = usize::from(arg.stored.node_command);
		let node_error = usize::from(arg.stored.node_error);
		let node_log = usize::from(arg.stored.node_log);
		let node_output = usize::from(arg.stored.node_output);
		let subtree = usize::from(arg.stored.subtree);
		let subtree_command = usize::from(arg.stored.subtree_command);
		let subtree_error = usize::from(arg.stored.subtree_error);
		let subtree_log = usize::from(arg.stored.subtree_log);
		let subtree_output = usize::from(arg.stored.subtree_output);
		let stored_flags = node_command
			+ node_error
			+ node_log
			+ node_output
			+ subtree + subtree_command
			+ subtree_error
			+ subtree_log
			+ subtree_output;

		let children = arg.children.len() * 2;
		let objects = arg.objects.len() * 2;

		base + node_metadata + subtree_count + subtree_metadata + stored_flags + children + objects
	}

	fn put_task_subtree_field_count(subtree: &tg::object::metadata::Subtree) -> usize {
		let count = usize::from(subtree.count.is_some());
		let depth = usize::from(subtree.depth.is_some());
		let size = usize::from(subtree.size.is_some());
		let solvable = usize::from(subtree.solvable.is_some());
		let solved = usize::from(subtree.solved.is_some());
		count + depth + size + solvable + solved
	}

	async fn put_task_put_batch(
		database: &fdb::Database,
		subspace: &fdbt::Subspace,
		arg: &PutArg,
		partition_total: u64,
	) -> tg::Result<()> {
		let result = database
			.run(|txn, _| {
				let subspace = subspace.clone();
				let arg = arg.clone();
				async move {
					for cache_entry in &arg.cache_entries {
						Self::put_cache_entry(&txn, &subspace, cache_entry, partition_total);
					}
					for object in &arg.objects {
						Self::put_object(&txn, &subspace, object, partition_total);
					}
					for process in &arg.processes {
						Self::put_process(&txn, &subspace, process, partition_total);
					}
					Ok::<_, fdb::FdbBindingError>(())
				}
			})
			.await;

		match result {
			Ok(()) => (),
			Err(fdb::FdbBindingError::NonRetryableFdbError(error)) if error.code() == 2101 => {
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
				))
				.await?;
				Box::pin(Self::put_task_put_batch(
					database,
					subspace,
					&right,
					partition_total,
				))
				.await?;
			},
			Err(error) => {
				return Err(tg::error!(!error, "failed to put"));
			},
		}

		Ok(())
	}

	fn put_cache_entry(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &PutCacheEntryArg,
		partition_total: u64,
	) {
		let id = &arg.id;

		txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
			.unwrap();
		let key = Key::CacheEntry {
			id: id.clone(),
			field: CacheEntryField::Core(CacheEntryCoreField::Exists),
		};
		let key = Self::pack(subspace, &key);
		txn.set(&key, &[]);

		let key = Key::CacheEntry {
			id: id.clone(),
			field: CacheEntryField::Core(CacheEntryCoreField::TouchedAt),
		};
		let key = Self::pack(subspace, &key);
		txn.atomic_op(
			&key,
			&arg.touched_at.to_le_bytes(),
			fdb::options::MutationType::Max,
		);

		let id_bytes = id.to_bytes();
		let partition = Self::partition_for_id(id_bytes.as_ref(), partition_total);
		txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
			.unwrap();
		let key = Key::Clean {
			partition,
			touched_at: arg.touched_at,
			kind: ItemKind::CacheEntry,
			id: tg::Either::Left(arg.id.clone().into()),
		};
		let key = Self::pack(subspace, &key);
		txn.set(&key, &[]);
	}

	fn put_object(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &PutObjectArg,
		partition_total: u64,
	) {
		let id = &arg.id;

		txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
			.unwrap();
		let key = Key::Object {
			id: id.clone(),
			field: ObjectField::Core(ObjectCoreField::Exists),
		};
		let key = Self::pack(subspace, &key);
		txn.set(&key, &[]);

		let key = Key::Object {
			id: id.clone(),
			field: ObjectField::Core(ObjectCoreField::TouchedAt),
		};
		let key = Self::pack(subspace, &key);
		txn.atomic_op(
			&key,
			&arg.touched_at.to_le_bytes(),
			fdb::options::MutationType::Max,
		);

		if let Some(cache_entry) = &arg.cache_entry {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = Key::Object {
				id: id.clone(),
				field: ObjectField::Core(ObjectCoreField::CacheEntry),
			};
			let key = Self::pack(subspace, &key);
			txn.set(&key, cache_entry.to_bytes().as_ref());
		}

		// Always write node.size, node.solvable, and node.solved.
		txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
			.unwrap();
		let key = Key::Object {
			id: id.clone(),
			field: ObjectField::Metadata(ObjectMetadataField::NodeSize),
		};
		let key = Self::pack(subspace, &key);
		txn.set(&key, &varint::encode_uvarint(arg.metadata.node.size));

		txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
			.unwrap();
		let key = Key::Object {
			id: id.clone(),
			field: ObjectField::Metadata(ObjectMetadataField::NodeSolvable),
		};
		let key = Self::pack(subspace, &key);
		txn.set(&key, &[u8::from(arg.metadata.node.solvable)]);

		txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
			.unwrap();
		let key = Key::Object {
			id: id.clone(),
			field: ObjectField::Metadata(ObjectMetadataField::NodeSolved),
		};
		let key = Self::pack(subspace, &key);
		txn.set(&key, &[u8::from(arg.metadata.node.solved)]);

		if let Some(count) = arg.metadata.subtree.count {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = Key::Object {
				id: id.clone(),
				field: ObjectField::Metadata(ObjectMetadataField::SubtreeCount),
			};
			let key = Self::pack(subspace, &key);
			txn.set(&key, &varint::encode_uvarint(count));
		}
		if let Some(depth) = arg.metadata.subtree.depth {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = Key::Object {
				id: id.clone(),
				field: ObjectField::Metadata(ObjectMetadataField::SubtreeDepth),
			};
			let key = Self::pack(subspace, &key);
			txn.set(&key, &varint::encode_uvarint(depth));
		}
		if let Some(size) = arg.metadata.subtree.size {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = Key::Object {
				id: id.clone(),
				field: ObjectField::Metadata(ObjectMetadataField::SubtreeSize),
			};
			let key = Self::pack(subspace, &key);
			txn.set(&key, &varint::encode_uvarint(size));
		}
		if let Some(solvable) = arg.metadata.subtree.solvable {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = Key::Object {
				id: id.clone(),
				field: ObjectField::Metadata(ObjectMetadataField::SubtreeSolvable),
			};
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[u8::from(solvable)]);
		}
		if let Some(solved) = arg.metadata.subtree.solved {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = Key::Object {
				id: id.clone(),
				field: ObjectField::Metadata(ObjectMetadataField::SubtreeSolved),
			};
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[u8::from(solved)]);
		}

		if arg.stored.subtree {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = Key::Object {
				id: id.clone(),
				field: ObjectField::Stored(ObjectStoredField::Subtree),
			};
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);
		}

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
			touched_at: arg.touched_at,
			kind: ItemKind::Object,
			id: tg::Either::Left(id.clone()),
		};
		let key = Self::pack(subspace, &key);
		txn.set(&key, &[]);

		Self::enqueue_put_update(
			txn,
			subspace,
			&tg::Either::Left(id.clone()),
			partition_total,
		);
	}

	fn put_process(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		arg: &PutProcessArg,
		partition_total: u64,
	) {
		let id = &arg.id;

		txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
			.unwrap();
		let key = Key::Process {
			id: id.clone(),
			field: ProcessField::Core(ProcessCoreField::Exists),
		};
		let key = Self::pack(subspace, &key);
		txn.set(&key, &[]);

		let key = Key::Process {
			id: id.clone(),
			field: ProcessField::Core(ProcessCoreField::TouchedAt),
		};
		let key = Self::pack(subspace, &key);
		txn.atomic_op(
			&key,
			&arg.touched_at.to_le_bytes(),
			fdb::options::MutationType::Max,
		);

		Self::put_process_object_metadata(
			txn,
			subspace,
			id,
			&arg.metadata.node.command,
			ProcessObjectKind::Command,
			false,
		);
		Self::put_process_object_metadata(
			txn,
			subspace,
			id,
			&arg.metadata.node.error,
			ProcessObjectKind::Error,
			false,
		);
		Self::put_process_object_metadata(
			txn,
			subspace,
			id,
			&arg.metadata.node.log,
			ProcessObjectKind::Log,
			false,
		);
		Self::put_process_object_metadata(
			txn,
			subspace,
			id,
			&arg.metadata.node.output,
			ProcessObjectKind::Output,
			false,
		);

		if let Some(count) = arg.metadata.subtree.count {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = Key::Process {
				id: id.clone(),
				field: ProcessField::Metadata(ProcessMetadataField::SubtreeCount),
			};
			let key = Self::pack(subspace, &key);
			txn.set(&key, &varint::encode_uvarint(count));
		}
		Self::put_process_object_metadata(
			txn,
			subspace,
			id,
			&arg.metadata.subtree.command,
			ProcessObjectKind::Command,
			true,
		);
		Self::put_process_object_metadata(
			txn,
			subspace,
			id,
			&arg.metadata.subtree.error,
			ProcessObjectKind::Error,
			true,
		);
		Self::put_process_object_metadata(
			txn,
			subspace,
			id,
			&arg.metadata.subtree.log,
			ProcessObjectKind::Log,
			true,
		);
		Self::put_process_object_metadata(
			txn,
			subspace,
			id,
			&arg.metadata.subtree.output,
			ProcessObjectKind::Output,
			true,
		);

		if arg.stored.node_command {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = Key::Process {
				id: id.clone(),
				field: ProcessField::Stored(ProcessStoredField::NodeCommand),
			};
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);
		}
		if arg.stored.node_error {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = Key::Process {
				id: id.clone(),
				field: ProcessField::Stored(ProcessStoredField::NodeError),
			};
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);
		}
		if arg.stored.node_log {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = Key::Process {
				id: id.clone(),
				field: ProcessField::Stored(ProcessStoredField::NodeLog),
			};
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);
		}
		if arg.stored.node_output {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = Key::Process {
				id: id.clone(),
				field: ProcessField::Stored(ProcessStoredField::NodeOutput),
			};
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);
		}
		if arg.stored.subtree {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = Key::Process {
				id: id.clone(),
				field: ProcessField::Stored(ProcessStoredField::Subtree),
			};
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);
		}
		if arg.stored.subtree_command {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = Key::Process {
				id: id.clone(),
				field: ProcessField::Stored(ProcessStoredField::SubtreeCommand),
			};
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);
		}
		if arg.stored.subtree_error {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = Key::Process {
				id: id.clone(),
				field: ProcessField::Stored(ProcessStoredField::SubtreeError),
			};
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);
		}
		if arg.stored.subtree_log {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = Key::Process {
				id: id.clone(),
				field: ProcessField::Stored(ProcessStoredField::SubtreeLog),
			};
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);
		}
		if arg.stored.subtree_output {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = Key::Process {
				id: id.clone(),
				field: ProcessField::Stored(ProcessStoredField::SubtreeOutput),
			};
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[]);
		}

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
			touched_at: arg.touched_at,
			kind: ItemKind::Process,
			id: tg::Either::Right(id.clone()),
		};
		let key = Self::pack(subspace, &key);
		txn.set(&key, &[]);

		Self::enqueue_put_update(
			txn,
			subspace,
			&tg::Either::Right(id.clone()),
			partition_total,
		);
	}

	fn put_process_object_metadata(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		id: &tg::process::Id,
		metadata: &tg::object::metadata::Subtree,
		kind: ProcessObjectKind,
		subtree: bool,
	) {
		if let Some(count) = metadata.count {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = Key::Process {
				id: id.clone(),
				field: ProcessField::Metadata(ProcessMetadataField::from_object_metadata_field(
					ObjectSubtreeMetadataField::Count,
					kind,
					subtree,
				)),
			};
			let key = Self::pack(subspace, &key);
			txn.set(&key, &varint::encode_uvarint(count));
		}
		if let Some(depth) = metadata.depth {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = Key::Process {
				id: id.clone(),
				field: ProcessField::Metadata(ProcessMetadataField::from_object_metadata_field(
					ObjectSubtreeMetadataField::Depth,
					kind,
					subtree,
				)),
			};
			let key = Self::pack(subspace, &key);
			txn.set(&key, &varint::encode_uvarint(depth));
		}
		if let Some(size) = metadata.size {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = Key::Process {
				id: id.clone(),
				field: ProcessField::Metadata(ProcessMetadataField::from_object_metadata_field(
					ObjectSubtreeMetadataField::Size,
					kind,
					subtree,
				)),
			};
			let key = Self::pack(subspace, &key);
			txn.set(&key, &varint::encode_uvarint(size));
		}
		if let Some(solvable) = metadata.solvable {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = Key::Process {
				id: id.clone(),
				field: ProcessField::Metadata(ProcessMetadataField::from_object_metadata_field(
					ObjectSubtreeMetadataField::Solvable,
					kind,
					subtree,
				)),
			};
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[u8::from(solvable)]);
		}
		if let Some(solved) = metadata.solved {
			txn.set_option(fdb::options::TransactionOption::NextWriteNoWriteConflictRange)
				.unwrap();
			let key = Key::Process {
				id: id.clone(),
				field: ProcessField::Metadata(ProcessMetadataField::from_object_metadata_field(
					ObjectSubtreeMetadataField::Solved,
					kind,
					subtree,
				)),
			};
			let key = Self::pack(subspace, &key);
			txn.set(&key, &[u8::from(solved)]);
		}
	}

	fn enqueue_put_update(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		id: &tg::Either<tg::object::Id, tg::process::Id>,
		partition_total: u64,
	) {
		let key = Self::pack(subspace, &Key::Update { id: id.clone() });
		let value = Update::Put.serialize().unwrap();
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

impl ProcessMetadataField {
	fn from_object_metadata_field(
		field: ObjectSubtreeMetadataField,
		kind: ProcessObjectKind,
		subtree: bool,
	) -> Self {
		match (kind, subtree, field) {
			(ProcessObjectKind::Command, false, ObjectSubtreeMetadataField::Count) => {
				Self::NodeCommandCount
			},
			(ProcessObjectKind::Command, false, ObjectSubtreeMetadataField::Depth) => {
				Self::NodeCommandDepth
			},
			(ProcessObjectKind::Command, false, ObjectSubtreeMetadataField::Size) => {
				Self::NodeCommandSize
			},
			(ProcessObjectKind::Command, false, ObjectSubtreeMetadataField::Solvable) => {
				Self::NodeCommandSolvable
			},
			(ProcessObjectKind::Command, false, ObjectSubtreeMetadataField::Solved) => {
				Self::NodeCommandSolved
			},
			(ProcessObjectKind::Error, false, ObjectSubtreeMetadataField::Count) => {
				Self::NodeErrorCount
			},
			(ProcessObjectKind::Error, false, ObjectSubtreeMetadataField::Depth) => {
				Self::NodeErrorDepth
			},
			(ProcessObjectKind::Error, false, ObjectSubtreeMetadataField::Size) => {
				Self::NodeErrorSize
			},
			(ProcessObjectKind::Error, false, ObjectSubtreeMetadataField::Solvable) => {
				Self::NodeErrorSolvable
			},
			(ProcessObjectKind::Error, false, ObjectSubtreeMetadataField::Solved) => {
				Self::NodeErrorSolved
			},
			(ProcessObjectKind::Log, false, ObjectSubtreeMetadataField::Count) => {
				Self::NodeLogCount
			},
			(ProcessObjectKind::Log, false, ObjectSubtreeMetadataField::Depth) => {
				Self::NodeLogDepth
			},
			(ProcessObjectKind::Log, false, ObjectSubtreeMetadataField::Size) => Self::NodeLogSize,
			(ProcessObjectKind::Log, false, ObjectSubtreeMetadataField::Solvable) => {
				Self::NodeLogSolvable
			},
			(ProcessObjectKind::Log, false, ObjectSubtreeMetadataField::Solved) => {
				Self::NodeLogSolved
			},
			(ProcessObjectKind::Output, false, ObjectSubtreeMetadataField::Count) => {
				Self::NodeOutputCount
			},
			(ProcessObjectKind::Output, false, ObjectSubtreeMetadataField::Depth) => {
				Self::NodeOutputDepth
			},
			(ProcessObjectKind::Output, false, ObjectSubtreeMetadataField::Size) => {
				Self::NodeOutputSize
			},
			(ProcessObjectKind::Output, false, ObjectSubtreeMetadataField::Solvable) => {
				Self::NodeOutputSolvable
			},
			(ProcessObjectKind::Output, false, ObjectSubtreeMetadataField::Solved) => {
				Self::NodeOutputSolved
			},
			(ProcessObjectKind::Command, true, ObjectSubtreeMetadataField::Count) => {
				Self::SubtreeCommandCount
			},
			(ProcessObjectKind::Command, true, ObjectSubtreeMetadataField::Depth) => {
				Self::SubtreeCommandDepth
			},
			(ProcessObjectKind::Command, true, ObjectSubtreeMetadataField::Size) => {
				Self::SubtreeCommandSize
			},
			(ProcessObjectKind::Command, true, ObjectSubtreeMetadataField::Solvable) => {
				Self::SubtreeCommandSolvable
			},
			(ProcessObjectKind::Command, true, ObjectSubtreeMetadataField::Solved) => {
				Self::SubtreeCommandSolved
			},
			(ProcessObjectKind::Error, true, ObjectSubtreeMetadataField::Count) => {
				Self::SubtreeErrorCount
			},
			(ProcessObjectKind::Error, true, ObjectSubtreeMetadataField::Depth) => {
				Self::SubtreeErrorDepth
			},
			(ProcessObjectKind::Error, true, ObjectSubtreeMetadataField::Size) => {
				Self::SubtreeErrorSize
			},
			(ProcessObjectKind::Error, true, ObjectSubtreeMetadataField::Solvable) => {
				Self::SubtreeErrorSolvable
			},
			(ProcessObjectKind::Error, true, ObjectSubtreeMetadataField::Solved) => {
				Self::SubtreeErrorSolved
			},
			(ProcessObjectKind::Log, true, ObjectSubtreeMetadataField::Count) => {
				Self::SubtreeLogCount
			},
			(ProcessObjectKind::Log, true, ObjectSubtreeMetadataField::Depth) => {
				Self::SubtreeLogDepth
			},
			(ProcessObjectKind::Log, true, ObjectSubtreeMetadataField::Size) => {
				Self::SubtreeLogSize
			},
			(ProcessObjectKind::Log, true, ObjectSubtreeMetadataField::Solvable) => {
				Self::SubtreeLogSolvable
			},
			(ProcessObjectKind::Log, true, ObjectSubtreeMetadataField::Solved) => {
				Self::SubtreeLogSolved
			},
			(ProcessObjectKind::Output, true, ObjectSubtreeMetadataField::Count) => {
				Self::SubtreeOutputCount
			},
			(ProcessObjectKind::Output, true, ObjectSubtreeMetadataField::Depth) => {
				Self::SubtreeOutputDepth
			},
			(ProcessObjectKind::Output, true, ObjectSubtreeMetadataField::Size) => {
				Self::SubtreeOutputSize
			},
			(ProcessObjectKind::Output, true, ObjectSubtreeMetadataField::Solvable) => {
				Self::SubtreeOutputSolvable
			},
			(ProcessObjectKind::Output, true, ObjectSubtreeMetadataField::Solved) => {
				Self::SubtreeOutputSolved
			},
		}
	}
}
