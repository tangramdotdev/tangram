use {
	crate::{CleanOutput, ItemArg, Object, Process, ProcessObjectKind, PutArg, PutTagArg},
	crossbeam_channel as crossbeam, foundationdb_tuple as fdbt, heed as lmdb,
	num_traits::{FromPrimitive as _, ToPrimitive as _},
	std::{collections::VecDeque, path::PathBuf},
	tangram_client::prelude::*,
};

mod clean;
mod get;
mod put;
mod tag;
mod touch;
mod update;

#[derive(Clone, Debug)]
pub struct Config {
	pub map_size: usize,
	pub max_items_per_transaction: usize,
	pub path: PathBuf,
}

pub struct Index {
	db: Db,
	env: lmdb::Env,
	sender_high: RequestSender,
	sender_low: RequestSender,
	subspace: fdbt::Subspace,
}

type Db = lmdb::Database<lmdb::types::Bytes, lmdb::types::Bytes>;

type RequestSender = crossbeam::Sender<(Request, ResponseSender)>;
type RequestReceiver = crossbeam::Receiver<(Request, ResponseSender)>;
type ResponseSender = tokio::sync::oneshot::Sender<tg::Result<Response>>;

#[derive(Clone)]
enum Request {
	Clean {
		max_touched_at: i64,
		batch_size: usize,
	},
	DeleteTags(Vec<String>),
	Put(PutArg),
	PutTags(Vec<PutTagArg>),
	TouchCacheEntries {
		ids: Vec<tg::artifact::Id>,
		touched_at: i64,
	},
	TouchObjects {
		ids: Vec<tg::object::Id>,
		touched_at: i64,
	},
	TouchProcesses {
		ids: Vec<tg::process::Id>,
		touched_at: i64,
	},
	Update {
		batch_size: usize,
	},
}

#[derive(Clone)]
enum Response {
	Unit,
	CacheEntries(Vec<Option<crate::CacheEntry>>),
	Objects(Vec<Option<Object>>),
	Processes(Vec<Option<Process>>),
	CleanOutput(CleanOutput),
	UpdateCount(usize),
}

struct RequestTracker {
	remaining: usize,
	response: tg::Result<Response>,
	sender: Option<ResponseSender>,
}

struct Batch {
	requests: Vec<Request>,
	tracker_indices: Vec<usize>,
}

#[derive(Debug)]
enum Key {
	CacheEntry(tg::artifact::Id),
	Object(tg::object::Id),
	Process(tg::process::Id),
	Tag(String),
	CacheEntryDependency {
		cache_entry: tg::artifact::Id,
		dependency: tg::artifact::Id,
	},
	DependencyCacheEntry {
		dependency: tg::artifact::Id,
		cache_entry: tg::artifact::Id,
	},
	ObjectChild {
		object: tg::object::Id,
		child: tg::object::Id,
	},
	ChildObject {
		child: tg::object::Id,
		object: tg::object::Id,
	},
	ObjectCacheEntry {
		object: tg::object::Id,
		cache_entry: tg::artifact::Id,
	},
	CacheEntryObject {
		cache_entry: tg::artifact::Id,
		object: tg::object::Id,
	},
	ProcessChild {
		process: tg::process::Id,
		child: tg::process::Id,
	},
	ChildProcess {
		child: tg::process::Id,
		parent: tg::process::Id,
	},
	ProcessObject {
		process: tg::process::Id,
		kind: ProcessObjectKind,
		object: tg::object::Id,
	},
	ObjectProcess {
		object: tg::object::Id,
		kind: ProcessObjectKind,
		process: tg::process::Id,
	},
	ItemTag {
		item: Vec<u8>,
		tag: String,
	},
	Clean {
		touched_at: i64,
		kind: ItemKind,
		id: tg::Either<tg::object::Id, tg::process::Id>,
	},
	Update {
		id: tg::Either<tg::object::Id, tg::process::Id>,
	},
	UpdateVersion {
		version: u64,
		id: tg::Either<tg::object::Id, tg::process::Id>,
	},
}

#[derive(Clone, Copy, Debug, PartialEq, num_derive::FromPrimitive, num_derive::ToPrimitive)]
#[repr(u8)]
enum KeyKind {
	CacheEntry = 0,
	Object = 1,
	Process = 2,
	Tag = 3,
	CacheEntryDependency = 4,
	DependencyCacheEntry = 5,
	ObjectChild = 6,
	ChildObject = 7,
	ObjectCacheEntry = 8,
	CacheEntryObject = 9,
	ProcessChild = 10,
	ChildProcess = 11,
	ProcessObject = 12,
	ObjectProcess = 13,
	ItemTag = 14,
	Clean = 15,
	Update = 16,
	UpdateVersion = 17,
}

#[derive(Clone, Copy, Debug, PartialEq, num_derive::FromPrimitive, num_derive::ToPrimitive)]
#[repr(u8)]
enum ItemKind {
	CacheEntry = 0,
	Object = 1,
	Process = 2,
}

#[derive(Clone, Copy, Debug, PartialEq, num_derive::FromPrimitive, num_derive::ToPrimitive)]
#[repr(u8)]
enum Update {
	Put = 0,
	Propagate = 1,
}

impl Index {
	pub fn new(config: &Config) -> tg::Result<Self> {
		std::fs::OpenOptions::new()
			.create(true)
			.truncate(false)
			.read(true)
			.write(true)
			.open(&config.path)
			.map_err(
				|source| tg::error!(!source, path = %config.path.display(), "failed to open the lmdb file"),
			)?;
		let env = unsafe {
			lmdb::EnvOpenOptions::new()
				.map_size(config.map_size)
				.max_dbs(3)
				.max_readers(1_000)
				.flags(
					lmdb::EnvFlags::NO_SUB_DIR
						| lmdb::EnvFlags::WRITE_MAP
						| lmdb::EnvFlags::MAP_ASYNC,
				)
				.open(&config.path)
				.map_err(|source| {
					tg::error!(!source, path = %config.path.display(), "failed to open the lmdb environment")
				})?
		};
		let mut transaction = env.write_txn().unwrap();
		let db = env
			.create_database(&mut transaction, None)
			.map_err(|source| tg::error!(!source, "failed to create the database"))?;
		transaction
			.commit()
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		let (sender_high, receiver_high) = crossbeam::bounded(256);
		let (sender_low, receiver_low) = crossbeam::bounded(256);

		let subspace = fdbt::Subspace::all();

		std::thread::spawn({
			let env = env.clone();
			let subspace = subspace.clone();
			let max_items_per_transaction = config.max_items_per_transaction;
			move || {
				Self::task(
					&env,
					&db,
					&subspace,
					&receiver_high,
					&receiver_low,
					max_items_per_transaction,
				);
			}
		});

		Ok(Self {
			db,
			env,
			sender_high,
			sender_low,
			subspace,
		})
	}

	fn pack<T: fdbt::TuplePack>(subspace: &fdbt::Subspace, key: &T) -> Vec<u8> {
		subspace.pack(key)
	}

	fn unpack<'a, T: fdbt::TupleUnpack<'a>>(
		subspace: &fdbt::Subspace,
		bytes: &'a [u8],
	) -> tg::Result<T> {
		subspace
			.unpack(bytes)
			.map_err(|source| tg::error!(!source, "failed to unpack key"))
	}

	fn task(
		env: &lmdb::Env,
		db: &Db,
		subspace: &fdbt::Subspace,
		receiver_high: &RequestReceiver,
		receiver_low: &RequestReceiver,
		max_items_per_transaction: usize,
	) {
		let mut trackers: Vec<RequestTracker> = Vec::new();
		let mut queue_high: VecDeque<Batch> = VecDeque::new();
		let mut queue_low: VecDeque<Batch> = VecDeque::new();

		loop {
			// Drain high-priority requests into the high-priority queue.
			let mut high_requests: Vec<(Request, ResponseSender)> = Vec::new();
			while let Ok(item) = receiver_high.try_recv() {
				high_requests.push(item);
			}
			if !high_requests.is_empty() {
				let batches =
					Self::create_batches(high_requests, &mut trackers, max_items_per_transaction);
				queue_high.extend(batches);
			}

			// If both queues are empty, try low-priority requests.
			if queue_high.is_empty() && queue_low.is_empty() {
				let mut low_requests: Vec<(Request, ResponseSender)> = Vec::new();
				while let Ok(item) = receiver_low.try_recv() {
					low_requests.push(item);
				}
				if !low_requests.is_empty() {
					let batches = Self::create_batches(
						low_requests,
						&mut trackers,
						max_items_per_transaction,
					);
					queue_low.extend(batches);
				}
			}

			// If still empty, block until a request arrives.
			if queue_high.is_empty() && queue_low.is_empty() {
				crossbeam::select! {
					recv(receiver_high) -> result => {
						if let Ok(item) = result {
							let batches = Self::create_batches(
								vec![item],
								&mut trackers,
								max_items_per_transaction,
							);
							queue_high.extend(batches);
						}
					},
					recv(receiver_low) -> result => {
						if let Ok(item) = result {
							let batches = Self::create_batches(
								vec![item],
								&mut trackers,
								max_items_per_transaction,
							);
							queue_low.extend(batches);
						}
					},
				}

				// After waking, drain high-priority requests.
				let mut high_requests: Vec<(Request, ResponseSender)> = Vec::new();
				while let Ok(item) = receiver_high.try_recv() {
					high_requests.push(item);
				}
				if !high_requests.is_empty() {
					let batches = Self::create_batches(
						high_requests,
						&mut trackers,
						max_items_per_transaction,
					);
					queue_high.extend(batches);
				}
			}

			// Pop a batch from high-priority first, then low-priority.
			let batch = if let Some(batch) = queue_high.pop_front() {
				batch
			} else if let Some(batch) = queue_low.pop_front() {
				batch
			} else {
				break;
			};

			// Begin a write transaction.
			let result = env
				.write_txn()
				.map_err(|source| tg::error!(!source, "failed to begin a transaction"));
			let mut transaction = match result {
				Ok(transaction) => transaction,
				Err(error) => {
					for tracker_index in &batch.tracker_indices {
						let tracker = &mut trackers[*tracker_index];
						tracker.response = Err(error.clone());
						tracker.remaining -= 1;
						if tracker.remaining == 0
							&& let Some(sender) = tracker.sender.take()
						{
							let tracker =
								std::mem::replace(&mut tracker.response, Ok(Response::Unit));
							sender.send(tracker).ok();
						}
					}
					continue;
				},
			};

			// Process all requests in the batch.
			let mut results: Vec<tg::Result<Response>> = Vec::new();
			for request in batch.requests {
				let result = match request {
					Request::Clean {
						max_touched_at,
						batch_size,
					} => {
						Self::task_clean(db, subspace, &mut transaction, max_touched_at, batch_size)
							.map(Response::CleanOutput)
					},
					Request::DeleteTags(tags) => {
						Self::task_delete_tags(db, subspace, &mut transaction, &tags)
							.map(|()| Response::Unit)
					},
					Request::Put(arg) => {
						Self::task_put(db, subspace, &mut transaction, arg).map(|()| Response::Unit)
					},
					Request::PutTags(tags) => {
						Self::task_put_tags(db, subspace, &mut transaction, &tags)
							.map(|()| Response::Unit)
					},
					Request::TouchCacheEntries { ids, touched_at } => {
						Self::task_touch_cache_entries(
							db,
							subspace,
							&mut transaction,
							&ids,
							touched_at,
						)
						.map(Response::CacheEntries)
					},
					Request::TouchObjects { ids, touched_at } => {
						Self::task_touch_objects(db, subspace, &mut transaction, &ids, touched_at)
							.map(Response::Objects)
					},
					Request::TouchProcesses { ids, touched_at } => {
						Self::task_touch_processes(db, subspace, &mut transaction, &ids, touched_at)
							.map(Response::Processes)
					},
					Request::Update { batch_size } => {
						Self::task_update_batch(db, subspace, &mut transaction, batch_size)
							.map(Response::UpdateCount)
					},
				};
				results.push(result);
			}

			// Commit the transaction.
			let commit_result = transaction
				.commit()
				.map_err(|source| tg::error!(!source, "failed to commit the transaction"));

			// Merge the results into the trackers and send completed responses.
			for (result, tracker_index) in std::iter::zip(results, &batch.tracker_indices) {
				let tracker = &mut trackers[*tracker_index];
				if let Err(ref commit_error) = commit_result {
					if tracker.response.is_ok() {
						tracker.response = Err(commit_error.clone());
					}
				} else if let Err(error) = result {
					if tracker.response.is_ok() {
						tracker.response = Err(error);
					}
				} else if let Ok(response) = result {
					Self::merge_response(&mut tracker.response, response);
				}
				tracker.remaining -= 1;
				if tracker.remaining == 0
					&& let Some(sender) = tracker.sender.take()
				{
					sender
						.send(std::mem::replace(&mut tracker.response, Ok(Response::Unit)))
						.ok();
				}
			}

			// Clean up completed trackers.
			while trackers
				.last()
				.is_some_and(|tracker| tracker.remaining == 0 && tracker.sender.is_none())
			{
				trackers.pop();
			}
		}
	}

	fn create_batches(
		requests: Vec<(Request, ResponseSender)>,
		trackers: &mut Vec<RequestTracker>,
		max_items: usize,
	) -> Vec<Batch> {
		assert!(max_items > 0, "max_items must be greater than zero");

		let mut batches: Vec<Batch> = Vec::new();
		let mut current_batch = Batch {
			requests: Vec::new(),
			tracker_indices: Vec::new(),
		};
		let mut current_count: usize = 0;

		for (request, sender) in requests {
			let tracker_idx = trackers.len();
			trackers.push(RequestTracker {
				remaining: 0,
				response: Ok(Self::create_initial_response(&request)),
				sender: Some(sender),
			});

			let count = Self::request_item_count(&request);

			if Self::request_splittable(&request) {
				let mut remaining_request = request;
				let mut remaining_count = count;

				while remaining_count > 0 {
					let space = max_items.saturating_sub(current_count);
					if space == 0 {
						// The current batch is full. Finalize it and start a new one.
						if !current_batch.requests.is_empty() {
							batches.push(current_batch);
							current_batch = Batch {
								requests: Vec::new(),
								tracker_indices: Vec::new(),
							};
							current_count = 0;
						}
						continue;
					}

					if remaining_count <= space {
						// The entire remaining request fits in the current batch.
						current_batch.requests.push(remaining_request);
						current_batch.tracker_indices.push(tracker_idx);
						trackers[tracker_idx].remaining += 1;
						current_count += remaining_count;
						break;
					}

					// Split: take what fits, push the rest to the next iteration.
					let (left, right) = Self::split_request(remaining_request, space);
					current_batch.requests.push(left);
					current_batch.tracker_indices.push(tracker_idx);
					trackers[tracker_idx].remaining += 1;
					remaining_request = right;
					remaining_count -= space;

					// Finalize the current batch.
					batches.push(current_batch);
					current_batch = Batch {
						requests: Vec::new(),
						tracker_indices: Vec::new(),
					};
					current_count = 0;
				}
			} else {
				// Unsplittable requests (Clean, Update) go into their own batch if they would overflow.
				if current_count > 0 && current_count + count > max_items {
					batches.push(current_batch);
					current_batch = Batch {
						requests: Vec::new(),
						tracker_indices: Vec::new(),
					};
					current_count = 0;
				}
				current_batch.requests.push(request);
				current_batch.tracker_indices.push(tracker_idx);
				trackers[tracker_idx].remaining += 1;
				current_count += count;
			}
		}

		if !current_batch.requests.is_empty() {
			batches.push(current_batch);
		}

		batches
	}

	fn create_initial_response(request: &Request) -> Response {
		match request {
			Request::Put(_) | Request::PutTags(_) | Request::DeleteTags(_) => Response::Unit,
			Request::TouchCacheEntries { .. } => Response::CacheEntries(Vec::new()),
			Request::TouchObjects { .. } => Response::Objects(Vec::new()),
			Request::TouchProcesses { .. } => Response::Processes(Vec::new()),
			Request::Clean { .. } => Response::CleanOutput(CleanOutput::default()),
			Request::Update { .. } => Response::UpdateCount(0),
		}
	}

	fn request_item_count(request: &Request) -> usize {
		match request {
			Request::Put(arg) => arg.cache_entries.len() + arg.objects.len() + arg.processes.len(),
			Request::TouchCacheEntries { ids, .. } => ids.len(),
			Request::TouchObjects { ids, .. } => ids.len(),
			Request::TouchProcesses { ids, .. } => ids.len(),
			Request::PutTags(tags) => tags.len(),
			Request::DeleteTags(tags) => tags.len(),
			Request::Clean { batch_size, .. } | Request::Update { batch_size } => *batch_size,
		}
	}

	fn request_splittable(request: &Request) -> bool {
		matches!(
			request,
			Request::Put(_)
				| Request::TouchCacheEntries { .. }
				| Request::TouchObjects { .. }
				| Request::TouchProcesses { .. }
				| Request::PutTags(_)
				| Request::DeleteTags(_)
		)
	}

	fn split_request(request: Request, count: usize) -> (Request, Request) {
		match request {
			Request::Put(arg) => {
				let mut items: Vec<ItemArg> = Vec::new();
				for entry in arg.cache_entries {
					items.push(ItemArg::CacheEntry(entry));
				}
				for object in arg.objects {
					items.push(ItemArg::Object(object));
				}
				for process in arg.processes {
					items.push(ItemArg::Process(process));
				}
				let right = items.split_off(count);
				let mut left_arg = PutArg::default();
				for item in items {
					match item {
						ItemArg::CacheEntry(entry) => left_arg.cache_entries.push(entry),
						ItemArg::Object(object) => left_arg.objects.push(object),
						ItemArg::Process(process) => left_arg.processes.push(process),
					}
				}
				let mut right_arg = PutArg::default();
				for item in right {
					match item {
						ItemArg::CacheEntry(entry) => right_arg.cache_entries.push(entry),
						ItemArg::Object(object) => right_arg.objects.push(object),
						ItemArg::Process(process) => right_arg.processes.push(process),
					}
				}
				(Request::Put(left_arg), Request::Put(right_arg))
			},
			Request::TouchCacheEntries { ids, touched_at } => {
				let mut ids = ids;
				let right = ids.split_off(count);
				(
					Request::TouchCacheEntries { ids, touched_at },
					Request::TouchCacheEntries {
						ids: right,
						touched_at,
					},
				)
			},
			Request::TouchObjects { ids, touched_at } => {
				let mut ids = ids;
				let right = ids.split_off(count);
				(
					Request::TouchObjects { ids, touched_at },
					Request::TouchObjects {
						ids: right,
						touched_at,
					},
				)
			},
			Request::TouchProcesses { ids, touched_at } => {
				let mut ids = ids;
				let right = ids.split_off(count);
				(
					Request::TouchProcesses { ids, touched_at },
					Request::TouchProcesses {
						ids: right,
						touched_at,
					},
				)
			},
			Request::PutTags(tags) => {
				let mut tags = tags;
				let right = tags.split_off(count);
				(Request::PutTags(tags), Request::PutTags(right))
			},
			Request::DeleteTags(tags) => {
				let mut tags = tags;
				let right = tags.split_off(count);
				(Request::DeleteTags(tags), Request::DeleteTags(right))
			},
			_ => unreachable!(),
		}
	}

	fn merge_response(target: &mut tg::Result<Response>, source: Response) {
		let Ok(target) = target else {
			return;
		};
		match (target, source) {
			(Response::CacheEntries(existing), Response::CacheEntries(new)) => {
				existing.extend(new);
			},
			(Response::Objects(existing), Response::Objects(new)) => {
				existing.extend(new);
			},
			(Response::Processes(existing), Response::Processes(new)) => {
				existing.extend(new);
			},
			(Response::CleanOutput(existing), Response::CleanOutput(new)) => {
				existing.bytes += new.bytes;
				existing.cache_entries.extend(new.cache_entries);
				existing.objects.extend(new.objects);
				existing.processes.extend(new.processes);
				existing.done = new.done;
			},
			(Response::UpdateCount(existing), Response::UpdateCount(new)) => {
				*existing += new;
			},
			_ => {},
		}
	}

	pub async fn get_transaction_id(&self) -> tg::Result<u64> {
		let env = self.env.clone();
		tokio::task::spawn_blocking(move || {
			let transaction = env
				.read_txn()
				.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
			Ok(transaction.id() as u64)
		})
		.await
		.map_err(|source| tg::error!(!source, "failed to join the task"))?
	}

	pub async fn sync(&self) -> tg::Result<()> {
		tokio::task::spawn_blocking({
			let env = self.env.clone();
			move || {
				env.force_sync()
					.map_err(|source| tg::error!(!source, "failed to sync"))
			}
		})
		.await
		.map_err(|source| tg::error!(!source, "failed to join the task"))??;
		Ok(())
	}
}

impl crate::Index for Index {
	async fn try_get_objects(&self, ids: &[tg::object::Id]) -> tg::Result<Vec<Option<Object>>> {
		self.try_get_objects(ids).await
	}

	async fn try_get_processes(&self, ids: &[tg::process::Id]) -> tg::Result<Vec<Option<Process>>> {
		self.try_get_processes(ids).await
	}

	async fn touch_cache_entries(
		&self,
		ids: &[tg::artifact::Id],
		touched_at: i64,
	) -> tg::Result<Vec<Option<crate::CacheEntry>>> {
		self.touch_cache_entries(ids, touched_at).await
	}

	async fn touch_objects(
		&self,
		ids: &[tg::object::Id],
		touched_at: i64,
	) -> tg::Result<Vec<Option<Object>>> {
		self.touch_objects(ids, touched_at).await
	}

	async fn touch_processes(
		&self,
		ids: &[tg::process::Id],
		touched_at: i64,
	) -> tg::Result<Vec<Option<Process>>> {
		self.touch_processes(ids, touched_at).await
	}

	async fn put(&self, arg: PutArg) -> tg::Result<()> {
		self.put(arg).await
	}

	async fn put_tags(&self, args: &[PutTagArg]) -> tg::Result<()> {
		self.put_tags(args).await
	}

	async fn delete_tags(&self, tags: &[String]) -> tg::Result<()> {
		self.delete_tags(tags).await
	}

	async fn updates_finished(&self, transaction_id: u64) -> tg::Result<bool> {
		self.updates_finished(transaction_id).await
	}

	async fn update_batch(
		&self,
		batch_size: usize,
		partition_start: u64,
		partition_count: u64,
	) -> tg::Result<usize> {
		Index::update_batch(self, batch_size, partition_start, partition_count).await
	}

	async fn get_transaction_id(&self) -> tg::Result<u64> {
		self.get_transaction_id().await
	}

	async fn clean(
		&self,
		max_touched_at: i64,
		batch_size: usize,
		partition_start: u64,
		partition_count: u64,
	) -> tg::Result<CleanOutput> {
		Index::clean(
			self,
			max_touched_at,
			batch_size,
			partition_start,
			partition_count,
		)
		.await
	}

	async fn sync(&self) -> tg::Result<()> {
		self.sync().await
	}

	fn partition_total(&self) -> u64 {
		1
	}
}

impl fdbt::TuplePack for Key {
	fn pack<W: std::io::Write>(
		&self,
		w: &mut W,
		tuple_depth: fdbt::TupleDepth,
	) -> std::io::Result<fdbt::VersionstampOffset> {
		match self {
			Key::CacheEntry(id) => (
				KeyKind::CacheEntry.to_i32().unwrap(),
				id.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Object(id) => {
				(KeyKind::Object.to_i32().unwrap(), id.to_bytes().as_ref()).pack(w, tuple_depth)
			},

			Key::Process(id) => {
				(KeyKind::Process.to_i32().unwrap(), id.to_bytes().as_ref()).pack(w, tuple_depth)
			},

			Key::Tag(tag) => (KeyKind::Tag.to_i32().unwrap(), tag.as_str()).pack(w, tuple_depth),

			Key::CacheEntryDependency {
				cache_entry,
				dependency,
			} => (
				KeyKind::CacheEntryDependency.to_i32().unwrap(),
				cache_entry.to_bytes().as_ref(),
				dependency.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::DependencyCacheEntry {
				dependency,
				cache_entry,
			} => (
				KeyKind::DependencyCacheEntry.to_i32().unwrap(),
				dependency.to_bytes().as_ref(),
				cache_entry.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::ObjectChild { object, child } => (
				KeyKind::ObjectChild.to_i32().unwrap(),
				object.to_bytes().as_ref(),
				child.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::ChildObject { child, object } => (
				KeyKind::ChildObject.to_i32().unwrap(),
				child.to_bytes().as_ref(),
				object.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::ObjectCacheEntry {
				object,
				cache_entry,
			} => (
				KeyKind::ObjectCacheEntry.to_i32().unwrap(),
				object.to_bytes().as_ref(),
				cache_entry.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::CacheEntryObject {
				cache_entry,
				object,
			} => (
				KeyKind::CacheEntryObject.to_i32().unwrap(),
				cache_entry.to_bytes().as_ref(),
				object.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::ProcessChild { process, child } => (
				KeyKind::ProcessChild.to_i32().unwrap(),
				process.to_bytes().as_ref(),
				child.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::ChildProcess { child, parent } => (
				KeyKind::ChildProcess.to_i32().unwrap(),
				child.to_bytes().as_ref(),
				parent.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::ProcessObject {
				process,
				kind,
				object,
			} => (
				KeyKind::ProcessObject.to_i32().unwrap(),
				process.to_bytes().as_ref(),
				kind.to_i32().unwrap(),
				object.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::ObjectProcess {
				object,
				kind,
				process,
			} => (
				KeyKind::ObjectProcess.to_i32().unwrap(),
				object.to_bytes().as_ref(),
				kind.to_i32().unwrap(),
				process.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::ItemTag { item, tag } => (
				KeyKind::ItemTag.to_i32().unwrap(),
				item.as_slice(),
				tag.as_str(),
			)
				.pack(w, tuple_depth),

			Key::Clean {
				touched_at,
				kind,
				id,
			} => {
				KeyKind::Clean.to_i32().unwrap().pack(w, tuple_depth)?;
				touched_at.pack(w, tuple_depth)?;
				kind.to_i32().unwrap().pack(w, tuple_depth)?;
				let id = match &id {
					tg::Either::Left(id) => id.to_bytes(),
					tg::Either::Right(id) => id.to_bytes(),
				};
				id.as_ref().pack(w, tuple_depth)
			},

			Key::Update { id } => {
				KeyKind::Update.to_i32().unwrap().pack(w, tuple_depth)?;
				let id = match &id {
					tg::Either::Left(id) => id.to_bytes(),
					tg::Either::Right(id) => id.to_bytes(),
				};
				id.as_ref().pack(w, tuple_depth)
			},

			Key::UpdateVersion { version, id } => {
				KeyKind::UpdateVersion
					.to_i32()
					.unwrap()
					.pack(w, tuple_depth)?;
				version.pack(w, tuple_depth)?;
				let id = match &id {
					tg::Either::Left(id) => id.to_bytes(),
					tg::Either::Right(id) => id.to_bytes(),
				};
				id.as_ref().pack(w, tuple_depth)
			},
		}
	}
}

impl fdbt::TupleUnpack<'_> for Key {
	fn unpack(input: &[u8], tuple_depth: fdbt::TupleDepth) -> fdbt::PackResult<(&[u8], Self)> {
		let (input, kind_value) = i32::unpack(input, tuple_depth)?;
		let kind =
			KeyKind::from_i32(kind_value).ok_or(fdbt::PackError::Message("invalid kind".into()))?;

		match kind {
			KeyKind::CacheEntry => {
				let (input, id_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let id = tg::artifact::Id::from_slice(&id_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid artifact id".into()))?;
				Ok((input, Key::CacheEntry(id)))
			},

			KeyKind::Object => {
				let (input, id_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let id = tg::object::Id::from_slice(&id_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
				Ok((input, Key::Object(id)))
			},

			KeyKind::Process => {
				let (input, id_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let id = tg::process::Id::from_slice(&id_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
				Ok((input, Key::Process(id)))
			},

			KeyKind::Tag => {
				let (input, tag): (_, String) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				Ok((input, Key::Tag(tag)))
			},

			KeyKind::CacheEntryDependency => {
				let (input, cache_entry_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, dependency_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let cache_entry = tg::artifact::Id::from_slice(&cache_entry_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid artifact id".into()))?;
				let dependency = tg::artifact::Id::from_slice(&dependency_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid artifact id".into()))?;
				let key = Key::CacheEntryDependency {
					cache_entry,
					dependency,
				};
				Ok((input, key))
			},

			KeyKind::DependencyCacheEntry => {
				let (input, dependency_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, cache_entry_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let dependency = tg::artifact::Id::from_slice(&dependency_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid artifact id".into()))?;
				let cache_entry = tg::artifact::Id::from_slice(&cache_entry_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid artifact id".into()))?;
				let key = Key::DependencyCacheEntry {
					dependency,
					cache_entry,
				};
				Ok((input, key))
			},

			KeyKind::ObjectChild => {
				let (input, object_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, child_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let object = tg::object::Id::from_slice(&object_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
				let child = tg::object::Id::from_slice(&child_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
				Ok((input, Key::ObjectChild { object, child }))
			},

			KeyKind::ChildObject => {
				let (input, child_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, object_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let child = tg::object::Id::from_slice(&child_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
				let object = tg::object::Id::from_slice(&object_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
				Ok((input, Key::ChildObject { child, object }))
			},

			KeyKind::ObjectCacheEntry => {
				let (input, object_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, cache_entry_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let object = tg::object::Id::from_slice(&object_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
				let cache_entry = tg::artifact::Id::from_slice(&cache_entry_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid artifact id".into()))?;
				let key = Key::ObjectCacheEntry {
					object,
					cache_entry,
				};
				Ok((input, key))
			},

			KeyKind::CacheEntryObject => {
				let (input, cache_entry_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, object_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let cache_entry = tg::artifact::Id::from_slice(&cache_entry_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid artifact id".into()))?;
				let object = tg::object::Id::from_slice(&object_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
				let key = Key::CacheEntryObject {
					cache_entry,
					object,
				};
				Ok((input, key))
			},

			KeyKind::ProcessChild => {
				let (input, process_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, child_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let process = tg::process::Id::from_slice(&process_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
				let child = tg::process::Id::from_slice(&child_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
				Ok((input, Key::ProcessChild { process, child }))
			},

			KeyKind::ChildProcess => {
				let (input, child_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, parent_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let child = tg::process::Id::from_slice(&child_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
				let parent = tg::process::Id::from_slice(&parent_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
				Ok((input, Key::ChildProcess { child, parent }))
			},

			KeyKind::ProcessObject => {
				let (input, process_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, kind_value) = i32::unpack(input, tuple_depth)?;
				let (input, object_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let process = tg::process::Id::from_slice(&process_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
				let kind = ProcessObjectKind::from_i32(kind_value).ok_or(
					fdbt::PackError::Message("invalid process object kind".into()),
				)?;
				let object = tg::object::Id::from_slice(&object_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
				let key = Key::ProcessObject {
					process,
					kind,
					object,
				};
				Ok((input, key))
			},

			KeyKind::ObjectProcess => {
				let (input, object_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, kind_value) = i32::unpack(input, tuple_depth)?;
				let (input, process_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let object = tg::object::Id::from_slice(&object_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
				let kind = ProcessObjectKind::from_i32(kind_value).ok_or(
					fdbt::PackError::Message("invalid process object kind".into()),
				)?;
				let process = tg::process::Id::from_slice(&process_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
				let key = Key::ObjectProcess {
					object,
					kind,
					process,
				};
				Ok((input, key))
			},

			KeyKind::ItemTag => {
				let (input, item): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, tag): (_, String) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				Ok((input, Key::ItemTag { item, tag }))
			},

			KeyKind::Clean => {
				let (input, touched_at): (_, i64) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, kind_value): (_, i32) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let kind = ItemKind::from_i32(kind_value)
					.ok_or(fdbt::PackError::Message("invalid item kind".into()))?;
				let (input, id_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let id = tg::Id::from_slice(&id_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid id".into()))?;
				let id = match kind {
					ItemKind::CacheEntry | ItemKind::Object => {
						let id = tg::object::Id::try_from(id)
							.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
						tg::Either::Left(id)
					},
					ItemKind::Process => {
						let id = tg::process::Id::try_from(id)
							.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
						tg::Either::Right(id)
					},
				};
				let key = Key::Clean {
					touched_at,
					kind,
					id,
				};
				Ok((input, key))
			},

			KeyKind::Update => {
				let (input, id): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let id = tg::Id::from_slice(&id)
					.map_err(|_| fdbt::PackError::Message("invalid id".into()))?;
				let id = if let Ok(id) = tg::process::Id::try_from(id.clone()) {
					tg::Either::Right(id)
				} else if let Ok(id) = tg::object::Id::try_from(id) {
					tg::Either::Left(id)
				} else {
					return Err(fdbt::PackError::Message("invalid id".into()));
				};
				Ok((input, Key::Update { id }))
			},

			KeyKind::UpdateVersion => {
				let (input, version): (_, u64) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, id): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let id = tg::Id::from_slice(&id)
					.map_err(|_| fdbt::PackError::Message("invalid id".into()))?;
				let id = if let Ok(id) = tg::process::Id::try_from(id.clone()) {
					tg::Either::Right(id)
				} else if let Ok(id) = tg::object::Id::try_from(id) {
					tg::Either::Left(id)
				} else {
					return Err(fdbt::PackError::Message("invalid id".into()));
				};
				Ok((input, Key::UpdateVersion { version, id }))
			},
		}
	}
}
