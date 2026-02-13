use {
	crate::{CleanOutput, Object, Process, ProcessObjectKind, PutArg, PutTagArg},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	futures::{StreamExt as _, stream},
	num_traits::{FromPrimitive as _, ToPrimitive as _},
	opentelemetry as otel,
	std::sync::{
		Arc, Mutex,
		atomic::{AtomicU64, Ordering},
	},
	tangram_client::prelude::*,
};

mod clean;
mod get;
mod put;
mod tag;
mod touch;
mod update;

pub struct Index {
	database: Arc<fdb::Database>,
	partition_total: u64,
	subspace: fdbt::Subspace,
	sender_high: RequestSender,
	sender_medium: RequestSender,
	sender_low: RequestSender,
}

pub struct Options {
	pub cluster: std::path::PathBuf,
	pub concurrency: usize,
	pub max_items_per_transaction: usize,
	pub partition_total: u64,
	pub prefix: Option<String>,
}

type RequestSender = tokio::sync::mpsc::UnboundedSender<(Request, ResponseSender)>;
type RequestReceiver = tokio::sync::mpsc::UnboundedReceiver<(Request, ResponseSender)>;
type ResponseSender = tokio::sync::oneshot::Sender<tg::Result<Response>>;

#[derive(Clone)]
enum Request {
	Clean {
		max_touched_at: i64,
		batch_size: usize,
		partition_start: u64,
		partition_count: u64,
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
		partition_start: u64,
		partition_count: u64,
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

enum RequestItem {
	Clean,
	DeleteTag(String),
	PutCacheEntry(crate::PutCacheEntryArg),
	PutObject(crate::PutObjectArg),
	PutProcess(crate::PutProcessArg),
	PutTag(PutTagArg),
	TouchCacheEntry(tg::artifact::Id),
	TouchObject(tg::object::Id),
	TouchProcess(tg::process::Id),
	Update,
}

enum RequestKind {
	Clean {
		max_touched_at: i64,
		partition_start: u64,
		partition_count: u64,
	},
	DeleteTags,
	Put,
	PutTags,
	TouchCacheEntries {
		touched_at: i64,
	},
	TouchObjects {
		touched_at: i64,
	},
	TouchProcesses {
		touched_at: i64,
	},
	Update {
		partition_start: u64,
		partition_count: u64,
	},
}

struct RequestTracker {
	remaining: usize,
	response: tg::Result<Response>,
	sender: Option<ResponseSender>,
}

struct Batch {
	requests: Vec<Request>,
	trackers: Vec<Arc<Mutex<RequestTracker>>>,
}

#[derive(Debug, Clone)]
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
		partition: u64,
		touched_at: i64,
		kind: ItemKind,
		id: tg::Either<tg::object::Id, tg::process::Id>,
	},
	Update {
		id: tg::Either<tg::object::Id, tg::process::Id>,
	},
	UpdateVersion {
		partition: u64,
		version: fdbt::Versionstamp,
		id: tg::Either<tg::object::Id, tg::process::Id>,
	},
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
enum Update {
	Put = 0,
	Propagate = 1,
}

#[derive(Clone)]
struct Metrics {
	commit_duration: otel::metrics::Histogram<f64>,
	transaction_conflict_retry: otel::metrics::Counter<u64>,
	transaction_too_large: otel::metrics::Counter<u64>,
	transactions: otel::metrics::Counter<u64>,
}

impl Index {
	pub fn new(options: &Options) -> tg::Result<Self> {
		let database = fdb::Database::new(Some(options.cluster.to_str().unwrap()))
			.map_err(|source| tg::error!(!source, "failed to open the foundationdb cluster"))?;
		let database = Arc::new(database);

		let subspace = match &options.prefix {
			Some(s) => fdbt::Subspace::from_bytes(s.clone().into_bytes()),
			None => fdbt::Subspace::all(),
		};

		let partition_total = options.partition_total;

		let metrics = Metrics::new();

		let (sender_high, receiver_high) = tokio::sync::mpsc::unbounded_channel();
		let (sender_medium, receiver_medium) = tokio::sync::mpsc::unbounded_channel();
		let (sender_low, receiver_low) = tokio::sync::mpsc::unbounded_channel();

		let concurrency = options.concurrency;
		let max_items_per_transaction = options.max_items_per_transaction;
		tokio::spawn({
			let database = database.clone();
			let subspace = subspace.clone();
			let metrics = metrics.clone();
			async move {
				Self::task(
					database,
					subspace,
					receiver_high,
					receiver_medium,
					receiver_low,
					concurrency,
					max_items_per_transaction,
					partition_total,
					metrics,
				)
				.await;
			}
		});

		let index = Self {
			database,
			partition_total,
			subspace,
			sender_high,
			sender_medium,
			sender_low,
		};

		Ok(index)
	}

	fn partition_for_id(id_bytes: &[u8], partition_total: u64) -> u64 {
		let len = id_bytes.len();
		let start = len.saturating_sub(8);
		let mut bytes = [0u8; 8];
		bytes[8 - (len - start)..].copy_from_slice(&id_bytes[start..]);
		u64::from_be_bytes(bytes) % partition_total
	}

	fn pack<T: fdbt::TuplePack>(subspace: &fdbt::Subspace, key: &T) -> Vec<u8> {
		subspace.pack(key)
	}

	fn pack_with_versionstamp<T: fdbt::TuplePack>(subspace: &fdbt::Subspace, key: &T) -> Vec<u8> {
		subspace.pack_with_versionstamp(key)
	}

	fn unpack<'a, T: fdbt::TupleUnpack<'a>>(
		subspace: &fdbt::Subspace,
		bytes: &'a [u8],
	) -> tg::Result<T> {
		subspace
			.unpack(bytes)
			.map_err(|source| tg::error!(!source, "failed to unpack key"))
	}

	pub async fn get_transaction_id(&self) -> tg::Result<u64> {
		let txn = self
			.database
			.create_trx()
			.map_err(|source| tg::error!(!source, "failed to create the transaction"))?;
		let version = txn
			.get_read_version()
			.await
			.map_err(|source| tg::error!(!source, "failed to get the read version"))?
			.cast_unsigned();
		Ok(version)
	}

	pub async fn sync(&self) -> tg::Result<()> {
		Ok(())
	}

	#[allow(clippy::too_many_arguments)]
	async fn task(
		database: Arc<fdb::Database>,
		subspace: fdbt::Subspace,
		mut receiver_high: RequestReceiver,
		mut receiver_medium: RequestReceiver,
		mut receiver_low: RequestReceiver,
		concurrency: usize,
		max_items_per_transaction: usize,
		partition_total: u64,
		metrics: Metrics,
	) {
		stream::unfold(
			(&mut receiver_high, &mut receiver_medium, &mut receiver_low),
			|(rh, rm, rl)| async move {
				// Drain high and medium priority channels.
				let mut requests_high = Self::drain_receiver(rh);
				let mut requests_medium = Self::drain_receiver(rm);

				// Only drain low-priority when high and medium are empty.
				let mut requests_low = if requests_high.is_empty() && requests_medium.is_empty() {
					Self::drain_receiver(rl)
				} else {
					Vec::new()
				};

				// If all channels are empty, block until a request arrives.
				if requests_high.is_empty() && requests_medium.is_empty() && requests_low.is_empty()
				{
					tokio::select! {
						result = rh.recv() => {
							if let Some(item) = result {
								requests_high.push(item);
							}
						},
						result = rm.recv() => {
							if let Some(item) = result {
								requests_medium.push(item);
							}
						},
						result = rl.recv() => {
							if let Some(item) = result {
								requests_low.push(item);
							}
						},
					}

					// If all channels are closed, stop.
					if requests_high.is_empty()
						&& requests_medium.is_empty()
						&& requests_low.is_empty()
					{
						return None;
					}

					// After waking, drain all channels again.
					requests_high.extend(Self::drain_receiver(rh));
					requests_medium.extend(Self::drain_receiver(rm));
					requests_low.extend(Self::drain_receiver(rl));
				}

				// Create batches with priority ordering: high first, then medium, then low.
				let mut batches = Vec::new();
				batches.extend(Self::create_batches(
					requests_high,
					max_items_per_transaction,
				));
				batches.extend(Self::create_batches(
					requests_medium,
					max_items_per_transaction,
				));
				batches.extend(Self::create_batches(
					requests_low,
					max_items_per_transaction,
				));

				Some((batches, (rh, rm, rl)))
			},
		)
		.flat_map(stream::iter)
		.for_each_concurrent(concurrency, |batch| {
			let database = database.clone();
			let subspace = subspace.clone();
			let metrics = metrics.clone();
			async move {
				Self::execute_batch(&database, &subspace, batch, partition_total, &metrics).await;
			}
		})
		.await;
	}

	fn drain_receiver(receiver: &mut RequestReceiver) -> Vec<(Request, ResponseSender)> {
		let mut requests = Vec::new();
		while let Ok(item) = receiver.try_recv() {
			requests.push(item);
		}
		requests
	}

	fn create_batches(requests: Vec<(Request, ResponseSender)>, max_items: usize) -> Vec<Batch> {
		if requests.is_empty() {
			return Vec::new();
		}

		let mut batches: Vec<Batch> = Vec::new();
		let mut current_batch = Batch {
			requests: Vec::new(),
			trackers: Vec::new(),
		};
		let mut current_count: usize = 0;

		for (request, sender) in requests {
			let tracker = Arc::new(Mutex::new(RequestTracker {
				remaining: 0,
				response: Ok(Self::create_initial_response(&request)),
				sender: Some(sender),
			}));

			let (items, kind) = Self::request_into_items(request);
			let mut iter = items.into_iter().peekable();
			let mut remaining_count = iter.len();

			while remaining_count > 0 {
				let space = max_items.saturating_sub(current_count);
				if space == 0 {
					if !current_batch.requests.is_empty() {
						batches.push(current_batch);
						current_batch = Batch {
							requests: Vec::new(),
							trackers: Vec::new(),
						};
						current_count = 0;
					}
					continue;
				}

				let take = remaining_count.min(space);
				let chunk: Vec<_> = iter.by_ref().take(take).collect();
				current_batch
					.requests
					.push(Self::request_from_items(chunk, &kind));
				current_batch.trackers.push(tracker.clone());
				tracker.lock().unwrap().remaining += 1;
				current_count += take;
				remaining_count -= take;

				if remaining_count > 0 {
					batches.push(current_batch);
					current_batch = Batch {
						requests: Vec::new(),
						trackers: Vec::new(),
					};
					current_count = 0;
				}
			}
		}

		if !current_batch.requests.is_empty() {
			batches.push(current_batch);
		}

		batches
	}

	fn create_initial_response(request: &Request) -> Response {
		match request {
			Request::Clean { .. } => Response::CleanOutput(CleanOutput::default()),
			Request::DeleteTags(_) | Request::Put(_) | Request::PutTags(_) => Response::Unit,
			Request::TouchCacheEntries { .. } => Response::CacheEntries(Vec::new()),
			Request::TouchObjects { .. } => Response::Objects(Vec::new()),
			Request::TouchProcesses { .. } => Response::Processes(Vec::new()),
			Request::Update { .. } => Response::UpdateCount(0),
		}
	}

	fn request_into_items(request: Request) -> (Vec<RequestItem>, RequestKind) {
		match request {
			Request::Clean {
				max_touched_at,
				batch_size,
				partition_start,
				partition_count,
			} => {
				let items = (0..batch_size).map(|_| RequestItem::Clean).collect();
				(
					items,
					RequestKind::Clean {
						max_touched_at,
						partition_start,
						partition_count,
					},
				)
			},
			Request::DeleteTags(tags) => {
				let items = tags.into_iter().map(RequestItem::DeleteTag).collect();
				(items, RequestKind::DeleteTags)
			},
			Request::Put(arg) => {
				let mut items = Vec::with_capacity(
					arg.cache_entries.len() + arg.objects.len() + arg.processes.len(),
				);
				items.extend(
					arg.cache_entries
						.into_iter()
						.map(RequestItem::PutCacheEntry),
				);
				items.extend(arg.objects.into_iter().map(RequestItem::PutObject));
				items.extend(arg.processes.into_iter().map(RequestItem::PutProcess));
				(items, RequestKind::Put)
			},
			Request::PutTags(tags) => {
				let items = tags.into_iter().map(RequestItem::PutTag).collect();
				(items, RequestKind::PutTags)
			},
			Request::TouchCacheEntries { ids, touched_at } => {
				let items = ids.into_iter().map(RequestItem::TouchCacheEntry).collect();
				(items, RequestKind::TouchCacheEntries { touched_at })
			},
			Request::TouchObjects { ids, touched_at } => {
				let items = ids.into_iter().map(RequestItem::TouchObject).collect();
				(items, RequestKind::TouchObjects { touched_at })
			},
			Request::TouchProcesses { ids, touched_at } => {
				let items = ids.into_iter().map(RequestItem::TouchProcess).collect();
				(items, RequestKind::TouchProcesses { touched_at })
			},
			Request::Update {
				batch_size,
				partition_start,
				partition_count,
			} => {
				let items = (0..batch_size).map(|_| RequestItem::Update).collect();
				(
					items,
					RequestKind::Update {
						partition_start,
						partition_count,
					},
				)
			},
		}
	}

	fn request_from_items(items: Vec<RequestItem>, kind: &RequestKind) -> Request {
		match kind {
			RequestKind::Clean {
				max_touched_at,
				partition_start,
				partition_count,
			} => Request::Clean {
				max_touched_at: *max_touched_at,
				batch_size: items.len(),
				partition_start: *partition_start,
				partition_count: *partition_count,
			},
			RequestKind::DeleteTags => {
				let tags = items
					.into_iter()
					.map(|item| match item {
						RequestItem::DeleteTag(tag) => tag,
						_ => unreachable!(),
					})
					.collect();
				Request::DeleteTags(tags)
			},
			RequestKind::Put => {
				let mut arg = PutArg::default();
				for item in items {
					match item {
						RequestItem::PutCacheEntry(entry) => arg.cache_entries.push(entry),
						RequestItem::PutObject(object) => arg.objects.push(object),
						RequestItem::PutProcess(process) => arg.processes.push(process),
						_ => unreachable!(),
					}
				}
				Request::Put(arg)
			},
			RequestKind::PutTags => {
				let tags = items
					.into_iter()
					.map(|item| match item {
						RequestItem::PutTag(tag) => tag,
						_ => unreachable!(),
					})
					.collect();
				Request::PutTags(tags)
			},
			RequestKind::TouchCacheEntries { touched_at } => {
				let ids = items
					.into_iter()
					.map(|item| match item {
						RequestItem::TouchCacheEntry(id) => id,
						_ => unreachable!(),
					})
					.collect();
				Request::TouchCacheEntries {
					ids,
					touched_at: *touched_at,
				}
			},
			RequestKind::TouchObjects { touched_at } => {
				let ids = items
					.into_iter()
					.map(|item| match item {
						RequestItem::TouchObject(id) => id,
						_ => unreachable!(),
					})
					.collect();
				Request::TouchObjects {
					ids,
					touched_at: *touched_at,
				}
			},
			RequestKind::TouchProcesses { touched_at } => {
				let ids = items
					.into_iter()
					.map(|item| match item {
						RequestItem::TouchProcess(id) => id,
						_ => unreachable!(),
					})
					.collect();
				Request::TouchProcesses {
					ids,
					touched_at: *touched_at,
				}
			},
			RequestKind::Update {
				partition_start,
				partition_count,
			} => Request::Update {
				batch_size: items.len(),
				partition_start: *partition_start,
				partition_count: *partition_count,
			},
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

	async fn execute_batch(
		database: &fdb::Database,
		subspace: &fdbt::Subspace,
		batch: Batch,
		partition_total: u64,
		metrics: &Metrics,
	) {
		let start = std::time::Instant::now();
		let retry_count = AtomicU64::new(0);

		let priority_batch = batch.requests.iter().all(|request| {
			matches!(
				request,
				Request::Clean { .. } | Request::Put(_) | Request::Update { .. }
			)
		});

		let result = database
			.run(|txn, _maybe_committed| {
				retry_count.fetch_add(1, Ordering::Relaxed);
				let requests = batch.requests.clone();
				let subspace = subspace.clone();
				async move {
					if priority_batch {
						txn.set_option(fdb::options::TransactionOption::PriorityBatch)
							.unwrap();
					}
					let mut responses = Vec::new();
					for request in requests {
						let response =
							Self::execute_request(&txn, &subspace, &request, partition_total)
								.await
								.map_err(|source| {
									fdb::FdbBindingError::CustomError(source.into())
								})?;
						responses.push(response);
					}
					Ok(responses)
				}
			})
			.await;

		let duration = start.elapsed().as_secs_f64();
		metrics.commit_duration.record(duration, &[]);
		metrics.transactions.add(1, &[]);

		let attempts = retry_count.load(Ordering::Relaxed);
		if attempts > 1 {
			metrics.transaction_conflict_retry.add(attempts - 1, &[]);
		}

		match result {
			Ok(responses) => {
				for (response, tracker) in std::iter::zip(responses, &batch.trackers) {
					Self::complete_tracker(tracker, Ok(response));
				}
			},
			Err(fdb::FdbBindingError::NonRetryableFdbError(ref fdb_error))
				if fdb_error.code() == 2101 =>
			{
				metrics.transaction_too_large.add(1, &[]);
				if batch.requests.len() <= 1 {
					let error = tg::error!("transaction too large");
					for tracker in &batch.trackers {
						Self::fail_tracker(tracker, &error);
					}
				} else {
					let mid = batch.requests.len() / 2;
					let mut requests = batch.requests;
					let mut trackers = batch.trackers;
					let right_requests = requests.split_off(mid);
					let right_trackers = trackers.split_off(mid);
					let left = Batch { requests, trackers };
					let right = Batch {
						requests: right_requests,
						trackers: right_trackers,
					};
					Box::pin(Self::execute_batch(
						database,
						subspace,
						left,
						partition_total,
						metrics,
					))
					.await;
					Box::pin(Self::execute_batch(
						database,
						subspace,
						right,
						partition_total,
						metrics,
					))
					.await;
				}
			},
			Err(error) => {
				let error = tg::error!(!error, "failed to execute batch");
				for tracker in &batch.trackers {
					Self::fail_tracker(tracker, &error);
				}
			},
		}
	}

	async fn execute_request(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		request: &Request,
		partition_total: u64,
	) -> tg::Result<Response> {
		match request {
			Request::Clean {
				max_touched_at,
				batch_size,
				partition_start,
				partition_count,
			} => Self::task_clean(
				txn,
				subspace,
				*max_touched_at,
				*batch_size,
				*partition_start,
				*partition_count,
				partition_total,
			)
			.await
			.map(Response::CleanOutput),
			Request::DeleteTags(tags) => {
				Self::task_delete_tags(txn, subspace, tags, partition_total)
					.await
					.map(|()| Response::Unit)
			},
			Request::Put(arg) => {
				Self::task_put(txn, subspace, arg, partition_total).await?;
				Ok(Response::Unit)
			},
			Request::PutTags(args) => Self::task_put_tags(txn, subspace, args, partition_total)
				.await
				.map(|()| Response::Unit),
			Request::TouchCacheEntries { ids, touched_at } => {
				Self::task_touch_cache_entries(txn, subspace, ids, *touched_at)
					.await
					.map(Response::CacheEntries)
			},
			Request::TouchObjects { ids, touched_at } => {
				Self::task_touch_objects(txn, subspace, ids, *touched_at)
					.await
					.map(Response::Objects)
			},
			Request::TouchProcesses { ids, touched_at } => {
				Self::task_touch_processes(txn, subspace, ids, *touched_at)
					.await
					.map(Response::Processes)
			},
			Request::Update {
				batch_size,
				partition_start,
				partition_count,
			} => Self::task_update(
				txn,
				subspace,
				*batch_size,
				*partition_start,
				*partition_count,
				partition_total,
			)
			.await
			.map(Response::UpdateCount),
		}
	}

	fn complete_tracker(tracker: &Arc<Mutex<RequestTracker>>, result: tg::Result<Response>) {
		let mut state = tracker.lock().unwrap();
		match result {
			Ok(response) => Self::merge_response(&mut state.response, response),
			Err(error) => {
				if state.response.is_ok() {
					state.response = Err(error);
				}
			},
		}
		state.remaining -= 1;
		if state.remaining == 0
			&& let Some(sender) = state.sender.take()
		{
			sender
				.send(std::mem::replace(&mut state.response, Ok(Response::Unit)))
				.ok();
		}
	}

	fn fail_tracker(tracker: &Arc<Mutex<RequestTracker>>, error: &tg::Error) {
		let mut state = tracker.lock().unwrap();
		if state.response.is_ok() {
			state.response = Err(error.clone());
		}
		state.remaining -= 1;
		if state.remaining == 0
			&& let Some(sender) = state.sender.take()
		{
			sender
				.send(std::mem::replace(&mut state.response, Ok(Response::Unit)))
				.ok();
		}
	}
}

impl crate::Index for Index {
	async fn try_get_objects(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<crate::Object>>> {
		self.try_get_objects(ids).await
	}

	async fn try_get_processes(
		&self,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<crate::Process>>> {
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
	) -> tg::Result<Vec<Option<crate::Object>>> {
		self.touch_objects(ids, touched_at).await
	}

	async fn touch_processes(
		&self,
		ids: &[tg::process::Id],
		touched_at: i64,
	) -> tg::Result<Vec<Option<crate::Process>>> {
		self.touch_processes(ids, touched_at).await
	}

	async fn put(&self, arg: crate::PutArg) -> tg::Result<()> {
		self.put(arg).await
	}

	async fn put_tags(&self, args: &[crate::PutTagArg]) -> tg::Result<()> {
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
		self.update_batch(batch_size, partition_start, partition_count)
			.await
	}

	async fn clean(
		&self,
		max_touched_at: i64,
		batch_size: usize,
		partition_start: u64,
		partition_count: u64,
	) -> tg::Result<CleanOutput> {
		self.clean(max_touched_at, batch_size, partition_start, partition_count)
			.await
	}

	fn partition_total(&self) -> u64 {
		self.partition_total
	}

	async fn get_transaction_id(&self) -> tg::Result<u64> {
		self.get_transaction_id().await
	}

	async fn sync(&self) -> tg::Result<()> {
		self.sync().await
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

			Key::ChildObject {
				child,
				object: parent,
			} => (
				KeyKind::ChildObject.to_i32().unwrap(),
				child.to_bytes().as_ref(),
				parent.to_bytes().as_ref(),
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
				partition,
				touched_at,
				kind,
				id,
			} => {
				KeyKind::Clean.to_i32().unwrap().pack(w, tuple_depth)?;
				partition.pack(w, tuple_depth)?;
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

			Key::UpdateVersion {
				partition,
				version,
				id,
			} => {
				let mut offset = KeyKind::UpdateVersion
					.to_i32()
					.unwrap()
					.pack(w, tuple_depth)?;
				offset += partition.pack(w, tuple_depth)?;
				offset += version.pack(w, tuple_depth)?;
				let id = match &id {
					tg::Either::Left(id) => id.to_bytes(),
					tg::Either::Right(id) => id.to_bytes(),
				};
				offset += id.as_ref().pack(w, tuple_depth)?;
				Ok(offset)
			},
		}
	}
}

impl fdbt::TupleUnpack<'_> for Key {
	fn unpack(input: &[u8], tuple_depth: fdbt::TupleDepth) -> fdbt::PackResult<(&[u8], Self)> {
		let (input, kind) = i32::unpack(input, tuple_depth)?;
		let kind =
			KeyKind::from_i32(kind).ok_or(fdbt::PackError::Message("invalid kind".into()))?;

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
				let (input, kind) = ProcessObjectKind::unpack(input, tuple_depth)?;
				let (input, object_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let process = tg::process::Id::from_slice(&process_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
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
				let (input, kind) = ProcessObjectKind::unpack(input, tuple_depth)?;
				let (input, process_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let object = tg::object::Id::from_slice(&object_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
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
				let (input, partition): (_, u64) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, touched_at): (_, i64) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, kind): (_, i32) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let kind = ItemKind::from_i32(kind)
					.ok_or(fdbt::PackError::Message("invalid item kind".into()))?;
				let (input, id_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let id = tg::Id::from_slice(&id_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid id".into()))?;
				let id = if let Ok(id) = tg::process::Id::try_from(id.clone()) {
					tg::Either::Right(id)
				} else if let Ok(id) = tg::object::Id::try_from(id) {
					tg::Either::Left(id)
				} else {
					return Err(fdbt::PackError::Message("invalid id".into()));
				};
				let key = Key::Clean {
					partition,
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
				let (input, partition): (_, u64) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, version) = fdbt::Versionstamp::unpack(input, tuple_depth)?;
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
				let key = Key::UpdateVersion {
					partition,
					version,
					id,
				};
				Ok((input, key))
			},
		}
	}
}

impl fdbt::TuplePack for KeyKind {
	fn pack<W: std::io::Write>(
		&self,
		w: &mut W,
		tuple_depth: fdbt::TupleDepth,
	) -> std::io::Result<fdbt::VersionstampOffset> {
		self.to_i32().unwrap().pack(w, tuple_depth)
	}
}

impl fdbt::TupleUnpack<'_> for KeyKind {
	fn unpack(input: &[u8], tuple_depth: fdbt::TupleDepth) -> fdbt::PackResult<(&[u8], Self)> {
		let (input, value) = i32::unpack(input, tuple_depth)?;
		let kind = Self::from_i32(value).ok_or(fdbt::PackError::Message("invalid kind".into()))?;
		Ok((input, kind))
	}
}

impl fdbt::TuplePack for ProcessObjectKind {
	fn pack<W: std::io::Write>(
		&self,
		w: &mut W,
		tuple_depth: fdbt::TupleDepth,
	) -> std::io::Result<fdbt::VersionstampOffset> {
		(*self as i32).pack(w, tuple_depth)
	}
}

impl fdbt::TupleUnpack<'_> for ProcessObjectKind {
	fn unpack(input: &[u8], tuple_depth: fdbt::TupleDepth) -> fdbt::PackResult<(&[u8], Self)> {
		let (input, value) = i32::unpack(input, tuple_depth)?;
		let kind = Self::from_i32(value).ok_or(fdbt::PackError::Message(
			"invalid process object kind".into(),
		))?;
		Ok((input, kind))
	}
}

impl Metrics {
	fn new() -> Self {
		let meter = otel::global::meter("tangram_index_fdb");

		let commit_duration = meter
			.f64_histogram("index.fdb.commit_duration")
			.with_description("FDB transaction commit duration in seconds.")
			.with_unit("s")
			.build();

		let transaction_conflict_retry = meter
			.u64_counter("index.fdb.transaction_conflict_retry")
			.with_description("Number of FDB transaction conflict retries.")
			.build();

		let transaction_too_large = meter
			.u64_counter("index.fdb.transaction_too_large")
			.with_description("Number of FDB transaction too large errors.")
			.build();

		let transactions = meter
			.u64_counter("index.fdb.transactions")
			.with_description("Total number of FDB transactions.")
			.build();

		Self {
			commit_duration,
			transaction_conflict_retry,
			transaction_too_large,
			transactions,
		}
	}
}
