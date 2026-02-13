use {
	super::{Index, Request, RequestReceiver, Response, ResponseSender},
	crate::{CleanOutput, PutArg, PutTagArg},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	futures::{StreamExt as _, stream},
	opentelemetry as otel,
	std::sync::{
		Arc, Mutex,
		atomic::{AtomicU64, Ordering},
	},
	tangram_client::prelude::*,
};

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

#[derive(Clone)]
pub struct Metrics {
	commit_duration: otel::metrics::Histogram<f64>,
	transaction_conflict_retry: otel::metrics::Counter<u64>,
	transaction_too_large: otel::metrics::Counter<u64>,
	transactions: otel::metrics::Counter<u64>,
}

impl Metrics {
	pub(super) fn new() -> Self {
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

impl Index {
	#[allow(clippy::too_many_arguments)]
	pub(super) async fn task(
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
