use {
	super::{Db, Index, Request, RequestReceiver, Response, ResponseSender},
	crate::{CleanOutput, PutArg, PutTagArg},
	crossbeam_channel as crossbeam, heed as lmdb,
	std::collections::VecDeque,
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
	Clean { max_touched_at: i64 },
	DeleteTags,
	Put,
	PutTags,
	TouchCacheEntries { touched_at: i64 },
	TouchObjects { touched_at: i64 },
	TouchProcesses { touched_at: i64 },
	Update,
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

impl Index {
	pub(super) fn task(
		env: &lmdb::Env,
		db: &Db,
		subspace: &foundationdb_tuple::Subspace,
		receiver_high: &RequestReceiver,
		receiver_medium: &RequestReceiver,
		receiver_low: &RequestReceiver,
		max_items_per_transaction: usize,
	) {
		let mut trackers: Vec<RequestTracker> = Vec::new();
		let mut queue_high: VecDeque<Batch> = VecDeque::new();
		let mut queue_medium: VecDeque<Batch> = VecDeque::new();
		let mut queue_low: VecDeque<Batch> = VecDeque::new();

		loop {
			// Drain high-priority requests.
			queue_high.extend(Self::create_batches(
				Self::drain_receiver(receiver_high),
				&mut trackers,
				max_items_per_transaction,
			));

			// Drain medium-priority requests.
			queue_medium.extend(Self::create_batches(
				Self::drain_receiver(receiver_medium),
				&mut trackers,
				max_items_per_transaction,
			));

			// If all queues are empty, try low-priority requests.
			if queue_high.is_empty() && queue_medium.is_empty() && queue_low.is_empty() {
				queue_low.extend(Self::create_batches(
					Self::drain_receiver(receiver_low),
					&mut trackers,
					max_items_per_transaction,
				));
			}

			// If still empty, block until a request arrives.
			if queue_high.is_empty() && queue_medium.is_empty() && queue_low.is_empty() {
				crossbeam::select! {
					recv(receiver_high) -> result => {
						if let Ok(item) = result {
							queue_high.extend(Self::create_batches(
								vec![item],
								&mut trackers,
								max_items_per_transaction,
							));
						}
					},
					recv(receiver_medium) -> result => {
						if let Ok(item) = result {
							queue_medium.extend(Self::create_batches(
								vec![item],
								&mut trackers,
								max_items_per_transaction,
							));
						}
					},
					recv(receiver_low) -> result => {
						if let Ok(item) = result {
							queue_low.extend(Self::create_batches(
								vec![item],
								&mut trackers,
								max_items_per_transaction,
							));
						}
					},
				}

				// After waking, drain all receivers.
				queue_high.extend(Self::create_batches(
					Self::drain_receiver(receiver_high),
					&mut trackers,
					max_items_per_transaction,
				));
				queue_medium.extend(Self::create_batches(
					Self::drain_receiver(receiver_medium),
					&mut trackers,
					max_items_per_transaction,
				));
				queue_low.extend(Self::create_batches(
					Self::drain_receiver(receiver_low),
					&mut trackers,
					max_items_per_transaction,
				));
			}

			// Pop a batch from high-priority first, then medium, then low-priority.
			let batch = if let Some(batch) = queue_high.pop_front() {
				batch
			} else if let Some(batch) = queue_medium.pop_front() {
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
						Self::fail_tracker(&mut trackers[*tracker_index], &error);
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
				let result = match commit_result {
					Err(ref error) => Err(error.clone()),
					Ok(()) => result,
				};
				Self::complete_tracker(&mut trackers[*tracker_index], result);
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

	fn drain_receiver(receiver: &RequestReceiver) -> Vec<(Request, ResponseSender)> {
		let mut requests = Vec::new();
		while let Ok(item) = receiver.try_recv() {
			requests.push(item);
		}
		requests
	}

	fn create_batches(
		requests: Vec<(Request, ResponseSender)>,
		trackers: &mut Vec<RequestTracker>,
		max_items: usize,
	) -> Vec<Batch> {
		if requests.is_empty() {
			return Vec::new();
		}

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
							tracker_indices: Vec::new(),
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
				current_batch.tracker_indices.push(tracker_idx);
				trackers[tracker_idx].remaining += 1;
				current_count += take;
				remaining_count -= take;

				if remaining_count > 0 {
					batches.push(current_batch);
					current_batch = Batch {
						requests: Vec::new(),
						tracker_indices: Vec::new(),
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
			} => {
				let items = (0..batch_size).map(|_| RequestItem::Clean).collect();
				(items, RequestKind::Clean { max_touched_at })
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
			Request::Update { batch_size } => {
				let items = (0..batch_size).map(|_| RequestItem::Update).collect();
				(items, RequestKind::Update)
			},
		}
	}

	fn request_from_items(items: Vec<RequestItem>, kind: &RequestKind) -> Request {
		match kind {
			RequestKind::Clean { max_touched_at } => Request::Clean {
				max_touched_at: *max_touched_at,
				batch_size: items.len(),
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
			RequestKind::Update => Request::Update {
				batch_size: items.len(),
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

	fn complete_tracker(tracker: &mut RequestTracker, result: tg::Result<Response>) {
		match result {
			Ok(response) => Self::merge_response(&mut tracker.response, response),
			Err(error) => {
				if tracker.response.is_ok() {
					tracker.response = Err(error);
				}
			},
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

	fn fail_tracker(tracker: &mut RequestTracker, error: &tg::Error) {
		if tracker.response.is_ok() {
			tracker.response = Err(error.clone());
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
}
