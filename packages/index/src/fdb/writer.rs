use {
	super::{
		Index, Request, Response,
		request::{Item, Kind, Priority},
	},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	futures::{StreamExt as _, stream},
	opentelemetry as otel,
	std::sync::{
		Arc, Mutex,
		atomic::{AtomicU64, Ordering},
	},
	tangram_client::prelude::*,
};

pub(super) type RequestReceiver = tokio::sync::mpsc::UnboundedReceiver<(Request, ResponseSender)>;
pub(super) type RequestSender = tokio::sync::mpsc::UnboundedSender<(Request, ResponseSender)>;
pub(super) type ResponseSender = tokio::sync::oneshot::Sender<tg::Result<Response>>;

#[derive(Clone)]
pub struct Metrics {
	commit_duration: otel::metrics::Histogram<f64>,
	transaction_conflict_retry: otel::metrics::Counter<u64>,
	transaction_too_large: otel::metrics::Counter<u64>,
	transactions: otel::metrics::Counter<u64>,
}

pub(super) struct Arg {
	pub database: Arc<fdb::Database>,
	pub max_process_depth: Option<u64>,
	pub metrics: Metrics,
	pub partition_total: u64,
	pub receiver_high: RequestReceiver,
	pub receiver_low: RequestReceiver,
	pub receiver_medium: RequestReceiver,
	pub subspace: fdbt::Subspace,
	pub usage_partition_total: u64,
	pub write_batch_size: usize,
	pub write_concurrency: usize,
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

impl Index {
	pub(super) async fn writer_task(arg: Arg) {
		let Arg {
			database,
			max_process_depth,
			metrics,
			partition_total,
			mut receiver_high,
			mut receiver_low,
			mut receiver_medium,
			subspace,
			usage_partition_total,
			write_batch_size,
			write_concurrency,
		} = arg;
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
				batches.extend(Self::create_batches(requests_high, write_batch_size));
				batches.extend(Self::create_batches(requests_medium, write_batch_size));
				batches.extend(Self::create_batches(requests_low, write_batch_size));

				Some((batches, (rh, rm, rl)))
			},
		)
		.flat_map(stream::iter)
		.for_each_concurrent(write_concurrency, |batch| {
			let database = database.clone();
			let subspace = subspace.clone();
			let metrics = metrics.clone();
			async move {
				Self::execute_batch(
					&database,
					&subspace,
					batch,
					max_process_depth,
					partition_total,
					usage_partition_total,
					&metrics,
				)
				.await;
			}
		})
		.await;
	}

	pub(super) async fn send_write_request(&self, request: Request) -> tg::Result<Response> {
		let sender = match request.priority() {
			Priority::High => &self.writer_sender_high,
			Priority::Low => &self.writer_sender_low,
			Priority::Medium => &self.writer_sender_medium,
		};
		let (response_sender, response_receiver) = tokio::sync::oneshot::channel();
		sender
			.send((request, response_sender))
			.map_err(|error| tg::error!(!error, "failed to send the write request"))?;
		let response = response_receiver
			.await
			.map_err(|error| tg::error!(!error, "failed to receive the write response"))??;

		Ok(response)
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
			if let Request::Batch(arg) = request {
				let count = arg.items.len();
				if !current_batch.requests.is_empty()
					&& current_count.saturating_add(count) > max_items
				{
					batches.push(current_batch);
					current_batch = Batch {
						requests: Vec::new(),
						trackers: Vec::new(),
					};
					current_count = 0;
				}
				current_batch.requests.push(Request::Batch(arg));
				current_batch.trackers.push(tracker.clone());
				tracker.lock().unwrap().remaining = 1;
				current_count = current_count.saturating_add(count);
				if current_count >= max_items {
					batches.push(current_batch);
					current_batch = Batch {
						requests: Vec::new(),
						trackers: Vec::new(),
					};
					current_count = 0;
				}
				continue;
			}

			let (items, kind) = Self::request_into_operations(request);
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
					.push(Self::request_from_operations(chunk, &kind));
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
			Request::Clean(_) => Response::CleanOutput(crate::clean::Output::default()),
			Request::CleanUsage(_) => {
				Response::CleanUsageOutput(crate::usage::clean::Output::default())
			},
			Request::CompactUsage(_) => {
				Response::CompactUsageOutput(crate::usage::compact::Output::default())
			},
			Request::Batch(_)
			| Request::CompleteLogCompaction(_)
			| Request::DeleteGrants(_)
			| Request::DeleteGroupMembers(_)
			| Request::DeleteGroups(_)
			| Request::DeleteOrganizationMembers(_)
			| Request::DeleteOrganizations(_)
			| Request::DeleteSandboxes(_)
			| Request::DeleteTags(_)
			| Request::DeleteUsers(_)
			| Request::EnqueueLogCompaction(_)
			| Request::PutCacheEntries(_)
			| Request::PutGrants(_)
			| Request::PutGroupMembers(_)
			| Request::PutGroups(_)
			| Request::PutObjects(_)
			| Request::PutOrganizationMembers(_)
			| Request::PutOrganizations(_)
			| Request::PutProcesses(_)
			| Request::PutSandboxes(_)
			| Request::PutTags(_)
			| Request::PutUsers(_) => Response::Unit,
			Request::GetUsage { .. } => Response::Usage(crate::usage::Aggregate::default()),
			Request::TouchCacheEntries(_) => Response::CacheEntries(Vec::new()),
			Request::TouchObjects(_) => Response::Objects(Vec::new()),
			Request::TouchProcesses(_) => Response::Processes(Vec::new()),
			Request::Update(_) => Response::UpdateOutput(crate::update::Output::default()),
		}
	}

	fn request_into_operations(request: Request) -> (Vec<Item>, Kind) {
		match request {
			Request::Batch(_) => unreachable!(),
			Request::Clean(crate::fdb::Clean {
				batch_size,
				max_object_touched_at,
				max_process_touched_at,
				max_sandbox_touched_at,
				now,
				partition_end,
				partition_start,
			}) => {
				let items = (0..batch_size).map(|_| Item::Clean).collect();
				(
					items,
					Kind::Clean {
						max_object_touched_at,
						max_process_touched_at,
						max_sandbox_touched_at,
						now,
						partition_end,
						partition_start,
					},
				)
			},
			Request::CleanUsage(arg) => (vec![Item::CleanUsage], Kind::CleanUsage(arg)),
			Request::CompactUsage(arg) => (vec![Item::CompactUsage], Kind::CompactUsage(arg)),
			Request::CompleteLogCompaction(entry) => (
				vec![Item::CompleteLogCompaction(entry)],
				Kind::CompleteLogCompaction,
			),
			Request::DeleteGrants(args) => {
				let items = args.into_iter().map(Item::DeleteGrant).collect();
				(items, Kind::DeleteGrants)
			},
			Request::DeleteGroupMembers(args) => {
				let items = args.into_iter().map(Item::DeleteGroupMember).collect();
				(items, Kind::DeleteGroupMembers)
			},
			Request::DeleteGroups(ids) => {
				let items = ids.into_iter().map(Item::DeleteGroup).collect();
				(items, Kind::DeleteGroups)
			},
			Request::DeleteOrganizationMembers(args) => {
				let items = args
					.into_iter()
					.map(Item::DeleteOrganizationMember)
					.collect();
				(items, Kind::DeleteOrganizationMembers)
			},
			Request::DeleteOrganizations(ids) => {
				let items = ids.into_iter().map(Item::DeleteOrganization).collect();
				(items, Kind::DeleteOrganizations)
			},
			Request::DeleteSandboxes(ids) => {
				let items = ids.into_iter().map(Item::DeleteSandbox).collect();
				(items, Kind::DeleteSandboxes)
			},
			Request::DeleteTags(tags) => {
				let items = tags.into_iter().map(Item::DeleteTag).collect();
				(items, Kind::DeleteTags)
			},
			Request::DeleteUsers(ids) => {
				let items = ids.into_iter().map(Item::DeleteUser).collect();
				(items, Kind::DeleteUsers)
			},
			Request::EnqueueLogCompaction(process) => (
				vec![Item::EnqueueLogCompaction(process)],
				Kind::EnqueueLogCompaction,
			),
			Request::GetUsage {
				account,
				now,
				period,
			} => (
				vec![Item::GetUsage],
				Kind::GetUsage {
					account,
					now,
					period,
				},
			),
			Request::PutCacheEntries(args) => {
				let items = args.into_iter().map(Item::PutCacheEntry).collect();
				(items, Kind::PutCacheEntries)
			},
			Request::PutGrants(args) => {
				let items = args.into_iter().map(Item::PutGrant).collect();
				(items, Kind::PutGrants)
			},
			Request::PutGroupMembers(args) => {
				let items = args.into_iter().map(Item::PutGroupMember).collect();
				(items, Kind::PutGroupMembers)
			},
			Request::PutGroups(args) => {
				let items = args.into_iter().map(Item::PutGroup).collect();
				(items, Kind::PutGroups)
			},
			Request::PutObjects(args) => {
				let items = args.into_iter().map(Item::PutObject).collect();
				(items, Kind::PutObjects)
			},
			Request::PutOrganizationMembers(args) => {
				let items = args.into_iter().map(Item::PutOrganizationMember).collect();
				(items, Kind::PutOrganizationMembers)
			},
			Request::PutOrganizations(args) => {
				let items = args.into_iter().map(Item::PutOrganization).collect();
				(items, Kind::PutOrganizations)
			},
			Request::PutProcesses(args) => {
				let items = args.into_iter().map(Item::PutProcess).collect();
				(items, Kind::PutProcesses)
			},
			Request::PutSandboxes(args) => {
				let items = args.into_iter().map(Item::PutSandbox).collect();
				(items, Kind::PutSandboxes)
			},
			Request::PutTags(tags) => {
				let items = tags.into_iter().map(Item::PutTag).collect();
				(items, Kind::PutTags)
			},
			Request::PutUsers(args) => {
				let items = args.into_iter().map(Item::PutUser).collect();
				(items, Kind::PutUsers)
			},
			Request::TouchCacheEntries(crate::fdb::TouchCacheEntries {
				ids,
				time_to_touch,
				touched_at,
			}) => {
				let items = ids.into_iter().map(Item::TouchCacheEntry).collect();
				(
					items,
					Kind::TouchCacheEntries {
						time_to_touch,
						touched_at,
					},
				)
			},
			Request::TouchObjects(crate::fdb::TouchObjects {
				account,
				ids,
				time_to_touch,
				touched_at,
			}) => {
				let items = ids.into_iter().map(Item::TouchObject).collect();
				(
					items,
					Kind::TouchObjects {
						account,
						time_to_touch,
						touched_at,
					},
				)
			},
			Request::TouchProcesses(crate::fdb::TouchProcesses {
				account,
				ids,
				put_account,
				time_to_touch,
				touched_at,
			}) => {
				let items = ids.into_iter().map(Item::TouchProcess).collect();
				(
					items,
					Kind::TouchProcesses {
						account,
						put_account,
						time_to_touch,
						touched_at,
					},
				)
			},
			Request::Update(crate::fdb::Update {
				batch_size,
				kind,
				partition_start,
				partition_end,
			}) => {
				let items = (0..batch_size).map(|_| Item::Update).collect();
				(
					items,
					Kind::Update {
						kind,
						partition_start,
						partition_end,
					},
				)
			},
		}
	}

	fn request_from_operations(items: Vec<Item>, kind: &Kind) -> Request {
		match kind {
			Kind::Clean {
				max_object_touched_at,
				max_process_touched_at,
				max_sandbox_touched_at,
				now,
				partition_end,
				partition_start,
			} => Request::Clean(crate::fdb::Clean {
				batch_size: items.len(),
				max_object_touched_at: *max_object_touched_at,
				max_process_touched_at: *max_process_touched_at,
				max_sandbox_touched_at: *max_sandbox_touched_at,
				now: *now,
				partition_end: *partition_end,
				partition_start: *partition_start,
			}),
			Kind::CleanUsage(arg) => {
				let items: [Item; 1] = items.try_into().ok().unwrap();
				let [Item::CleanUsage] = items else {
					unreachable!();
				};
				Request::CleanUsage(arg.clone())
			},
			Kind::CompactUsage(arg) => {
				let items: [Item; 1] = items.try_into().ok().unwrap();
				let [Item::CompactUsage] = items else {
					unreachable!();
				};
				Request::CompactUsage(arg.clone())
			},
			Kind::CompleteLogCompaction => {
				let items: [Item; 1] = items.try_into().ok().unwrap();
				let [Item::CompleteLogCompaction(entry)] = items else {
					unreachable!();
				};
				Request::CompleteLogCompaction(entry)
			},
			Kind::DeleteGrants => {
				let args = items
					.into_iter()
					.map(|item| match item {
						Item::DeleteGrant(arg) => arg,
						_ => unreachable!(),
					})
					.collect();
				Request::DeleteGrants(args)
			},
			Kind::DeleteGroupMembers => {
				let args = items
					.into_iter()
					.map(|item| match item {
						Item::DeleteGroupMember(arg) => arg,
						_ => unreachable!(),
					})
					.collect();
				Request::DeleteGroupMembers(args)
			},
			Kind::DeleteGroups => {
				let ids = items
					.into_iter()
					.map(|item| match item {
						Item::DeleteGroup(id) => id,
						_ => unreachable!(),
					})
					.collect();
				Request::DeleteGroups(ids)
			},
			Kind::DeleteOrganizationMembers => {
				let args = items
					.into_iter()
					.map(|item| match item {
						Item::DeleteOrganizationMember(arg) => arg,
						_ => unreachable!(),
					})
					.collect();
				Request::DeleteOrganizationMembers(args)
			},
			Kind::DeleteOrganizations => {
				let ids = items
					.into_iter()
					.map(|item| match item {
						Item::DeleteOrganization(id) => id,
						_ => unreachable!(),
					})
					.collect();
				Request::DeleteOrganizations(ids)
			},
			Kind::DeleteSandboxes => {
				let ids = items
					.into_iter()
					.map(|item| match item {
						Item::DeleteSandbox(id) => id,
						_ => unreachable!(),
					})
					.collect();
				Request::DeleteSandboxes(ids)
			},
			Kind::DeleteTags => {
				let tags = items
					.into_iter()
					.map(|item| match item {
						Item::DeleteTag(tag) => tag,
						_ => unreachable!(),
					})
					.collect();
				Request::DeleteTags(tags)
			},
			Kind::DeleteUsers => {
				let ids = items
					.into_iter()
					.map(|item| match item {
						Item::DeleteUser(id) => id,
						_ => unreachable!(),
					})
					.collect();
				Request::DeleteUsers(ids)
			},
			Kind::EnqueueLogCompaction => {
				let items: [Item; 1] = items.try_into().ok().unwrap();
				let [Item::EnqueueLogCompaction(process)] = items else {
					unreachable!();
				};
				Request::EnqueueLogCompaction(process)
			},
			Kind::GetUsage {
				account,
				now,
				period,
			} => {
				let items: [Item; 1] = items.try_into().ok().unwrap();
				let [Item::GetUsage] = items else {
					unreachable!();
				};
				Request::GetUsage {
					account: account.clone(),
					now: *now,
					period: *period,
				}
			},
			Kind::PutCacheEntries => {
				let args = items
					.into_iter()
					.map(|item| match item {
						Item::PutCacheEntry(arg) => arg,
						_ => unreachable!(),
					})
					.collect();
				Request::PutCacheEntries(args)
			},
			Kind::PutGrants => {
				let args = items
					.into_iter()
					.map(|item| match item {
						Item::PutGrant(arg) => arg,
						_ => unreachable!(),
					})
					.collect();
				Request::PutGrants(args)
			},
			Kind::PutGroupMembers => {
				let args = items
					.into_iter()
					.map(|item| match item {
						Item::PutGroupMember(arg) => arg,
						_ => unreachable!(),
					})
					.collect();
				Request::PutGroupMembers(args)
			},
			Kind::PutGroups => {
				let args = items
					.into_iter()
					.map(|item| match item {
						Item::PutGroup(arg) => arg,
						_ => unreachable!(),
					})
					.collect();
				Request::PutGroups(args)
			},
			Kind::PutObjects => {
				let args = items
					.into_iter()
					.map(|item| match item {
						Item::PutObject(arg) => arg,
						_ => unreachable!(),
					})
					.collect();
				Request::PutObjects(args)
			},
			Kind::PutOrganizationMembers => {
				let args = items
					.into_iter()
					.map(|item| match item {
						Item::PutOrganizationMember(arg) => arg,
						_ => unreachable!(),
					})
					.collect();
				Request::PutOrganizationMembers(args)
			},
			Kind::PutOrganizations => {
				let args = items
					.into_iter()
					.map(|item| match item {
						Item::PutOrganization(arg) => arg,
						_ => unreachable!(),
					})
					.collect();
				Request::PutOrganizations(args)
			},
			Kind::PutProcesses => {
				let args = items
					.into_iter()
					.map(|item| match item {
						Item::PutProcess(arg) => arg,
						_ => unreachable!(),
					})
					.collect();
				Request::PutProcesses(args)
			},
			Kind::PutSandboxes => {
				let args = items
					.into_iter()
					.map(|item| match item {
						Item::PutSandbox(arg) => arg,
						_ => unreachable!(),
					})
					.collect();
				Request::PutSandboxes(args)
			},
			Kind::PutTags => {
				let tags = items
					.into_iter()
					.map(|item| match item {
						Item::PutTag(tag) => tag,
						_ => unreachable!(),
					})
					.collect();
				Request::PutTags(tags)
			},
			Kind::PutUsers => {
				let args = items
					.into_iter()
					.map(|item| match item {
						Item::PutUser(arg) => arg,
						_ => unreachable!(),
					})
					.collect();
				Request::PutUsers(args)
			},
			Kind::TouchCacheEntries {
				time_to_touch,
				touched_at,
			} => {
				let ids = items
					.into_iter()
					.map(|item| match item {
						Item::TouchCacheEntry(id) => id,
						_ => unreachable!(),
					})
					.collect();
				Request::TouchCacheEntries(crate::fdb::TouchCacheEntries {
					ids,
					time_to_touch: *time_to_touch,
					touched_at: *touched_at,
				})
			},
			Kind::TouchObjects {
				account,
				time_to_touch,
				touched_at,
			} => {
				let ids = items
					.into_iter()
					.map(|item| match item {
						Item::TouchObject(id) => id,
						_ => unreachable!(),
					})
					.collect();
				Request::TouchObjects(crate::fdb::TouchObjects {
					account: account.clone(),
					ids,
					time_to_touch: *time_to_touch,
					touched_at: *touched_at,
				})
			},
			Kind::TouchProcesses {
				account,
				put_account,
				time_to_touch,
				touched_at,
			} => {
				let ids = items
					.into_iter()
					.map(|item| match item {
						Item::TouchProcess(id) => id,
						_ => unreachable!(),
					})
					.collect();
				Request::TouchProcesses(crate::fdb::TouchProcesses {
					account: account.clone(),
					ids,
					put_account: *put_account,
					time_to_touch: *time_to_touch,
					touched_at: *touched_at,
				})
			},
			Kind::Update {
				kind,
				partition_start,
				partition_end,
			} => Request::Update(crate::fdb::Update {
				batch_size: items.len(),
				kind: *kind,
				partition_start: *partition_start,
				partition_end: *partition_end,
			}),
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
			(Response::CleanUsageOutput(existing), Response::CleanUsageOutput(new)) => {
				*existing = new;
			},
			(Response::CompactUsageOutput(existing), Response::CompactUsageOutput(new)) => {
				existing.count += new.count;
			},
			(Response::UpdateOutput(existing), Response::UpdateOutput(new)) => {
				existing.merge(new);
			},
			(Response::Usage(existing), Response::Usage(new)) => {
				*existing = new;
			},
			_ => {},
		}
	}

	async fn execute_batch(
		database: &fdb::Database,
		subspace: &fdbt::Subspace,
		batch: Batch,
		max_process_depth: Option<u64>,
		partition_total: u64,
		usage_partition_total: u64,
		metrics: &Metrics,
	) {
		let result = Self::execute_transaction(
			database,
			subspace,
			&batch.requests,
			max_process_depth,
			partition_total,
			usage_partition_total,
			metrics,
		)
		.await;

		match result {
			Ok(responses) => {
				for (response, tracker) in std::iter::zip(responses, &batch.trackers) {
					Self::complete_tracker(tracker, Ok(response));
				}
			},
			Err(error) if Self::is_transaction_too_large(&error) => {
				if batch.requests.len() > 1 {
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
						max_process_depth,
						partition_total,
						usage_partition_total,
						metrics,
					))
					.await;
					Box::pin(Self::execute_batch(
						database,
						subspace,
						right,
						max_process_depth,
						partition_total,
						usage_partition_total,
						metrics,
					))
					.await;
					return;
				}

				let request = batch.requests.into_iter().next().unwrap();
				let tracker = batch.trackers.into_iter().next().unwrap();
				let result = match request {
					Request::Batch(arg) if arg.items.len() > 1 => {
						Self::execute_ordered_batch(
							database,
							subspace,
							arg,
							max_process_depth,
							partition_total,
							usage_partition_total,
							metrics,
						)
						.await
					},
					_ => Err(tg::error!(!error, "transaction too large")),
				};
				match result {
					Ok(()) => Self::complete_tracker(&tracker, Ok(Response::Unit)),
					Err(error) => Self::fail_tracker(&tracker, &error),
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

	async fn execute_ordered_batch(
		database: &fdb::Database,
		subspace: &fdbt::Subspace,
		arg: crate::batch::Arg,
		max_process_depth: Option<u64>,
		partition_total: u64,
		usage_partition_total: u64,
		metrics: &Metrics,
	) -> tg::Result<()> {
		let Some((left, right)) = Self::try_split_batch_arg(arg) else {
			return Err(tg::error!(
				"cannot split an index batch with fewer than two items"
			));
		};
		// Push the right half first so every left half commits before its right half.
		let mut pending = vec![right, left];
		while let Some(arg) = pending.pop() {
			let request = Request::Batch(arg);
			let result = Self::execute_transaction(
				database,
				subspace,
				std::slice::from_ref(&request),
				max_process_depth,
				partition_total,
				usage_partition_total,
				metrics,
			)
			.await;
			match result {
				Ok(responses) => {
					let [Response::Unit] = responses.as_slice() else {
						return Err(tg::error!("unexpected write response"));
					};
				},
				Err(error) if Self::is_transaction_too_large(&error) => {
					let Request::Batch(arg) = request else {
						unreachable!();
					};
					let Some((left, right)) = Self::try_split_batch_arg(arg) else {
						return Err(tg::error!(!error, "transaction too large"));
					};
					// Preserve the order when another adaptive split is required.
					pending.push(right);
					pending.push(left);
				},
				Err(error) => return Err(tg::error!(!error, "failed to execute batch")),
			}
		}

		Ok(())
	}

	async fn execute_transaction(
		database: &fdb::Database,
		subspace: &fdbt::Subspace,
		requests: &[Request],
		max_process_depth: Option<u64>,
		partition_total: u64,
		usage_partition_total: u64,
		metrics: &Metrics,
	) -> crate::fdb::Result<Vec<Response>> {
		let start = std::time::Instant::now();
		let retry_count = AtomicU64::new(0);

		let priority_batch = requests.iter().all(|request| {
			matches!(
				request,
				Request::Batch(_)
					| Request::Clean(_)
					| Request::CleanUsage(_)
					| Request::CompactUsage(_)
					| Request::CompleteLogCompaction(_)
					| Request::EnqueueLogCompaction(_)
					| Request::GetUsage { .. }
					| Request::PutCacheEntries(_)
					| Request::PutGrants(_)
					| Request::PutGroupMembers(_)
					| Request::PutGroups(_)
					| Request::PutObjects(_)
					| Request::PutOrganizationMembers(_)
					| Request::PutOrganizations(_)
					| Request::PutProcesses(_)
					| Request::PutSandboxes(_)
					| Request::PutTags(_)
					| Request::PutUsers(_)
					| Request::Update(_)
			)
		});

		let result = database
			.run(|txn, _maybe_committed| {
				retry_count.fetch_add(1, Ordering::Relaxed);
				let requests = requests.to_vec();
				let subspace = subspace.clone();
				async move {
					if priority_batch {
						txn.set_option(fdb::options::TransactionOption::PriorityBatch)
							.unwrap();
					}
					let mut responses = Vec::new();
					for request in requests {
						let response = Self::execute_request(
							&txn,
							&subspace,
							&request,
							max_process_depth,
							partition_total,
							usage_partition_total,
						)
						.await?;
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
		if result
			.as_ref()
			.err()
			.is_some_and(Self::is_transaction_too_large)
		{
			metrics.transaction_too_large.add(1, &[]);
		}

		result
	}

	async fn execute_request(
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		request: &Request,
		max_process_depth: Option<u64>,
		partition_total: u64,
		usage_partition_total: u64,
	) -> crate::fdb::Result<Response> {
		match request {
			Request::Batch(arg) => {
				Self::batch_with_transaction(
					txn,
					subspace,
					arg,
					partition_total,
					usage_partition_total,
				)
				.await?;
				Ok(Response::Unit)
			},
			Request::Clean(crate::fdb::Clean {
				batch_size,
				max_object_touched_at,
				max_process_touched_at,
				max_sandbox_touched_at,
				now,
				partition_end,
				partition_start,
			}) => {
				let arg = super::clean::TransactionArg {
					batch_size: *batch_size,
					max_object_touched_at: *max_object_touched_at,
					max_process_touched_at: *max_process_touched_at,
					max_sandbox_touched_at: *max_sandbox_touched_at,
					now: *now,
					partition_end: *partition_end,
					partition_start: *partition_start,
					partition_total,
					subspace,
					usage_partition_total,
					txn,
				};
				Self::clean_with_transaction(arg)
					.await
					.map(Response::CleanOutput)
			},
			Request::CleanUsage(arg) => {
				Self::clean_usage_with_transaction(txn, subspace, arg, usage_partition_total)
					.await
					.map(Response::CleanUsageOutput)
			},
			Request::CompactUsage(arg) => Self::compact_usage_with_transaction(txn, subspace, arg)
				.await
				.map(Response::CompactUsageOutput),
			Request::CompleteLogCompaction(entry) => {
				Self::complete_log_compaction_with_transaction(txn, subspace, entry).await?;
				Ok(Response::Unit)
			},
			Request::DeleteGrants(args) => {
				Self::delete_grants_with_transaction(txn, subspace, args, partition_total).await?;
				Ok(Response::Unit)
			},
			Request::DeleteGroupMembers(args) => {
				Self::delete_group_members_with_transaction(txn, subspace, args)?;
				Ok(Response::Unit)
			},
			Request::DeleteGroups(ids) => {
				Self::delete_groups_with_transaction(txn, subspace, ids).await?;
				Ok(Response::Unit)
			},
			Request::DeleteOrganizationMembers(args) => {
				Self::delete_organization_members_with_transaction(txn, subspace, args)?;
				Ok(Response::Unit)
			},
			Request::DeleteOrganizations(ids) => {
				Self::delete_organizations_with_transaction(txn, subspace, ids).await?;
				Ok(Response::Unit)
			},
			Request::DeleteSandboxes(ids) => {
				Self::delete_sandboxes_with_transaction(txn, subspace, ids)?;
				Ok(Response::Unit)
			},
			Request::DeleteUsers(ids) => {
				Self::delete_users_with_transaction(txn, subspace, ids).await?;
				Ok(Response::Unit)
			},
			Request::DeleteTags(tags) => {
				Self::delete_tags_with_transaction(txn, subspace, tags, partition_total)
					.await
					.map(|()| Response::Unit)
			},
			Request::EnqueueLogCompaction(process) => {
				Self::enqueue_log_compaction_with_transaction(
					txn,
					subspace,
					process,
					partition_total,
				)
				.await?;
				Ok(Response::Unit)
			},
			Request::GetUsage {
				account,
				now,
				period,
			} => Self::get_usage_with_transaction(
				txn,
				subspace,
				account,
				*period,
				*now,
				usage_partition_total,
			)
			.await
			.map(Response::Usage),
			Request::PutCacheEntries(args) => {
				Self::put_cache_entries_with_transaction(txn, subspace, args, partition_total)?;
				Ok(Response::Unit)
			},
			Request::PutGrants(args) => {
				Self::put_grants_with_transaction(txn, subspace, args, partition_total).await?;
				Ok(Response::Unit)
			},
			Request::PutGroupMembers(args) => {
				Self::put_group_members_with_transaction(txn, subspace, args)?;
				Ok(Response::Unit)
			},
			Request::PutGroups(args) => {
				Self::put_groups_with_transaction(txn, subspace, args)?;
				Ok(Response::Unit)
			},
			Request::PutObjects(args) => {
				Self::put_objects_with_transaction(txn, subspace, args, partition_total).await?;
				Ok(Response::Unit)
			},
			Request::PutOrganizationMembers(args) => {
				Self::put_organization_members_with_transaction(txn, subspace, args)?;
				Ok(Response::Unit)
			},
			Request::PutOrganizations(args) => {
				Self::put_organizations_with_transaction(txn, subspace, args).await?;
				Ok(Response::Unit)
			},
			Request::PutProcesses(args) => {
				Self::put_processes_with_transaction(txn, subspace, args, partition_total).await?;
				Ok(Response::Unit)
			},
			Request::PutSandboxes(args) => {
				Self::put_sandboxes_with_transaction(
					txn,
					subspace,
					args,
					partition_total,
					usage_partition_total,
				)
				.await?;
				Ok(Response::Unit)
			},
			Request::PutTags(args) => {
				Self::put_tags_with_transaction(txn, subspace, args, partition_total)
					.await
					.map(|()| Response::Unit)
			},
			Request::PutUsers(args) => {
				Self::put_users_with_transaction(txn, subspace, args).await?;
				Ok(Response::Unit)
			},
			Request::TouchCacheEntries(crate::fdb::TouchCacheEntries {
				ids,
				time_to_touch,
				touched_at,
			}) => Self::touch_cache_entries_with_transaction(
				txn,
				subspace,
				ids,
				*touched_at,
				*time_to_touch,
				partition_total,
			)
			.await
			.map(Response::CacheEntries),
			Request::TouchObjects(crate::fdb::TouchObjects {
				account,
				ids,
				time_to_touch,
				touched_at,
			}) => Self::touch_objects_with_account_with_transaction(
				txn,
				subspace,
				ids,
				account.as_ref(),
				*touched_at,
				*time_to_touch,
				partition_total,
			)
			.await
			.map(Response::Objects),
			Request::TouchProcesses(arg) => Self::touch_processes_with_account_with_transaction(
				txn,
				subspace,
				arg,
				partition_total,
				usage_partition_total,
			)
			.await
			.map(Response::Processes),
			Request::Update(crate::fdb::Update {
				batch_size,
				kind,
				partition_start,
				partition_end,
			}) => Self::update_with_transaction(
				txn,
				subspace,
				*batch_size,
				*kind,
				*partition_start,
				*partition_end,
				max_process_depth,
				partition_total,
				usage_partition_total,
			)
			.await
			.map(Response::UpdateOutput),
		}
	}

	fn try_split_batch_arg(
		mut arg: crate::batch::Arg,
	) -> Option<(crate::batch::Arg, crate::batch::Arg)> {
		if arg.items.len() <= 1 {
			return None;
		}
		let right_items = arg.items.split_off(arg.items.len() / 2);
		let right = crate::batch::Arg { items: right_items };

		Some((arg, right))
	}

	fn is_transaction_too_large(error: &fdb::FdbBindingError) -> bool {
		error
			.get_fdb_error()
			.is_some_and(|error| error.code() == 2101)
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

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn batch_arg_splitting_preserves_order() {
		let ids = (0..9).map(|_| tg::group::Id::new()).collect::<Vec<_>>();
		let arg = crate::batch::Arg {
			items: ids
				.iter()
				.cloned()
				.map(crate::batch::Item::DeleteGroup)
				.collect(),
		};
		let mut pending = vec![arg];
		let mut items = Vec::new();
		while let Some(arg) = pending.pop() {
			if arg.items.len() <= 2 {
				items.extend(arg.items);
				continue;
			}
			let (left, right) = Index::try_split_batch_arg(arg).unwrap();
			pending.push(right);
			pending.push(left);
		}
		let actual = items
			.into_iter()
			.map(|item| {
				let crate::batch::Item::DeleteGroup(id) = item else {
					panic!();
				};
				id
			})
			.collect::<Vec<_>>();

		assert_eq!(actual, ids);
	}
}
