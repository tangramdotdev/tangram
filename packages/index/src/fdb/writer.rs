use {
	super::{
		Index, Request, Response,
		request::{Item, Kind, Priority},
	},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	futures::{StreamExt as _, stream},
	opentelemetry as otel,
	std::{
		ops::ControlFlow,
		sync::{Arc, Mutex},
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
	pub authorize: crate::fdb::AuthorizeConfig,
	pub database: Arc<fdb::Database>,
	pub max_process_depth: Option<u64>,
	pub metrics: Metrics,
	pub partition_total: u64,
	pub receiver_high: RequestReceiver,
	pub receiver_low: RequestReceiver,
	pub receiver_medium: RequestReceiver,
	pub subspace: fdbt::Subspace,
	pub usage_partition_total: u64,
	pub write_operation_batch_size: usize,
	pub write_transaction_concurrency: usize,
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

#[derive(Clone, Copy)]
struct ExecutionConfig<'a> {
	authorize: crate::fdb::AuthorizeConfig,
	max_process_depth: Option<u64>,
	metrics: &'a Metrics,
	partition_total: u64,
	usage_partition_total: u64,
}

enum TransactionError {
	FoundationDb(fdb::FdbError),
	Tangram(tg::Error),
}

impl Index {
	pub(super) async fn writer_task(arg: Arg) {
		let Arg {
			authorize,
			database,
			max_process_depth,
			metrics,
			partition_total,
			mut receiver_high,
			mut receiver_low,
			mut receiver_medium,
			subspace,
			usage_partition_total,
			write_operation_batch_size,
			write_transaction_concurrency,
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
				batches.extend(Self::create_batches(
					requests_high,
					write_operation_batch_size,
				));
				batches.extend(Self::create_batches(
					requests_medium,
					write_operation_batch_size,
				));
				batches.extend(Self::create_batches(
					requests_low,
					write_operation_batch_size,
				));

				Some((batches, (rh, rm, rl)))
			},
		)
		.flat_map(stream::iter)
		.for_each_concurrent(write_transaction_concurrency, |batch| {
			let database = database.clone();
			let metrics = metrics.clone();
			let subspace = subspace.clone();
			async move {
				let config = ExecutionConfig {
					authorize,
					max_process_depth,
					metrics: &metrics,
					partition_total,
					usage_partition_total,
				};
				Self::execute_batch(&database, &subspace, batch, config).await;
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
			Request::AggregateUsage(_) => {
				Response::AggregateUsageOutput(crate::usage::aggregate::Output::default())
			},
			Request::Clean(_) => Response::CleanOutput(crate::clean::Output::default()),
			Request::ExpireUsage(_) => {
				Response::ExpireUsageOutput(crate::usage::expire::Output::default())
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
			| Request::PutCheckouts(_)
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
			Request::TouchCheckouts(_) => Response::Checkouts(Vec::new()),
			Request::TouchObjects(_) => Response::Objects(Vec::new()),
			Request::TouchProcesses(_) => Response::Processes(Vec::new()),
			Request::Update(_) => Response::UpdateOutput(crate::update::Output::default()),
		}
	}

	fn request_into_operations(request: Request) -> (Vec<Item>, Kind) {
		match request {
			Request::AggregateUsage(arg) => (vec![Item::AggregateUsage], Kind::AggregateUsage(arg)),
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
			Request::ExpireUsage(arg) => (vec![Item::ExpireUsage], Kind::ExpireUsage(arg)),
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
			Request::PutCheckouts(args) => {
				let items = args.into_iter().map(Item::PutCheckout).collect();
				(items, Kind::PutCheckouts)
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
			Request::TouchCheckouts(crate::fdb::TouchCheckouts {
				ids,
				time_to_touch,
				touched_at,
			}) => {
				let items = ids.into_iter().map(Item::TouchCheckout).collect();
				(
					items,
					Kind::TouchCheckouts {
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
			Kind::AggregateUsage(arg) => {
				let items: [Item; 1] = items.try_into().ok().unwrap();
				let [Item::AggregateUsage] = items else {
					unreachable!();
				};
				Request::AggregateUsage(arg.clone())
			},
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
			Kind::ExpireUsage(arg) => {
				let items: [Item; 1] = items.try_into().ok().unwrap();
				let [Item::ExpireUsage] = items else {
					unreachable!();
				};
				Request::ExpireUsage(arg.clone())
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
			Kind::PutCheckouts => {
				let args = items
					.into_iter()
					.map(|item| match item {
						Item::PutCheckout(arg) => arg,
						_ => unreachable!(),
					})
					.collect();
				Request::PutCheckouts(args)
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
			Kind::TouchCheckouts {
				time_to_touch,
				touched_at,
			} => {
				let ids = items
					.into_iter()
					.map(|item| match item {
						Item::TouchCheckout(id) => id,
						_ => unreachable!(),
					})
					.collect();
				Request::TouchCheckouts(crate::fdb::TouchCheckouts {
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
			(Response::AggregateUsageOutput(existing), Response::AggregateUsageOutput(new)) => {
				existing.count += new.count;
			},
			(Response::Checkouts(existing), Response::Checkouts(new)) => {
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
				existing.checkouts.extend(new.checkouts);
				existing.objects.extend(new.objects);
				existing.processes.extend(new.processes);
				existing.done = new.done;
			},
			(Response::ExpireUsageOutput(existing), Response::ExpireUsageOutput(new)) => {
				*existing = new;
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
		config: ExecutionConfig<'_>,
	) {
		let result = Self::execute_transaction(database, subspace, &batch.requests, config).await;

		match result {
			Ok(responses) => {
				for (response, tracker) in std::iter::zip(responses, &batch.trackers) {
					Self::complete_tracker(tracker, Ok(response));
				}
			},
			Err(TransactionError::FoundationDb(error)) if Self::is_transaction_too_large(error) => {
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
					Box::pin(Self::execute_batch(database, subspace, left, config)).await;
					Box::pin(Self::execute_batch(database, subspace, right, config)).await;
					return;
				}

				let request = batch.requests.into_iter().next().unwrap();
				let tracker = batch.trackers.into_iter().next().unwrap();
				let result = match request {
					Request::Batch(arg) if arg.items.len() > 1 => {
						Self::execute_ordered_batch(database, subspace, arg, config).await
					},
					_ => Err(tg::error!(!error, "transaction too large")),
				};
				match result {
					Ok(()) => Self::complete_tracker(&tracker, Ok(Response::Unit)),
					Err(error) => Self::fail_tracker(&tracker, &error),
				}
			},
			Err(error) => {
				let error = match error {
					TransactionError::FoundationDb(error) => {
						tg::error!(!error, "failed to execute a batch")
					},
					TransactionError::Tangram(error) => error,
				};
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
		config: ExecutionConfig<'_>,
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
				config,
			)
			.await;
			match result {
				Ok(responses) => {
					let [Response::Unit] = responses.as_slice() else {
						return Err(tg::error!("unexpected write response"));
					};
				},
				Err(TransactionError::FoundationDb(error))
					if Self::is_transaction_too_large(error) =>
				{
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
				Err(error) => {
					let error = match error {
						TransactionError::FoundationDb(error) => {
							tg::error!(!error, "failed to execute a batch")
						},
						TransactionError::Tangram(error) => error,
					};
					return Err(error);
				},
			}
		}

		Ok(())
	}

	async fn execute_transaction(
		database: &fdb::Database,
		subspace: &fdbt::Subspace,
		requests: &[Request],
		config: ExecutionConfig<'_>,
	) -> std::result::Result<Vec<Response>, TransactionError> {
		let start = std::time::Instant::now();
		let mut attempt_count = 0;

		let priority_batch = requests.iter().all(|request| {
			matches!(
				request,
				Request::AggregateUsage(_)
					| Request::Batch(_)
					| Request::Clean(_)
					| Request::ExpireUsage(_)
					| Request::CompleteLogCompaction(_)
					| Request::EnqueueLogCompaction(_)
					| Request::GetUsage { .. }
					| Request::PutCheckouts(_)
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

		let transaction = database.create_trx();
		let result = match transaction {
			Err(error) => Err(TransactionError::FoundationDb(error)),
			Ok(transaction) => {
				let mut transaction = crate::fdb::Transaction::new(transaction);
				loop {
					attempt_count += 1;
					let result = Self::execute_requests_with_transaction(
						&transaction,
						subspace,
						requests,
						config,
						priority_batch,
					)
					.await;
					let responses = match result {
						Err(error) => break Err(TransactionError::Tangram(error)),
						Ok(ControlFlow::Break(responses)) => responses,
						Ok(ControlFlow::Continue(error)) => {
							let inner = match transaction.take() {
								Err(error) => break Err(TransactionError::Tangram(error)),
								Ok(transaction) => transaction,
							};
							match inner.on_error(error).await {
								Ok(value) => {
									transaction = crate::fdb::Transaction::new(value);
									continue;
								},
								Err(error) => break Err(TransactionError::FoundationDb(error)),
							}
						},
					};
					let inner = match transaction.take() {
						Err(error) => break Err(TransactionError::Tangram(error)),
						Ok(transaction) => transaction,
					};
					match inner.commit().await {
						Ok(_) => break Ok(responses),
						Err(error) => match error.on_error().await {
							Ok(value) => {
								transaction = crate::fdb::Transaction::new(value);
							},
							Err(error) => break Err(TransactionError::FoundationDb(error)),
						},
					}
				}
			},
		};

		let duration = start.elapsed().as_secs_f64();
		config.metrics.commit_duration.record(duration, &[]);
		config.metrics.transactions.add(1, &[]);

		if attempt_count > 1 {
			config
				.metrics
				.transaction_conflict_retry
				.add(attempt_count - 1, &[]);
		}
		if matches!(
			&result,
			Err(TransactionError::FoundationDb(error)) if Self::is_transaction_too_large(*error)
		) {
			config.metrics.transaction_too_large.add(1, &[]);
		}

		result
	}

	async fn execute_requests_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		requests: &[Request],
		config: ExecutionConfig<'_>,
		priority_batch: bool,
	) -> tg::Result<ControlFlow<Vec<Response>, fdb::FdbError>> {
		if priority_batch {
			txn.set_option(fdb::options::TransactionOption::PriorityBatch)
				.unwrap();
		}
		let mut responses = Vec::with_capacity(requests.len());
		for request in requests {
			let result = Self::execute_request(txn, subspace, request, config).await;
			let response = crate::fdb::propagate!(result);
			responses.push(response);
		}

		Ok(ControlFlow::Break(responses))
	}

	async fn execute_request(
		txn: &crate::fdb::Transaction,
		subspace: &fdbt::Subspace,
		request: &Request,
		config: ExecutionConfig<'_>,
	) -> tg::Result<ControlFlow<Response, fdb::FdbError>> {
		let ExecutionConfig {
			max_process_depth,
			partition_total,
			usage_partition_total,
			..
		} = config;
		let response = match request {
			Request::AggregateUsage(arg) => {
				let result = Self::aggregate_usage_with_transaction(txn, subspace, arg).await;
				let output = crate::fdb::propagate!(result);
				Response::AggregateUsageOutput(output)
			},
			Request::Batch(arg) => {
				let result = Self::batch_with_transaction(
					config.authorize,
					txn,
					subspace,
					arg,
					config.partition_total,
					config.usage_partition_total,
				)
				.await;
				crate::fdb::propagate!(result);
				Response::Unit
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
				let result = Self::clean_with_transaction(arg).await;
				let output = crate::fdb::propagate!(result);
				Response::CleanOutput(output)
			},
			Request::ExpireUsage(arg) => {
				let result =
					Self::expire_usage_with_transaction(txn, subspace, arg, usage_partition_total)
						.await;
				let output = crate::fdb::propagate!(result);
				Response::ExpireUsageOutput(output)
			},
			Request::CompleteLogCompaction(entry) => {
				let result =
					Self::complete_log_compaction_with_transaction(txn, subspace, entry).await;
				crate::fdb::propagate!(result);
				Response::Unit
			},
			Request::DeleteGrants(args) => {
				let result =
					Self::delete_grants_with_transaction(txn, subspace, args, partition_total)
						.await;
				crate::fdb::propagate!(result);
				Response::Unit
			},
			Request::DeleteGroupMembers(args) => {
				let result = Self::delete_group_members_with_transaction(txn, subspace, args);
				crate::fdb::propagate!(result);
				Response::Unit
			},
			Request::DeleteGroups(ids) => {
				let result = Self::delete_groups_with_transaction(txn, subspace, ids).await;
				crate::fdb::propagate!(result);
				Response::Unit
			},
			Request::DeleteOrganizationMembers(args) => {
				let result =
					Self::delete_organization_members_with_transaction(txn, subspace, args);
				crate::fdb::propagate!(result);
				Response::Unit
			},
			Request::DeleteOrganizations(ids) => {
				let result = Self::delete_organizations_with_transaction(txn, subspace, ids).await;
				crate::fdb::propagate!(result);
				Response::Unit
			},
			Request::DeleteSandboxes(ids) => {
				let result = Self::delete_sandboxes_with_transaction(txn, subspace, ids);
				crate::fdb::propagate!(result);
				Response::Unit
			},
			Request::DeleteUsers(ids) => {
				let result = Self::delete_users_with_transaction(txn, subspace, ids).await;
				crate::fdb::propagate!(result);
				Response::Unit
			},
			Request::DeleteTags(tags) => {
				let result =
					Self::delete_tags_with_transaction(txn, subspace, tags, partition_total).await;
				crate::fdb::propagate!(result);
				Response::Unit
			},
			Request::EnqueueLogCompaction(process) => {
				let result = Self::enqueue_log_compaction_with_transaction(
					txn,
					subspace,
					process,
					partition_total,
				)
				.await;
				crate::fdb::propagate!(result);
				Response::Unit
			},
			Request::GetUsage {
				account,
				now,
				period,
			} => {
				let result = Self::get_usage_with_transaction(
					txn,
					subspace,
					account,
					*period,
					*now,
					usage_partition_total,
				)
				.await;
				let output = crate::fdb::propagate!(result);
				Response::Usage(output)
			},
			Request::PutCheckouts(args) => {
				let result =
					Self::put_checkouts_with_transaction(txn, subspace, args, partition_total);
				crate::fdb::propagate!(result);
				Response::Unit
			},
			Request::PutGrants(args) => {
				let result =
					Self::put_grants_with_transaction(txn, subspace, args, partition_total).await;
				crate::fdb::propagate!(result);
				Response::Unit
			},
			Request::PutGroupMembers(args) => {
				let result = Self::put_group_members_with_transaction(txn, subspace, args);
				crate::fdb::propagate!(result);
				Response::Unit
			},
			Request::PutGroups(args) => {
				let result = Self::put_groups_with_transaction(txn, subspace, args);
				crate::fdb::propagate!(result);
				Response::Unit
			},
			Request::PutObjects(args) => {
				let result =
					Self::put_objects_with_transaction(txn, subspace, args, partition_total).await;
				crate::fdb::propagate!(result);
				Response::Unit
			},
			Request::PutOrganizationMembers(args) => {
				let result = Self::put_organization_members_with_transaction(txn, subspace, args);
				crate::fdb::propagate!(result);
				Response::Unit
			},
			Request::PutOrganizations(args) => {
				let result = Self::put_organizations_with_transaction(txn, subspace, args).await;
				crate::fdb::propagate!(result);
				Response::Unit
			},
			Request::PutProcesses(args) => {
				let result =
					Self::put_processes_with_transaction(txn, subspace, args, partition_total)
						.await;
				crate::fdb::propagate!(result);
				Response::Unit
			},
			Request::PutSandboxes(args) => {
				let result = Self::put_sandboxes_with_transaction(
					txn,
					subspace,
					args,
					partition_total,
					usage_partition_total,
				)
				.await;
				crate::fdb::propagate!(result);
				Response::Unit
			},
			Request::PutTags(args) => {
				let result =
					Self::put_tags_with_transaction(txn, subspace, args, partition_total).await;
				crate::fdb::propagate!(result);
				Response::Unit
			},
			Request::PutUsers(args) => {
				let result = Self::put_users_with_transaction(txn, subspace, args).await;
				crate::fdb::propagate!(result);
				Response::Unit
			},
			Request::TouchCheckouts(crate::fdb::TouchCheckouts {
				ids,
				time_to_touch,
				touched_at,
			}) => {
				let result = Self::touch_checkouts_with_transaction(
					txn,
					subspace,
					ids,
					*touched_at,
					*time_to_touch,
					partition_total,
				)
				.await;
				let output = crate::fdb::propagate!(result);
				Response::Checkouts(output)
			},
			Request::TouchObjects(crate::fdb::TouchObjects {
				account,
				ids,
				time_to_touch,
				touched_at,
			}) => {
				let result = Self::touch_objects_with_account_with_transaction(
					txn,
					subspace,
					ids,
					account.as_ref(),
					*touched_at,
					*time_to_touch,
					partition_total,
				)
				.await;
				let output = crate::fdb::propagate!(result);
				Response::Objects(output)
			},
			Request::TouchProcesses(arg) => {
				let result = Self::touch_processes_with_account_with_transaction(
					txn,
					subspace,
					arg,
					partition_total,
					usage_partition_total,
				)
				.await;
				let output = crate::fdb::propagate!(result);
				Response::Processes(output)
			},
			Request::Update(crate::fdb::Update {
				batch_size,
				kind,
				partition_start,
				partition_end,
			}) => {
				let result = Self::update_with_transaction(
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
				.await;
				let output = crate::fdb::propagate!(result);
				Response::UpdateOutput(output)
			},
		};

		Ok(ControlFlow::Break(response))
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

	fn is_transaction_too_large(error: fdb::FdbError) -> bool {
		error.code() == 2101
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
