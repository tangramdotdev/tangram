use {
	super::{
		Index, Request, Response,
		request::{Item, Kind, Priority},
	},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	futures::{StreamExt as _, stream},
	opentelemetry as otel,
	std::{
		ops::{ControlFlow, Range},
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
	pub database: Arc<fdb::Database>,
	pub max_process_depth: Option<u64>,
	pub max_write_operation_batch_size: usize,
	pub metrics: Metrics,
	pub partition_totals: crate::PartitionTotals,
	pub receiver_high: RequestReceiver,
	pub receiver_low: RequestReceiver,
	pub receiver_medium: RequestReceiver,
	pub subspace: fdbt::Subspace,
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
	max_process_depth: Option<u64>,
	max_write_operation_batch_size: usize,
	metrics: &'a Metrics,
	partition_totals: crate::PartitionTotals,
}

enum TransactionError {
	FoundationDb(fdb::FdbError),
	Tangram(tg::Error),
}

impl Index {
	pub(super) async fn writer_task(arg: Arg) {
		let Arg {
			database,
			max_process_depth,
			max_write_operation_batch_size,
			metrics,
			partition_totals,
			mut receiver_high,
			mut receiver_low,
			mut receiver_medium,
			subspace,
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
					max_process_depth,
					max_write_operation_batch_size,
					metrics: &metrics,
					partition_totals,
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
			// Use an ordered batch so a rejected write prevents later writes in the same request.
			let request = match request {
				Request::PutProcesses(args) => {
					let items = args
						.into_iter()
						.map(tangram_index::batch::Item::PutProcess)
						.collect();
					Request::Batch(tangram_index::batch::Arg { items })
				},
				Request::PutSandboxes(args) => {
					let items = args
						.into_iter()
						.map(tangram_index::batch::Item::PutSandbox)
						.collect();
					Request::Batch(tangram_index::batch::Arg { items })
				},
				request => request,
			};
			let tracker = Arc::new(Mutex::new(RequestTracker {
				remaining: 0,
				response: Ok(Self::create_initial_response(&request)),
				sender: Some(sender),
			}));
			let operation_count = match &request {
				Request::Batch(arg) => Some(arg.items.len()),
				Request::CompletePermissionCapture(_) => Some(1),
				Request::DeleteIndexer(_) | Request::PutIndexer(_) | Request::UpdateIndexer(_) => {
					Some(1)
				},
				_ => None,
			};
			if let Some(count) = operation_count {
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
				current_batch.requests.push(request);
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
				Response::AggregateUsageOutput(tangram_index::usage::aggregate::Output::default())
			},
			Request::Clean(_) => Response::CleanOutput(tangram_index::clean::Output::default()),
			Request::ExpireUsage(_) => {
				Response::ExpireUsageOutput(tangram_index::usage::expire::Output::default())
			},
			Request::Batch(_) | Request::PutProcesses(_) | Request::PutSandboxes(_) => {
				Response::Mutation(Ok(()))
			},
			Request::CompletePermissionCapture(_)
			| Request::DeletePermissions(_)
			| Request::DeleteGroupMembers(_)
			| Request::DeleteGroups(_)
			| Request::DeleteIndexer(_)
			| Request::DeleteOrganizationMembers(_)
			| Request::DeleteOrganizations(_)
			| Request::DeleteSandboxes(_)
			| Request::DeleteTags(_)
			| Request::DeleteUsers(_)
			| Request::PutCheckouts(_)
			| Request::PutPermissions(_)
			| Request::PutGroupMembers(_)
			| Request::PutGroups(_)
			| Request::PutIndexer(_)
			| Request::PutObjects(_)
			| Request::PutOrganizationMembers(_)
			| Request::PutOrganizations(_)
			| Request::PutTags(_)
			| Request::PutUsers(_)
			| Request::UpdateIndexer(_) => Response::Unit,
			Request::GetUsage { .. } => Response::Usage(tangram_index::usage::Aggregate::default()),
			Request::TouchCheckouts(_) => Response::Checkouts(Vec::new()),
			Request::TouchObjects(_) => Response::Objects(Vec::new()),
			Request::TouchProcesses(_) => Response::Processes(Vec::new()),
			Request::Update(_) => Response::UpdateOutput(tangram_index::update::Output::default()),
		}
	}

	fn request_into_operations(request: Request) -> (Vec<Item>, Kind) {
		match request {
			Request::AggregateUsage(arg) => (vec![Item::AggregateUsage], Kind::AggregateUsage(arg)),
			Request::Batch(_)
			| Request::CompletePermissionCapture(_)
			| Request::DeleteIndexer(_)
			| Request::PutIndexer(_)
			| Request::UpdateIndexer(_) => unreachable!(),
			Request::Clean(crate::Clean {
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
			Request::DeletePermissions(args) => {
				let items = args.into_iter().map(Item::DeletePermission).collect();
				(items, Kind::DeletePermissions)
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
			Request::PutPermissions(args) => {
				let items = args.into_iter().map(Item::PutPermission).collect();
				(items, Kind::PutPermissions)
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
			Request::TouchCheckouts(crate::TouchCheckouts {
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
			Request::TouchObjects(crate::TouchObjects {
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
			Request::TouchProcesses(crate::TouchProcesses {
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
			Request::Update(crate::Update {
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
			} => Request::Clean(crate::Clean {
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
			Kind::DeletePermissions => {
				let args = items
					.into_iter()
					.map(|item| match item {
						Item::DeletePermission(arg) => arg,
						_ => unreachable!(),
					})
					.collect();
				Request::DeletePermissions(args)
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
			Kind::PutPermissions => {
				let args = items
					.into_iter()
					.map(|item| match item {
						Item::PutPermission(arg) => arg,
						_ => unreachable!(),
					})
					.collect();
				Request::PutPermissions(args)
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
				Request::TouchCheckouts(crate::TouchCheckouts {
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
				Request::TouchObjects(crate::TouchObjects {
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
				Request::TouchProcesses(crate::TouchProcesses {
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
			} => Request::Update(crate::Update {
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
			(Response::Mutation(existing), Response::Mutation(new)) => {
				if existing.is_ok() {
					*existing = new;
				}
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
				existing.sandboxes.extend(new.sandboxes);
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
		if let [Request::Batch(arg)] = batch.requests.as_slice()
			&& arg.items.len() > config.max_write_operation_batch_size
		{
			let request = batch.requests.into_iter().next().unwrap();
			let tracker = batch.trackers.into_iter().next().unwrap();
			let Request::Batch(arg) = request else {
				unreachable!();
			};
			match Self::execute_ordered_batch(database, subspace, arg, config).await {
				Ok(result) => Self::complete_tracker(&tracker, Ok(Response::Mutation(result))),
				Err(error) => Self::fail_tracker(&tracker, &error),
			}
			return;
		}

		let result = Self::execute_transaction(database, subspace, &batch.requests, config).await;

		match result {
			Ok(responses) => {
				for (response, tracker) in std::iter::zip(responses, &batch.trackers) {
					Self::complete_tracker(tracker, Ok(response));
				}
			},
			Err(TransactionError::FoundationDb(error)) if Self::is_split_error(error) => {
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
					_ => Err(tg::error!(
						!error,
						"failed to execute a request that cannot be split"
					)),
				};
				match result {
					Ok(result) => Self::complete_tracker(&tracker, Ok(Response::Mutation(result))),
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
		arg: tangram_index::batch::Arg,
		config: ExecutionConfig<'_>,
	) -> tg::Result<tg::Result<()>> {
		let items = arg.items;
		let size = config.max_write_operation_batch_size;
		Self::execute_ordered_ranges(items.len(), size, |range| {
			let items_or_requests = tg::Either::Left(&items[range]);
			async move {
				let responses = Self::execute_transaction_with(
					database,
					subspace,
					items_or_requests,
					config,
					true,
				)
				.await?;
				let [Response::Mutation(result)] = responses.as_slice() else {
					return Err(TransactionError::Tangram(tg::error!(
						"unexpected write response"
					)));
				};
				Ok(result.clone())
			}
		})
		.await
	}

	async fn execute_ordered_ranges<F, Fut>(
		len: usize,
		size: usize,
		mut execute: F,
	) -> tg::Result<tg::Result<()>>
	where
		F: FnMut(Range<usize>) -> Fut,
		Fut: Future<Output = Result<tg::Result<()>, TransactionError>>,
	{
		let mut pending = if len > size {
			(0..len)
				.step_by(size.max(1))
				.map(|start| start..(start + size.max(1)).min(len))
				.collect::<Vec<_>>()
		} else if let Some((left, right)) = Self::try_split_range(0..len) {
			vec![left, right]
		} else {
			std::iter::once(0..len).collect()
		};
		// Reverse the ranges so they are popped, and therefore committed, in order.
		pending.reverse();
		while let Some(range) = pending.pop() {
			match execute(range.clone()).await {
				Ok(Ok(())) => {},
				Ok(Err(error)) => return Ok(Err(error)),
				Err(TransactionError::FoundationDb(error)) if Self::is_split_error(error) => {
					let Some((left, right)) = Self::try_split_range(range) else {
						return Err(tg::error!(!error, "failed to execute an index batch item"));
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

		Ok(Ok(()))
	}

	async fn execute_transaction(
		database: &fdb::Database,
		subspace: &fdbt::Subspace,
		requests: &[Request],
		config: ExecutionConfig<'_>,
	) -> Result<Vec<Response>, TransactionError> {
		let priority_batch = requests.iter().all(|request| {
			matches!(
				request,
				Request::AggregateUsage(_)
					| Request::Batch(_)
					| Request::CompletePermissionCapture(_)
					| Request::Clean(_)
					| Request::ExpireUsage(_)
					| Request::GetUsage { .. }
					| Request::PutCheckouts(_)
					| Request::PutPermissions(_)
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

		let items_or_requests = tg::Either::Right(requests);
		Self::execute_transaction_with(
			database,
			subspace,
			items_or_requests,
			config,
			priority_batch,
		)
		.await
	}

	async fn execute_transaction_with(
		database: &fdb::Database,
		subspace: &fdbt::Subspace,
		items_or_requests: tg::Either<&[tangram_index::batch::Item], &[Request]>,
		config: ExecutionConfig<'_>,
		priority_batch: bool,
	) -> Result<Vec<Response>, TransactionError> {
		let start = std::time::Instant::now();
		let mut attempt_count = 0;

		let transaction = database.create_trx();
		let result = match transaction {
			Err(error) => Err(TransactionError::FoundationDb(error)),
			Ok(transaction) => {
				let mut transaction = crate::Transaction::new(transaction);
				loop {
					attempt_count += 1;
					if priority_batch {
						transaction
							.set_option(fdb::options::TransactionOption::PriorityBatch)
							.unwrap();
					}
					let result = match &items_or_requests {
						tg::Either::Left(items) => Self::batch_with_transaction(
							&transaction,
							subspace,
							items,
							config.partition_totals,
						)
						.await
						.map(|flow| flow.map_break(|result| vec![Response::Mutation(result)])),
						tg::Either::Right(requests) => {
							Self::execute_requests_with_transaction(
								&transaction,
								subspace,
								requests,
								config,
							)
							.await
						},
					};
					let responses = match result {
						Err(error) => break Err(TransactionError::Tangram(error)),
						Ok(ControlFlow::Break(responses)) => responses,
						Ok(ControlFlow::Continue(error)) => {
							if Self::is_transaction_too_old(error) {
								break Err(TransactionError::FoundationDb(error));
							}
							let inner = match transaction.take() {
								Err(error) => break Err(TransactionError::Tangram(error)),
								Ok(transaction) => transaction,
							};
							match inner.on_error(error).await {
								Ok(value) => {
									transaction = crate::Transaction::new(value);
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
						Err(error) => {
							if Self::is_transaction_too_old(*error) {
								break Err(TransactionError::FoundationDb(error.into()));
							}
							match error.on_error().await {
								Ok(value) => {
									transaction = crate::Transaction::new(value);
								},
								Err(error) => break Err(TransactionError::FoundationDb(error)),
							}
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
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		requests: &[Request],
		config: ExecutionConfig<'_>,
	) -> tg::Result<ControlFlow<Vec<Response>, fdb::FdbError>> {
		let mut responses = Vec::with_capacity(requests.len());
		for request in requests {
			let result = Self::execute_request(txn, subspace, request, config).await;
			let response = crate::propagate!(result);
			responses.push(response);
		}

		Ok(ControlFlow::Break(responses))
	}

	async fn execute_request(
		txn: &crate::Transaction,
		subspace: &fdbt::Subspace,
		request: &Request,
		config: ExecutionConfig<'_>,
	) -> tg::Result<ControlFlow<Response, fdb::FdbError>> {
		let ExecutionConfig {
			max_process_depth,
			partition_totals,
			..
		} = config;
		let partition_total = partition_totals.cleaning;
		let usage_partition_total = partition_totals.usage;
		let response = match request {
			Request::CompletePermissionCapture(entry) => {
				Self::complete_permission_capture_with_transaction(txn, subspace, entry);
				Response::Unit
			},
			Request::AggregateUsage(arg) => {
				let result = Self::aggregate_usage_with_transaction(txn, subspace, arg).await;
				let output = crate::propagate!(result);
				Response::AggregateUsageOutput(output)
			},
			Request::Batch(arg) => {
				let result = Self::batch_with_transaction(
					txn,
					subspace,
					&arg.items,
					config.partition_totals,
				)
				.await;
				let result = crate::propagate!(result);
				Response::Mutation(result)
			},
			Request::Clean(crate::Clean {
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
					partition_totals,
					subspace,
					txn,
				};
				let result = Self::clean_with_transaction(arg).await;
				let output = crate::propagate!(result);
				Response::CleanOutput(output)
			},
			Request::ExpireUsage(arg) => {
				let result =
					Self::expire_usage_with_transaction(txn, subspace, arg, usage_partition_total)
						.await;
				let output = crate::propagate!(result);
				Response::ExpireUsageOutput(output)
			},
			Request::DeletePermissions(args) => {
				let result = Self::delete_permissions_with_transaction(
					txn,
					subspace,
					args,
					partition_totals,
				)
				.await;
				crate::propagate!(result);
				Response::Unit
			},
			Request::DeleteGroupMembers(args) => {
				let result = Self::delete_group_members_with_transaction(txn, subspace, args);
				crate::propagate!(result);
				Response::Unit
			},
			Request::DeleteGroups(ids) => {
				let result = Self::delete_groups_with_transaction(txn, subspace, ids).await;
				crate::propagate!(result);
				Response::Unit
			},
			Request::DeleteIndexer(arg) => {
				let result = Self::delete_indexer_with_transaction(txn, subspace, arg).await;
				crate::propagate!(result);
				Response::Unit
			},
			Request::DeleteOrganizationMembers(args) => {
				let result =
					Self::delete_organization_members_with_transaction(txn, subspace, args);
				crate::propagate!(result);
				Response::Unit
			},
			Request::DeleteOrganizations(ids) => {
				let result = Self::delete_organizations_with_transaction(txn, subspace, ids).await;
				crate::propagate!(result);
				Response::Unit
			},
			Request::DeleteSandboxes(ids) => {
				let result =
					Self::delete_sandboxes_with_transaction(txn, subspace, ids, partition_total)
						.await;
				crate::propagate!(result);
				Response::Unit
			},
			Request::DeleteUsers(ids) => {
				let result = Self::delete_users_with_transaction(txn, subspace, ids).await;
				crate::propagate!(result);
				Response::Unit
			},
			Request::DeleteTags(tags) => {
				let result =
					Self::delete_tags_with_transaction(txn, subspace, tags, partition_totals).await;
				crate::propagate!(result);
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
				let output = crate::propagate!(result);
				Response::Usage(output)
			},
			Request::PutCheckouts(args) => {
				let result =
					Self::put_checkouts_with_transaction(txn, subspace, args, partition_total);
				crate::propagate!(result);
				Response::Unit
			},
			Request::PutPermissions(args) => {
				let result =
					Self::put_permissions_with_transaction(txn, subspace, args, partition_totals)
						.await;
				crate::propagate!(result);
				Response::Unit
			},
			Request::PutGroupMembers(args) => {
				let result = Self::put_group_members_with_transaction(txn, subspace, args);
				crate::propagate!(result);
				Response::Unit
			},
			Request::PutGroups(args) => {
				let result = Self::put_groups_with_transaction(txn, subspace, args);
				crate::propagate!(result);
				Response::Unit
			},
			Request::PutIndexer(arg) => {
				let result = Self::put_indexer_with_transaction(txn, subspace, arg).await;
				crate::propagate!(result);
				Response::Unit
			},
			Request::PutObjects(args) => {
				let result =
					Self::put_objects_with_transaction(txn, subspace, args, partition_totals).await;
				crate::propagate!(result);
				Response::Unit
			},
			Request::PutOrganizationMembers(args) => {
				let result = Self::put_organization_members_with_transaction(txn, subspace, args);
				crate::propagate!(result);
				Response::Unit
			},
			Request::PutOrganizations(args) => {
				let result = Self::put_organizations_with_transaction(txn, subspace, args).await;
				crate::propagate!(result);
				Response::Unit
			},
			Request::PutProcesses(args) => {
				let result =
					Self::put_processes_with_transaction(txn, subspace, args, partition_totals)
						.await;
				let result = crate::propagate!(result);
				Response::Mutation(result)
			},
			Request::PutSandboxes(args) => {
				let result =
					Self::put_sandboxes_with_transaction(txn, subspace, args, partition_totals)
						.await;
				let result = crate::propagate!(result);
				Response::Mutation(result)
			},
			Request::PutTags(args) => {
				let result =
					Self::put_tags_with_transaction(txn, subspace, args, partition_totals).await;
				crate::propagate!(result);
				Response::Unit
			},
			Request::PutUsers(args) => {
				let result = Self::put_users_with_transaction(txn, subspace, args).await;
				crate::propagate!(result);
				Response::Unit
			},
			Request::TouchCheckouts(crate::TouchCheckouts {
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
				let output = crate::propagate!(result);
				Response::Checkouts(output)
			},
			Request::TouchObjects(crate::TouchObjects {
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
				let output = crate::propagate!(result);
				Response::Objects(output)
			},
			Request::TouchProcesses(arg) => {
				let result = Self::touch_processes_with_account_with_transaction(
					txn,
					subspace,
					arg,
					partition_totals,
				)
				.await;
				let output = crate::propagate!(result);
				Response::Processes(output)
			},
			Request::Update(crate::Update {
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
					partition_totals,
				)
				.await;
				let output = crate::propagate!(result);
				Response::UpdateOutput(output)
			},
			Request::UpdateIndexer(arg) => {
				let result = Self::update_indexer_with_transaction(txn, subspace, arg).await;
				crate::propagate!(result);
				Response::Unit
			},
		};

		Ok(ControlFlow::Break(response))
	}

	fn try_split_range(range: Range<usize>) -> Option<(Range<usize>, Range<usize>)> {
		if range.len() <= 1 {
			return None;
		}
		let mid = range.start + range.len() / 2;

		Some((range.start..mid, mid..range.end))
	}

	fn is_transaction_too_large(error: fdb::FdbError) -> bool {
		error.code() == 2101
	}

	fn is_transaction_too_old(error: fdb::FdbError) -> bool {
		error.code() == 1007
	}

	fn is_split_error(error: fdb::FdbError) -> bool {
		Self::is_transaction_too_large(error) || Self::is_transaction_too_old(error)
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

	#[tokio::test]
	async fn split_request_preserves_the_first_logic_error() {
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let tracker = Arc::new(Mutex::new(RequestTracker {
			remaining: 3,
			response: Ok(Response::Mutation(Ok(()))),
			sender: Some(sender),
		}));
		Index::complete_tracker(
			&tracker,
			Ok(Response::Mutation(Err(tg::error!("first rejection")))),
		);
		Index::complete_tracker(&tracker, Ok(Response::Mutation(Ok(()))));
		Index::complete_tracker(
			&tracker,
			Ok(Response::Mutation(Err(tg::error!("second rejection")))),
		);
		let Response::Mutation(result) = receiver.await.unwrap().unwrap() else {
			panic!()
		};
		assert_eq!(
			result.unwrap_err().message().as_deref(),
			Some("first rejection")
		);
	}

	#[tokio::test]
	async fn transaction_failure_overrides_a_pending_logic_error() {
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let tracker = Arc::new(Mutex::new(RequestTracker {
			remaining: 2,
			response: Ok(Response::Mutation(Ok(()))),
			sender: Some(sender),
		}));
		Index::complete_tracker(
			&tracker,
			Ok(Response::Mutation(Err(tg::error!("logic rejection")))),
		);
		Index::fail_tracker(&tracker, &tg::error!("transaction failure"));
		let error = receiver.await.unwrap().err().unwrap();
		assert_eq!(error.message().as_deref(), Some("transaction failure"));
	}

	#[tokio::test]
	async fn ordered_ranges_commit_every_item_in_order() {
		let len = 10_007;
		let size = 1_000;
		let mut committed = Vec::new();
		let result = Index::execute_ordered_ranges(len, size, |range: Range<usize>| {
			assert!(range.len() <= size);
			committed.extend(range);
			std::future::ready(Ok(Ok(())))
		})
		.await;

		assert!(matches!(result, Ok(Ok(()))));
		assert_eq!(committed, (0..len).collect::<Vec<_>>());
	}
}
