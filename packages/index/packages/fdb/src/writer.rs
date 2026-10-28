use {
	super::{
		Index, Request, Response,
		request::{Item, Kind, Priority},
	},
	foundationdb as fdb, foundationdb_tuple as fdbt,
	futures::{FutureExt as _, StreamExt as _, stream},
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
	pub database: Arc<fdb::Database>,
	pub max_process_depth: Option<u64>,
	pub max_write_operation_batch_size: usize,
	pub metrics: Metrics,
	pub object_permission_time_to_touch: std::time::Duration,
	pub partition_totals: crate::PartitionTotals,
	pub process_data_fragment_size: usize,
	pub process_permission_time_to_touch: std::time::Duration,
	pub receiver_high: RequestReceiver,
	pub receiver_low: RequestReceiver,
	pub receiver_medium: RequestReceiver,
	pub storage_relation_batch_size: usize,
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
	metrics: &'a Metrics,
	object_permission_time_to_touch: std::time::Duration,
	partition_totals: crate::PartitionTotals,
	process_data_fragment_size: usize,
	process_permission_time_to_touch: std::time::Duration,
	storage_relation_batch_size: usize,
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
			object_permission_time_to_touch,
			partition_totals,
			process_data_fragment_size,
			process_permission_time_to_touch,
			mut receiver_high,
			mut receiver_low,
			mut receiver_medium,
			storage_relation_batch_size,
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
					max_write_operation_batch_size,
				));
				batches.extend(Self::create_batches(
					requests_medium,
					write_operation_batch_size,
					max_write_operation_batch_size,
				));
				batches.extend(Self::create_batches(
					requests_low,
					write_operation_batch_size,
					max_write_operation_batch_size,
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
					metrics: &metrics,
					object_permission_time_to_touch,
					partition_totals,
					process_data_fragment_size,
					process_permission_time_to_touch,
					storage_relation_batch_size,
				};
				Self::execute_batch(&database, &subspace, batch, config)
					.boxed()
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

	fn create_batches(
		requests: Vec<(Request, ResponseSender)>,
		max_items: usize,
		batch_size: usize,
	) -> Vec<Batch> {
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
			let operation_count = match &request {
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

			// Split the request's operations across transactions, limiting batch items to the batch size because they read more.
			let (items, kind) = Self::request_into_operations(request);
			let limit = if matches!(kind, Kind::Batch) {
				batch_size
			} else {
				max_items
			};
			let mut iter = items.into_iter().peekable();
			let mut remaining_count = iter.len();

			while remaining_count > 0 {
				let space = limit.saturating_sub(current_count);
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
			Request::Batch(arg) => {
				let items = arg.items.into_iter().map(Item::Batch).collect();
				(items, Kind::Batch)
			},
			Request::CompletePermissionCapture(_)
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
				delta_time_to_live,
				now,
				period,
			} => (
				vec![Item::GetUsage],
				Kind::GetUsage {
					account,
					delta_time_to_live,
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
				data,
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
						data,
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
			Kind::Batch => {
				let items = items
					.into_iter()
					.map(|item| match item {
						Item::Batch(item) => item,
						_ => unreachable!(),
					})
					.collect();
				Request::Batch(tangram_index::batch::Arg { items })
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
				delta_time_to_live,
				now,
				period,
			} => {
				let items: [Item; 1] = items.try_into().ok().unwrap();
				let [Item::GetUsage] = items else {
					unreachable!();
				};
				Request::GetUsage {
					account: account.clone(),
					delta_time_to_live: *delta_time_to_live,
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
				data,
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
					data: *data,
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
					futures::join!(
						Box::pin(Self::execute_batch(database, subspace, left, config)),
						Box::pin(Self::execute_batch(database, subspace, right, config)),
					);
					return;
				}

				let request = batch.requests.into_iter().next().unwrap();
				let tracker = batch.trackers.into_iter().next().unwrap();
				let result = match request {
					Request::CompletePermissionCapture(_)
					| Request::DeleteIndexer(_)
					| Request::PutIndexer(_)
					| Request::UpdateIndexer(_) => Err(tg::error!(
						!error,
						"failed to execute a request that cannot be split"
					)),
					request => {
						// Split the request in half and execute the halves concurrently.
						let (mut items, kind) = Self::request_into_operations(request);
						if items.len() > 1 {
							let right = items.split_off(items.len() / 2);
							tracker.lock().unwrap().remaining += 1;
							let [left, right] = [items, right].map(|items| {
								let request = Self::request_from_operations(items, &kind);
								Batch {
									requests: vec![request],
									trackers: vec![tracker.clone()],
								}
							});
							futures::join!(
								Box::pin(Self::execute_batch(database, subspace, left, config)),
								Box::pin(Self::execute_batch(database, subspace, right, config)),
							);
							return;
						}
						Err(tg::error!(
							!error,
							"failed to execute a request that cannot be split"
						))
					},
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

	/// Describe a batch that exceeds the `FoundationDB` key or value size limit, naming its largest item, so that it is clear that indexing cannot proceed until a fix is rolled out.
	fn size_limit_error(error: fdb::FdbError, requests: &[Request]) -> tg::Error {
		let items = requests
			.iter()
			.filter_map(|request| match request {
				Request::Batch(arg) => Some(arg.items.iter()),
				_ => None,
			})
			.flatten()
			.collect::<Vec<_>>();
		let largest = items
			.into_iter()
			.filter_map(|item| {
				let size = tangram_serialize::to_vec(item).ok()?.len();
				Some((item, size))
			})
			.max_by_key(|(_, size)| *size);
		let Some((item, size)) = largest else {
			return tg::error!(
				!error,
				"a batch exceeds the FoundationDB key or value size limit, so indexing cannot proceed until a fix is rolled out"
			);
		};
		let kind = batch_item_kind(item);

		tg::error!(
			!error,
			%kind,
			%size,
			"a batch item exceeds the FoundationDB key or value size limit, so indexing cannot proceed until a fix is rolled out"
		)
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

		let result =
			Self::execute_transaction_inner(database, subspace, requests, config, priority_batch)
				.await;

		// Name the largest item of a batch that exceeds the size limit, so that the indexer's retry logs show what is blocking indexing.
		match result {
			Err(TransactionError::FoundationDb(error)) if Self::is_size_limit(error) => {
				let error = Self::size_limit_error(error, requests);
				Err(TransactionError::Tangram(error))
			},
			result => result,
		}
	}

	async fn execute_transaction_inner(
		database: &fdb::Database,
		subspace: &fdbt::Subspace,
		requests: &[Request],
		config: ExecutionConfig<'_>,
		priority_batch: bool,
	) -> Result<Vec<Response>, TransactionError> {
		let start = std::time::Instant::now();
		let mut attempt_count = 0;

		let transaction = database.create_trx();
		let result = match transaction {
			Err(error) => Err(TransactionError::FoundationDb(error)),
			Ok(mut transaction) => loop {
				attempt_count += 1;
				if priority_batch {
					transaction
						.set_option(fdb::options::TransactionOption::PriorityBatch)
						.unwrap();
				}
				let result = Self::execute_requests_with_transaction(
					&transaction,
					subspace,
					requests,
					config,
				)
				.await;
				let responses = match result {
					Err(error) => break Err(TransactionError::Tangram(error)),
					Ok(ControlFlow::Break(responses)) => responses,
					Ok(ControlFlow::Continue(error)) => {
						if Self::is_transaction_too_old(error) {
							break Err(TransactionError::FoundationDb(error));
						}
						match transaction.on_error(error).await {
							Ok(value) => {
								transaction = value;
								continue;
							},
							Err(error) => break Err(TransactionError::FoundationDb(error)),
						}
					},
				};
				match transaction.commit().await {
					Ok(_) => break Ok(responses),
					Err(error) => {
						if Self::is_transaction_too_old(*error) {
							break Err(TransactionError::FoundationDb(error.into()));
						}
						match error.on_error().await {
							Ok(value) => {
								transaction = value;
							},
							Err(error) => break Err(TransactionError::FoundationDb(error)),
						}
					},
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
		txn: &fdb::Transaction,
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
		txn: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		request: &Request,
		config: ExecutionConfig<'_>,
	) -> tg::Result<ControlFlow<Response, fdb::FdbError>> {
		let ExecutionConfig {
			max_process_depth,
			object_permission_time_to_touch,
			partition_totals,
			process_permission_time_to_touch,
			storage_relation_batch_size,
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
					config.process_data_fragment_size,
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
				delta_time_to_live,
				now,
				period,
			} => {
				let result = Self::get_usage_with_transaction(
					txn,
					subspace,
					account,
					*period,
					*now,
					*delta_time_to_live,
					usage_partition_total,
				)
				.await;
				let output = crate::propagate!(result);
				Response::Usage(output)
			},
			Request::PutCheckouts(args) => {
				let result =
					Self::put_checkouts_with_transaction(txn, subspace, args, partition_total)
						.await;
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
				let result = Self::put_processes_with_transaction(
					txn,
					subspace,
					args,
					partition_totals,
					config.process_data_fragment_size,
				)
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
					object_permission_time_to_touch,
					process_permission_time_to_touch,
					partition_totals,
					storage_relation_batch_size,
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

	fn is_transaction_too_large(error: fdb::FdbError) -> bool {
		error.code() == 2101
	}

	fn is_transaction_too_old(error: fdb::FdbError) -> bool {
		error.code() == 1007
	}

	fn is_size_limit(error: fdb::FdbError) -> bool {
		matches!(error.code(), 2102 | 2103)
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

fn batch_item_kind(item: &tangram_index::batch::Item) -> &'static str {
	match item {
		tangram_index::batch::Item::DeleteCheckout(_) => "delete_checkout",
		tangram_index::batch::Item::DeleteDelegations(_) => "delete_delegations",
		tangram_index::batch::Item::DeleteGroup(_) => "delete_group",
		tangram_index::batch::Item::DeleteGroupMember(_) => "delete_group_member",
		tangram_index::batch::Item::DeleteOrganization(_) => "delete_organization",
		tangram_index::batch::Item::DeleteOrganizationMember(_) => "delete_organization_member",
		tangram_index::batch::Item::DeletePermission(_) => "delete_permission",
		tangram_index::batch::Item::DeleteSandbox(_) => "delete_sandbox",
		tangram_index::batch::Item::DeleteTag(_) => "delete_tag",
		tangram_index::batch::Item::DeleteUser(_) => "delete_user",
		tangram_index::batch::Item::EnqueuePermissionCapture(_) => "enqueue_permission_capture",
		tangram_index::batch::Item::PutAccountObject(_) => "put_account_object",
		tangram_index::batch::Item::PutAccountProcess(_) => "put_account_process",
		tangram_index::batch::Item::PutCheckout(_) => "put_checkout",
		tangram_index::batch::Item::PutDelegation(_) => "put_delegation",
		tangram_index::batch::Item::PutGroup(_) => "put_group",
		tangram_index::batch::Item::PutGroupMember(_) => "put_group_member",
		tangram_index::batch::Item::PutObject(_) => "put_object",
		tangram_index::batch::Item::PutOrganization(_) => "put_organization",
		tangram_index::batch::Item::PutOrganizationMember(_) => "put_organization_member",
		tangram_index::batch::Item::PutPermission(_) => "put_permission",
		tangram_index::batch::Item::PutProcess(_) => "put_process",
		tangram_index::batch::Item::PutSandbox(_) => "put_sandbox",
		tangram_index::batch::Item::PutSync(_) => "put_sync",
		tangram_index::batch::Item::PutTag(_) => "put_tag",
		tangram_index::batch::Item::PutUser(_) => "put_user",
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
	async fn batch_items_split_across_transactions_up_to_the_batch_size() {
		// Create a batch request with more items than the batch size.
		let items = (0..2_500)
			.map(|_| tangram_index::batch::Item::DeleteTag(tg::tag::Id::new()))
			.collect();
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::Batch(tangram_index::batch::Arg { items });

		// Split the request and complete every transaction.
		let batches = Index::create_batches(vec![(request, sender)], 8_000, 1_000);
		let sizes = batches
			.iter()
			.map(|batch| {
				let [Request::Batch(arg)] = batch.requests.as_slice() else {
					panic!()
				};
				arg.items.len()
			})
			.collect::<Vec<_>>();
		assert_eq!(sizes, [1_000, 1_000, 500]);
		for batch in &batches {
			Index::complete_tracker(&batch.trackers[0], Ok(Response::Mutation(Ok(()))));
		}

		// The request completes once every transaction completes.
		let response = receiver.await.unwrap().unwrap();
		assert!(matches!(response, Response::Mutation(Ok(()))));
	}
}
