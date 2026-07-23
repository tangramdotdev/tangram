use {
	super::{
		Db, Index, Request, Response,
		request::{Item, Kind, Priority},
	},
	crossbeam_channel as crossbeam, foundationdb_tuple as fdbt, heed as lmdb,
	std::collections::VecDeque,
	tangram_client::prelude::*,
};

pub(super) const CHANNEL_CAPACITY: usize = 256;

pub(super) type RequestReceiver = crossbeam::Receiver<(Request, ResponseSender)>;
pub(super) type RequestSender = crossbeam::Sender<(Request, ResponseSender)>;
pub(super) type ResponseSender = tokio::sync::oneshot::Sender<tg::Result<Response>>;

#[derive(Clone, Copy)]
pub(super) struct Arg<'a> {
	pub db: &'a Db,
	pub env: &'a lmdb::Env,
	pub max_process_depth: Option<u64>,
	pub receiver_high: &'a RequestReceiver,
	pub receiver_low: &'a RequestReceiver,
	pub receiver_medium: &'a RequestReceiver,
	pub subspace: &'a fdbt::Subspace,
	pub write_batch_size: usize,
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
	pub(super) fn writer_task(arg: Arg<'_>) {
		let Arg {
			db,
			env,
			max_process_depth,
			receiver_high,
			receiver_low,
			receiver_medium,
			subspace,
			write_batch_size,
		} = arg;
		let mut trackers: Vec<RequestTracker> = Vec::new();
		let mut queue_high: VecDeque<Batch> = VecDeque::new();
		let mut queue_medium: VecDeque<Batch> = VecDeque::new();
		let mut queue_low: VecDeque<Batch> = VecDeque::new();

		loop {
			// Drain high-priority requests.
			queue_high.extend(Self::create_batches(
				Self::drain_receiver(receiver_high),
				&mut trackers,
				write_batch_size,
			));

			// Drain medium-priority requests.
			queue_medium.extend(Self::create_batches(
				Self::drain_receiver(receiver_medium),
				&mut trackers,
				write_batch_size,
			));

			// If all queues are empty, try low-priority requests.
			if queue_high.is_empty() && queue_medium.is_empty() && queue_low.is_empty() {
				queue_low.extend(Self::create_batches(
					Self::drain_receiver(receiver_low),
					&mut trackers,
					write_batch_size,
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
								write_batch_size,
							));
						}
					},
					recv(receiver_medium) -> result => {
						if let Ok(item) = result {
							queue_medium.extend(Self::create_batches(
								vec![item],
								&mut trackers,
								write_batch_size,
							));
						}
					},
					recv(receiver_low) -> result => {
						if let Ok(item) = result {
							queue_low.extend(Self::create_batches(
								vec![item],
								&mut trackers,
								write_batch_size,
							));
						}
					},
				}

				// After waking, drain all receivers.
				queue_high.extend(Self::create_batches(
					Self::drain_receiver(receiver_high),
					&mut trackers,
					write_batch_size,
				));
				queue_medium.extend(Self::create_batches(
					Self::drain_receiver(receiver_medium),
					&mut trackers,
					write_batch_size,
				));
				queue_low.extend(Self::create_batches(
					Self::drain_receiver(receiver_low),
					&mut trackers,
					write_batch_size,
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
				.map_err(|error| tg::error!(!error, "failed to begin a transaction"));
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
					Request::Batch(arg) => {
						Self::batch_with_transaction(db, subspace, &mut transaction, &arg)
							.map(|()| Response::Unit)
					},
					Request::Clean(crate::lmdb::Clean {
						batch_size,
						max_object_touched_at,
						max_process_touched_at,
						max_sandbox_touched_at,
						now,
					}) => Self::clean_with_transaction(crate::lmdb::clean::TransactionArg {
						batch_size,
						db,
						max_object_touched_at,
						max_process_touched_at,
						max_sandbox_touched_at,
						now,
						subspace,
						transaction: &mut transaction,
					})
					.map(Response::CleanOutput),
					Request::CompleteFinalization(entry) => {
						Self::complete_finalization_with_transaction(
							db,
							subspace,
							&mut transaction,
							&entry,
						)
						.map(|()| Response::Unit)
					},
					Request::DeleteGrants(args) => {
						Self::delete_grants_with_transaction(db, subspace, &mut transaction, &args)
							.map(|()| Response::Unit)
					},
					Request::DeleteGroupMembers(args) => {
						Self::delete_group_members_with_transaction(
							db,
							subspace,
							&mut transaction,
							&args,
						)
						.map(|()| Response::Unit)
					},
					Request::DeleteGroups(ids) => {
						Self::delete_groups_with_transaction(db, subspace, &mut transaction, &ids)
							.map(|()| Response::Unit)
					},
					Request::DeleteOrganizationMembers(args) => {
						Self::delete_organization_members_with_transaction(
							db,
							subspace,
							&mut transaction,
							&args,
						)
						.map(|()| Response::Unit)
					},
					Request::DeleteOrganizations(ids) => {
						Self::delete_organizations_with_transaction(
							db,
							subspace,
							&mut transaction,
							&ids,
						)
						.map(|()| Response::Unit)
					},
					Request::DeleteSandboxes(ids) => Self::delete_sandboxes_with_transaction(
						db,
						subspace,
						&mut transaction,
						&ids,
					)
					.map(|()| Response::Unit),
					Request::DeleteUsers(ids) => {
						Self::delete_users_with_transaction(db, subspace, &mut transaction, &ids)
							.map(|()| Response::Unit)
					},
					Request::DeleteTags(tags) => {
						Self::delete_tags_with_transaction(db, subspace, &mut transaction, &tags)
							.map(|()| Response::Unit)
					},
					Request::EnqueueFinalization(item) => {
						Self::enqueue_finalization_with_transaction(
							db,
							subspace,
							&mut transaction,
							&item,
						)
						.map(|()| Response::Unit)
					},
					Request::PutCacheEntries(args) => Self::put_cache_entries_with_transaction(
						db,
						subspace,
						&mut transaction,
						&args,
					)
					.map(|()| Response::Unit),
					Request::PutGrants(args) => {
						Self::put_grants_with_transaction(db, subspace, &mut transaction, &args)
							.map(|()| Response::Unit)
					},
					Request::PutGroupMembers(args) => Self::put_group_members_with_transaction(
						db,
						subspace,
						&mut transaction,
						&args,
					)
					.map(|()| Response::Unit),
					Request::PutGroups(args) => {
						Self::put_groups_with_transaction(db, subspace, &mut transaction, &args)
							.map(|()| Response::Unit)
					},
					Request::PutObjects(args) => {
						Self::put_objects_with_transaction(db, subspace, &mut transaction, &args)
							.map(|()| Response::Unit)
					},
					Request::PutOrganizationMembers(args) => {
						Self::put_organization_members_with_transaction(
							db,
							subspace,
							&mut transaction,
							&args,
						)
						.map(|()| Response::Unit)
					},
					Request::PutOrganizations(args) => Self::put_organizations_with_transaction(
						db,
						subspace,
						&mut transaction,
						&args,
					)
					.map(|()| Response::Unit),
					Request::PutProcesses(args) => {
						Self::put_processes_with_transaction(db, subspace, &mut transaction, &args)
							.map(|()| Response::Unit)
					},
					Request::PutRunners(args) => {
						Self::put_runners_with_transaction(db, subspace, &mut transaction, &args)
							.map(|()| Response::Unit)
					},
					Request::PutSandboxes(args) => {
						Self::put_sandboxes_with_transaction(db, subspace, &mut transaction, &args)
							.map(|()| Response::Unit)
					},
					Request::PutTags(tags) => {
						Self::put_tags_with_transaction(db, subspace, &mut transaction, &tags)
							.map(|()| Response::Unit)
					},
					Request::PutUsers(args) => {
						Self::put_users_with_transaction(db, subspace, &mut transaction, &args)
							.map(|()| Response::Unit)
					},
					Request::TouchCacheEntries(crate::lmdb::TouchCacheEntries {
						ids,
						time_to_touch,
						touched_at,
					}) => Self::touch_cache_entries_with_transaction(
						db,
						subspace,
						&mut transaction,
						&ids,
						touched_at,
						time_to_touch,
					)
					.map(Response::CacheEntries),
					Request::TouchObjects(crate::lmdb::TouchObjects {
						ids,
						time_to_touch,
						touched_at,
					}) => Self::touch_objects_with_transaction(
						db,
						subspace,
						&mut transaction,
						&ids,
						touched_at,
						time_to_touch,
					)
					.map(Response::Objects),
					Request::TouchProcesses(crate::lmdb::TouchProcesses {
						ids,
						time_to_touch,
						touched_at,
					}) => Self::touch_processes_with_transaction(
						db,
						subspace,
						&mut transaction,
						&ids,
						touched_at,
						time_to_touch,
					)
					.map(Response::Processes),
					Request::Update(crate::lmdb::Update { batch_size }) => {
						Self::update_batch_with_transaction(
							db,
							subspace,
							&mut transaction,
							batch_size,
							max_process_depth,
						)
						.map(Response::UpdateOutput)
					},
				};
				results.push(result);
			}

			// Commit the transaction.
			let commit_result = transaction
				.commit()
				.map_err(|error| tg::error!(!error, "failed to commit the transaction"));

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

	pub(super) async fn send_write_request(&self, request: Request) -> tg::Result<Response> {
		let sender = match request.priority() {
			Priority::High => &self.writer_sender_high,
			Priority::Low => &self.writer_sender_low,
			Priority::Medium => &self.writer_sender_medium,
		};
		let Some(sender) = sender else {
			return Err(tg::error!("the writer is unavailable"));
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
			if let Request::Batch(arg) = request {
				let count = arg.items.len();
				if !current_batch.requests.is_empty()
					&& current_count.saturating_add(count) > max_items
				{
					batches.push(current_batch);
					current_batch = Batch {
						requests: Vec::new(),
						tracker_indices: Vec::new(),
					};
					current_count = 0;
				}
				current_batch.requests.push(Request::Batch(arg));
				current_batch.tracker_indices.push(tracker_idx);
				trackers[tracker_idx].remaining = 1;
				current_count = current_count.saturating_add(count);
				if current_count >= max_items {
					batches.push(current_batch);
					current_batch = Batch {
						requests: Vec::new(),
						tracker_indices: Vec::new(),
					};
					current_count = 0;
				}
				continue;
			}

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
			Request::Clean(_) => Response::CleanOutput(crate::clean::Output::default()),
			Request::Batch(_)
			| Request::CompleteFinalization(_)
			| Request::DeleteGrants(_)
			| Request::DeleteGroupMembers(_)
			| Request::DeleteGroups(_)
			| Request::DeleteOrganizationMembers(_)
			| Request::DeleteOrganizations(_)
			| Request::DeleteSandboxes(_)
			| Request::DeleteTags(_)
			| Request::DeleteUsers(_)
			| Request::EnqueueFinalization(_)
			| Request::PutCacheEntries(_)
			| Request::PutGrants(_)
			| Request::PutGroupMembers(_)
			| Request::PutGroups(_)
			| Request::PutObjects(_)
			| Request::PutOrganizationMembers(_)
			| Request::PutOrganizations(_)
			| Request::PutProcesses(_)
			| Request::PutRunners(_)
			| Request::PutSandboxes(_)
			| Request::PutTags(_)
			| Request::PutUsers(_) => Response::Unit,
			Request::TouchCacheEntries(_) => Response::CacheEntries(Vec::new()),
			Request::TouchObjects(_) => Response::Objects(Vec::new()),
			Request::TouchProcesses(_) => Response::Processes(Vec::new()),
			Request::Update(_) => Response::UpdateOutput(crate::update::Output::default()),
		}
	}

	fn request_into_items(request: Request) -> (Vec<Item>, Kind) {
		match request {
			Request::Batch(_) => unreachable!(),
			Request::Clean(crate::lmdb::Clean {
				batch_size,
				max_object_touched_at,
				max_process_touched_at,
				max_sandbox_touched_at,
				now,
			}) => {
				let items = (0..batch_size).map(|_| Item::Clean).collect();
				(
					items,
					Kind::Clean {
						max_object_touched_at,
						max_process_touched_at,
						max_sandbox_touched_at,
						now,
					},
				)
			},
			Request::CompleteFinalization(entry) => (
				vec![Item::CompleteFinalization(entry)],
				Kind::CompleteFinalization,
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
			Request::EnqueueFinalization(item) => (
				vec![Item::EnqueueFinalization(item)],
				Kind::EnqueueFinalization,
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
			Request::PutRunners(args) => {
				let items = args.into_iter().map(Item::PutRunner).collect();
				(items, Kind::PutRunners)
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
			Request::TouchCacheEntries(crate::lmdb::TouchCacheEntries {
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
			Request::TouchObjects(crate::lmdb::TouchObjects {
				ids,
				time_to_touch,
				touched_at,
			}) => {
				let items = ids.into_iter().map(Item::TouchObject).collect();
				(
					items,
					Kind::TouchObjects {
						time_to_touch,
						touched_at,
					},
				)
			},
			Request::TouchProcesses(crate::lmdb::TouchProcesses {
				ids,
				time_to_touch,
				touched_at,
			}) => {
				let items = ids.into_iter().map(Item::TouchProcess).collect();
				(
					items,
					Kind::TouchProcesses {
						time_to_touch,
						touched_at,
					},
				)
			},
			Request::Update(crate::lmdb::Update { batch_size }) => {
				let items = (0..batch_size).map(|_| Item::Update).collect();
				(items, Kind::Update)
			},
		}
	}

	fn request_from_items(items: Vec<Item>, kind: &Kind) -> Request {
		match kind {
			Kind::Clean {
				max_object_touched_at,
				max_process_touched_at,
				max_sandbox_touched_at,
				now,
			} => Request::Clean(crate::lmdb::Clean {
				batch_size: items.len(),
				max_object_touched_at: *max_object_touched_at,
				max_process_touched_at: *max_process_touched_at,
				max_sandbox_touched_at: *max_sandbox_touched_at,
				now: *now,
			}),
			Kind::CompleteFinalization => {
				let items: [Item; 1] = items.try_into().ok().unwrap();
				let [Item::CompleteFinalization(entry)] = items else {
					unreachable!();
				};
				Request::CompleteFinalization(entry)
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
			Kind::EnqueueFinalization => {
				let items: [Item; 1] = items.try_into().ok().unwrap();
				let [Item::EnqueueFinalization(item)] = items else {
					unreachable!();
				};
				Request::EnqueueFinalization(item)
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
			Kind::PutRunners => {
				let args = items
					.into_iter()
					.map(|item| {
						let Item::PutRunner(arg) = item else {
							unreachable!();
						};
						arg
					})
					.collect();
				Request::PutRunners(args)
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
				Request::TouchCacheEntries(crate::lmdb::TouchCacheEntries {
					ids,
					time_to_touch: *time_to_touch,
					touched_at: *touched_at,
				})
			},
			Kind::TouchObjects {
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
				Request::TouchObjects(crate::lmdb::TouchObjects {
					ids,
					time_to_touch: *time_to_touch,
					touched_at: *touched_at,
				})
			},
			Kind::TouchProcesses {
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
				Request::TouchProcesses(crate::lmdb::TouchProcesses {
					ids,
					time_to_touch: *time_to_touch,
					touched_at: *touched_at,
				})
			},
			Kind::Update => Request::Update(crate::lmdb::Update {
				batch_size: items.len(),
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
			(Response::UpdateOutput(existing), Response::UpdateOutput(new)) => {
				existing.merge(new);
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
