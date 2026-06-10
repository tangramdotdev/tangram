use {
	super::{
		Db, Index, Request, RequestReceiver, Response, ResponseSender,
		request::{Item, Kind},
	},
	crossbeam_channel as crossbeam, heed as lmdb,
	std::collections::VecDeque,
	tangram_client::prelude::*,
};

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
					Request::Clean(crate::lmdb::Clean {
						batch_size,
						max_object_touched_at,
						max_process_touched_at,
					}) => Self::task_clean(
						db,
						subspace,
						&mut transaction,
						max_object_touched_at,
						max_process_touched_at,
						batch_size,
					)
					.map(Response::CleanOutput),
					Request::DeleteGrants(args) => {
						Self::task_delete_grants(&args).map(|()| Response::Unit)
					},
					Request::DeleteGroupMembers(args) => {
						Self::task_delete_group_members(&args).map(|()| Response::Unit)
					},
					Request::DeleteGroups(ids) => {
						Self::task_delete_groups(&ids).map(|()| Response::Unit)
					},
					Request::DeleteOrganizationMembers(args) => {
						Self::task_delete_organization_members(&args).map(|()| Response::Unit)
					},
					Request::DeleteOrganizations(ids) => {
						Self::task_delete_organizations(&ids).map(|()| Response::Unit)
					},
					Request::DeleteUsers(ids) => {
						Self::task_delete_users(&ids).map(|()| Response::Unit)
					},
					Request::DeleteTags(tags) => {
						Self::task_delete_tags(db, subspace, &mut transaction, &tags)
							.map(|()| Response::Unit)
					},
					Request::PutCacheEntries(args) => {
						Self::task_put_cache_entries(db, subspace, &mut transaction, &args)
							.map(|()| Response::Unit)
					},
					Request::PutGrants(args) => {
						Self::task_put_grants(&args).map(|()| Response::Unit)
					},
					Request::PutGroupMembers(args) => {
						Self::task_put_group_members(&args).map(|()| Response::Unit)
					},
					Request::PutGroups(args) => {
						Self::task_put_groups(&args).map(|()| Response::Unit)
					},
					Request::PutObjects(args) => {
						Self::task_put_objects(db, subspace, &mut transaction, &args)
							.map(|()| Response::Unit)
					},
					Request::PutOrganizationMembers(args) => {
						Self::task_put_organization_members(&args).map(|()| Response::Unit)
					},
					Request::PutOrganizations(args) => {
						Self::task_put_organizations(&args).map(|()| Response::Unit)
					},
					Request::PutProcesses(args) => {
						Self::task_put_processes(db, subspace, &mut transaction, &args)
							.map(|()| Response::Unit)
					},
					Request::PutTags(tags) => {
						Self::task_put_tags(db, subspace, &mut transaction, &tags)
							.map(|()| Response::Unit)
					},
					Request::PutUsers(args) => Self::task_put_users(&args).map(|()| Response::Unit),
					Request::TouchCacheEntries(crate::lmdb::TouchCacheEntries {
						ids,
						time_to_touch,
						touched_at,
					}) => Self::task_touch_cache_entries(
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
					}) => Self::task_touch_objects(
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
					}) => Self::task_touch_processes(
						db,
						subspace,
						&mut transaction,
						&ids,
						touched_at,
						time_to_touch,
					)
					.map(Response::Processes),
					Request::Update(crate::lmdb::Update { batch_size }) => {
						Self::task_update_batch(db, subspace, &mut transaction, batch_size)
							.map(Response::UpdateCount)
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
			Request::Clean(_) => Response::CleanOutput(crate::clean::Output::default()),
			Request::DeleteGrants(_)
			| Request::DeleteGroupMembers(_)
			| Request::DeleteGroups(_)
			| Request::DeleteOrganizationMembers(_)
			| Request::DeleteOrganizations(_)
			| Request::DeleteTags(_)
			| Request::DeleteUsers(_)
			| Request::PutCacheEntries(_)
			| Request::PutGrants(_)
			| Request::PutGroupMembers(_)
			| Request::PutGroups(_)
			| Request::PutObjects(_)
			| Request::PutOrganizationMembers(_)
			| Request::PutOrganizations(_)
			| Request::PutProcesses(_)
			| Request::PutTags(_)
			| Request::PutUsers(_) => Response::Unit,
			Request::TouchCacheEntries(_) => Response::CacheEntries(Vec::new()),
			Request::TouchObjects(_) => Response::Objects(Vec::new()),
			Request::TouchProcesses(_) => Response::Processes(Vec::new()),
			Request::Update(_) => Response::UpdateCount(0),
		}
	}

	fn request_into_items(request: Request) -> (Vec<Item>, Kind) {
		match request {
			Request::Clean(crate::lmdb::Clean {
				batch_size,
				max_object_touched_at,
				max_process_touched_at,
			}) => {
				let items = (0..batch_size).map(|_| Item::Clean).collect();
				(
					items,
					Kind::Clean {
						max_object_touched_at,
						max_process_touched_at,
					},
				)
			},
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
			Request::DeleteTags(tags) => {
				let items = tags.into_iter().map(Item::DeleteTag).collect();
				(items, Kind::DeleteTags)
			},
			Request::DeleteUsers(ids) => {
				let items = ids.into_iter().map(Item::DeleteUser).collect();
				(items, Kind::DeleteUsers)
			},
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
			} => Request::Clean(crate::lmdb::Clean {
				batch_size: items.len(),
				max_object_touched_at: *max_object_touched_at,
				max_process_touched_at: *max_process_touched_at,
			}),
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
