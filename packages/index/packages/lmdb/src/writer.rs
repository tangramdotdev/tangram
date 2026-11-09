use {
	super::{
		Db, Index, Request, Response,
		request::{Kind, Operation, Priority},
	},
	crossbeam_channel as crossbeam, foundationdb_tuple as fdbt, heed as lmdb,
	std::collections::VecDeque,
	tangram_client::prelude::*,
};

pub(super) type RequestReceiver = crossbeam::Receiver<(Request, ResponseSender)>;
pub(super) type RequestSender = crossbeam::Sender<(Request, ResponseSender)>;
pub(super) type ResponseSender = tokio::sync::oneshot::Sender<tg::Result<Response>>;

#[derive(Clone, Copy)]
pub(super) struct Arg<'a> {
	pub db: &'a Db,
	pub env: &'a lmdb::Env,
	pub max_process_depth: Option<u64>,
	pub object_permission_time_to_touch: std::time::Duration,
	pub process_data_fragment_size: usize,
	pub process_permission_time_to_touch: std::time::Duration,
	pub receiver_high: &'a RequestReceiver,
	pub receiver_low: &'a RequestReceiver,
	pub receiver_medium: &'a RequestReceiver,
	pub subspace: &'a fdbt::Subspace,
	pub usage_partition_total: u64,
	pub write_operation_batch_size: usize,
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
			object_permission_time_to_touch,
			process_data_fragment_size,
			process_permission_time_to_touch,
			receiver_high,
			receiver_low,
			receiver_medium,
			subspace,
			usage_partition_total,
			write_operation_batch_size,
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
				write_operation_batch_size,
			));

			// Drain medium-priority requests.
			queue_medium.extend(Self::create_batches(
				Self::drain_receiver(receiver_medium),
				&mut trackers,
				write_operation_batch_size,
			));

			// If all queues are empty, try low-priority requests.
			if queue_high.is_empty() && queue_medium.is_empty() && queue_low.is_empty() {
				queue_low.extend(Self::create_batches(
					Self::drain_receiver(receiver_low),
					&mut trackers,
					write_operation_batch_size,
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
								write_operation_batch_size,
							));
						}
					},
					recv(receiver_medium) -> result => {
						if let Ok(item) = result {
							queue_medium.extend(Self::create_batches(
								vec![item],
								&mut trackers,
								write_operation_batch_size,
							));
						}
					},
					recv(receiver_low) -> result => {
						if let Ok(item) = result {
							queue_low.extend(Self::create_batches(
								vec![item],
								&mut trackers,
								write_operation_batch_size,
							));
						}
					},
				}

				// After waking, drain all receivers.
				queue_high.extend(Self::create_batches(
					Self::drain_receiver(receiver_high),
					&mut trackers,
					write_operation_batch_size,
				));
				queue_medium.extend(Self::create_batches(
					Self::drain_receiver(receiver_medium),
					&mut trackers,
					write_operation_batch_size,
				));
				queue_low.extend(Self::create_batches(
					Self::drain_receiver(receiver_low),
					&mut trackers,
					write_operation_batch_size,
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
					Request::NextRunnerRegistrationIndex(runner) => {
						Self::next_runner_registration_index_with_transaction(
							db,
							subspace,
							&mut transaction,
							&runner,
						)
						.map(Response::RunnerRegistrationIndex)
					},
					Request::CompletePermissionCapture(entry) => {
						Self::complete_permission_capture_with_transaction(
							db,
							subspace,
							&mut transaction,
							&entry,
						)
						.map(|()| Response::Unit)
					},
					Request::AggregateUsage(arg) => {
						Self::aggregate_usage_with_transaction(db, subspace, &mut transaction, &arg)
							.map(Response::AggregateUsageOutput)
					},
					Request::Write(arg) => Self::write_with_transaction(
						db,
						subspace,
						&mut transaction,
						&arg,
						process_data_fragment_size,
						usage_partition_total,
					)
					.map(Response::Mutation),
					Request::Clean(crate::Clean {
						batch_size,
						max_object_touched_at,
						max_process_touched_at,
						max_sandbox_touched_at,
						now,
					}) => Self::clean_with_transaction(crate::clean::TransactionArg {
						batch_size,
						db,
						max_object_touched_at,
						max_process_touched_at,
						max_sandbox_touched_at,
						now,
						subspace,
						usage_partition_total,
						transaction: &mut transaction,
					})
					.map(Response::CleanOutput),
					Request::ExpireUsage(arg) => Self::expire_usage_with_transaction(
						db,
						subspace,
						&mut transaction,
						&arg,
						usage_partition_total,
					)
					.map(Response::ExpireUsageOutput),
					Request::DeleteIndexer(arg) => {
						Self::delete_indexer_with_transaction(db, subspace, &mut transaction, &arg)
							.map(|()| Response::Unit)
					},
					Request::DeleteScheduler(arg) => Self::delete_scheduler_with_transaction(
						db,
						subspace,
						&mut transaction,
						&arg,
					)
					.map(|()| Response::Unit),
					Request::GetUsage {
						account,
						delta_time_to_live,
						now,
						period,
					} => Self::get_usage_with_transaction(
						db,
						subspace,
						&mut transaction,
						&account,
						period,
						now,
						delta_time_to_live,
						usage_partition_total,
					)
					.map(Response::Usage),
					Request::PutIndexer(arg) => {
						Self::put_indexer_with_transaction(db, subspace, &mut transaction, &arg)
							.map(|()| Response::Unit)
					},
					Request::PutScheduler(arg) => {
						Self::put_scheduler_with_transaction(db, subspace, &mut transaction, &arg)
							.map(|()| Response::Unit)
					},
					Request::TouchCheckouts(crate::TouchCheckouts {
						ids,
						time_to_touch,
						touched_at,
					}) => Self::touch_checkouts_with_transaction(
						db,
						subspace,
						&mut transaction,
						&ids,
						touched_at,
						time_to_touch,
					)
					.map(Response::Checkouts),
					Request::TouchObjects(crate::TouchObjects {
						account,
						ids,
						time_to_touch,
						touched_at,
					}) => Self::touch_objects_with_account_with_transaction(
						db,
						subspace,
						&mut transaction,
						&ids,
						account.as_ref(),
						touched_at,
						time_to_touch,
					)
					.map(Response::Objects),
					Request::TouchProcesses(arg) => {
						Self::touch_processes_with_account_with_transaction(
							db,
							subspace,
							&mut transaction,
							&arg,
							usage_partition_total,
						)
						.map(Response::Processes)
					},
					Request::Update(crate::Update { batch_size, kind }) => {
						Self::update_batch_with_transaction(
							db,
							subspace,
							&mut transaction,
							batch_size,
							kind,
							max_process_depth,
							object_permission_time_to_touch,
							process_permission_time_to_touch,
							usage_partition_total,
						)
						.map(Response::UpdateOutput)
					},
					Request::UpdateIndexer(arg) => {
						Self::update_indexer_with_transaction(db, subspace, &mut transaction, &arg)
							.map(|()| Response::Unit)
					},
					Request::UpdateScheduler(arg) => Self::update_scheduler_with_transaction(
						db,
						subspace,
						&mut transaction,
						&arg,
					)
					.map(|()| Response::Unit),
				};
				let failed = result.is_err();
				results.push(result);
				if failed {
					break;
				}
			}

			// Abort failed transactions so a commit error does not hide the original failure.
			let commit_result = if let Some(Err(error)) = results.last() {
				drop(transaction);
				Err(error.clone())
			} else {
				transaction
					.commit()
					.map_err(|error| tg::error!(!error, "failed to commit the transaction"))
			};

			// Merge the results into the trackers and send completed responses.
			let mut results = results.into_iter();
			for tracker_index in &batch.tracker_indices {
				let result = match commit_result {
					Err(ref error) => Err(error.clone()),
					Ok(()) => results.next().unwrap(),
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
			let operation_count = match &request {
				Request::Write(arg) => Some(arg.items.len()),
				Request::CompletePermissionCapture(_)
				| Request::DeleteIndexer(_)
				| Request::DeleteScheduler(_)
				| Request::NextRunnerRegistrationIndex(_)
				| Request::PutIndexer(_)
				| Request::PutScheduler(_)
				| Request::UpdateIndexer(_)
				| Request::UpdateScheduler(_) => Some(1),
				_ => None,
			};
			if let Some(count) = operation_count {
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
				current_batch.requests.push(request);
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
					.push(Self::request_from_operations(chunk, &kind));
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
			Request::NextRunnerRegistrationIndex(_) => Response::RunnerRegistrationIndex(0),
			Request::AggregateUsage(_) => {
				Response::AggregateUsageOutput(tangram_index::usage::aggregate::Output::default())
			},
			Request::Clean(_) => Response::CleanOutput(tangram_index::clean::Output::default()),
			Request::ExpireUsage(_) => {
				Response::ExpireUsageOutput(tangram_index::usage::expire::Output::default())
			},
			Request::Write(_) => Response::Mutation(Ok(())),
			Request::CompletePermissionCapture(_)
			| Request::DeleteIndexer(_)
			| Request::DeleteScheduler(_)
			| Request::PutIndexer(_)
			| Request::PutScheduler(_)
			| Request::UpdateIndexer(_)
			| Request::UpdateScheduler(_) => Response::Unit,
			Request::GetUsage { .. } => Response::Usage(tangram_index::usage::Aggregate::default()),
			Request::TouchCheckouts(_) => Response::Checkouts(Vec::new()),
			Request::TouchObjects(_) => Response::Objects(Vec::new()),
			Request::TouchProcesses(_) => Response::Processes(Vec::new()),
			Request::Update(_) => Response::UpdateOutput(tangram_index::update::Output::default()),
		}
	}

	fn request_into_operations(request: Request) -> (Vec<Operation>, Kind) {
		match request {
			Request::AggregateUsage(arg) => {
				(vec![Operation::AggregateUsage], Kind::AggregateUsage(arg))
			},
			Request::Write(_)
			| Request::CompletePermissionCapture(_)
			| Request::DeleteIndexer(_)
			| Request::DeleteScheduler(_)
			| Request::NextRunnerRegistrationIndex(_)
			| Request::PutIndexer(_)
			| Request::PutScheduler(_)
			| Request::UpdateIndexer(_)
			| Request::UpdateScheduler(_) => unreachable!(),
			Request::Clean(crate::Clean {
				batch_size,
				max_object_touched_at,
				max_process_touched_at,
				max_sandbox_touched_at,
				now,
			}) => {
				let items = (0..batch_size).map(|_| Operation::Clean).collect();
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
			Request::ExpireUsage(arg) => (vec![Operation::ExpireUsage], Kind::ExpireUsage(arg)),
			Request::GetUsage {
				account,
				delta_time_to_live,
				now,
				period,
			} => (
				vec![Operation::GetUsage],
				Kind::GetUsage {
					account,
					delta_time_to_live,
					now,
					period,
				},
			),
			Request::TouchCheckouts(crate::TouchCheckouts {
				ids,
				time_to_touch,
				touched_at,
			}) => {
				let items = ids.into_iter().map(Operation::TouchCheckout).collect();
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
				let items = ids.into_iter().map(Operation::TouchObject).collect();
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
				let items = ids.into_iter().map(Operation::TouchProcess).collect();
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
			Request::Update(crate::Update { batch_size, kind }) => {
				let items = (0..batch_size).map(|_| Operation::Update).collect();
				(items, Kind::Update { kind })
			},
		}
	}

	fn request_from_operations(items: Vec<Operation>, kind: &Kind) -> Request {
		match kind {
			Kind::AggregateUsage(arg) => {
				let items: [Operation; 1] = items.try_into().ok().unwrap();
				let [Operation::AggregateUsage] = items else {
					unreachable!();
				};
				Request::AggregateUsage(arg.clone())
			},
			Kind::Clean {
				max_object_touched_at,
				max_process_touched_at,
				max_sandbox_touched_at,
				now,
			} => Request::Clean(crate::Clean {
				batch_size: items.len(),
				max_object_touched_at: *max_object_touched_at,
				max_process_touched_at: *max_process_touched_at,
				max_sandbox_touched_at: *max_sandbox_touched_at,
				now: *now,
			}),
			Kind::ExpireUsage(arg) => {
				let items: [Operation; 1] = items.try_into().ok().unwrap();
				let [Operation::ExpireUsage] = items else {
					unreachable!();
				};
				Request::ExpireUsage(arg.clone())
			},
			Kind::GetUsage {
				account,
				delta_time_to_live,
				now,
				period,
			} => {
				let items: [Operation; 1] = items.try_into().ok().unwrap();
				let [Operation::GetUsage] = items else {
					unreachable!();
				};
				Request::GetUsage {
					account: account.clone(),
					delta_time_to_live: *delta_time_to_live,
					now: *now,
					period: *period,
				}
			},
			Kind::TouchCheckouts {
				time_to_touch,
				touched_at,
			} => {
				let ids = items
					.into_iter()
					.map(|item| match item {
						Operation::TouchCheckout(id) => id,
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
						Operation::TouchObject(id) => id,
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
						Operation::TouchProcess(id) => id,
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
			Kind::Update { kind } => Request::Update(crate::Update {
				batch_size: items.len(),
				kind: *kind,
			}),
		}
	}

	fn merge_response(target: &mut tg::Result<Response>, source: Response) {
		let Ok(target) = target else {
			return;
		};
		match (target, source) {
			(
				Response::RunnerRegistrationIndex(existing),
				Response::RunnerRegistrationIndex(new),
			) => {
				*existing = new;
			},
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
				existing.checkout_bytes += new.checkout_bytes;
				existing.checkout_files += new.checkout_files;
				existing.checkouts.extend(new.checkouts);
				existing.object_bytes += new.object_bytes;
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
