use {
	super::{Db, Store, request::Request},
	heed as lmdb,
	std::collections::VecDeque,
	tangram_client::prelude::*,
};

pub(super) const CHANNEL_CAPACITY: usize = 256;

pub(super) type RequestReceiver = tokio::sync::mpsc::Receiver<(Request, ResponseSender)>;
pub(super) type RequestSender = tokio::sync::mpsc::Sender<(Request, ResponseSender)>;
pub(super) type ResponseSender = tokio::sync::oneshot::Sender<tg::Result<()>>;

pub(super) struct Arg<'a> {
	pub db: &'a Db,
	pub env: &'a lmdb::Env,
	pub receiver: RequestReceiver,
	pub write_batch_size: usize,
}

struct RequestTracker {
	remaining: usize,
	response: tg::Result<()>,
	sender: Option<ResponseSender>,
}

struct Batch {
	requests: Vec<Request>,
	tracker_indices: Vec<usize>,
}

impl Store {
	pub(super) fn writer_task(arg: Arg<'_>) {
		let Arg {
			db,
			env,
			mut receiver,
			write_batch_size,
		} = arg;
		let mut batches = VecDeque::new();
		let mut trackers = Vec::new();

		loop {
			// Drain pending requests into bounded batches.
			batches.extend(Self::create_batches(
				Self::drain_receiver(&mut receiver),
				&mut trackers,
				write_batch_size,
			));

			// Block until a request arrives when there is no queued work.
			if batches.is_empty() {
				let Some(request) = receiver.blocking_recv() else {
					break;
				};
				let mut requests = vec![request];
				requests.extend(Self::drain_receiver(&mut receiver));
				batches.extend(Self::create_batches(
					requests,
					&mut trackers,
					write_batch_size,
				));
			}

			let batch = batches.pop_front().unwrap();

			// Open one transaction for the batch.
			let transaction = env
				.write_txn()
				.map_err(|error| tg::error!(!error, "failed to begin a transaction"));
			let mut transaction = match transaction {
				Ok(transaction) => transaction,
				Err(error) => {
					Self::complete_batch(&mut trackers, &batch.tracker_indices, &Err(error));
					Self::remove_completed_trackers(&mut trackers);
					continue;
				},
			};

			// Execute every request against the shared transaction.
			let mut error = None;
			for request in batch.requests {
				if let Err(current) = Self::execute_request(db, &mut transaction, request)
					&& error.is_none()
				{
					error = Some(current);
				}
			}

			// Commit the transaction and complete the original requests.
			let result = if let Some(error) = error {
				Err(error)
			} else {
				transaction
					.commit()
					.map_err(|error| tg::error!(!error, "failed to commit the transaction"))
			};
			Self::complete_batch(&mut trackers, &batch.tracker_indices, &result);
			Self::remove_completed_trackers(&mut trackers);
		}
	}

	pub(super) async fn send_write_request(&self, request: Request) -> tg::Result<()> {
		let Some(sender) = &self.writer_sender else {
			return Err(tg::error!("the writer is unavailable"));
		};
		let (response_sender, response_receiver) = tokio::sync::oneshot::channel();
		sender
			.send((request, response_sender))
			.await
			.map_err(|error| tg::error!(!error, "failed to send the write request"))?;
		response_receiver
			.await
			.map_err(|error| tg::error!(!error, "failed to receive the write response"))??;

		Ok(())
	}

	fn drain_receiver(receiver: &mut RequestReceiver) -> Vec<(Request, ResponseSender)> {
		let mut requests = Vec::new();
		while let Ok(request) = receiver.try_recv() {
			requests.push(request);
		}

		requests
	}

	fn create_batches(
		requests: Vec<(Request, ResponseSender)>,
		trackers: &mut Vec<RequestTracker>,
		write_batch_size: usize,
	) -> Vec<Batch> {
		let mut batches = Vec::new();
		let mut current = Batch {
			requests: Vec::new(),
			tracker_indices: Vec::new(),
		};
		let mut current_size: usize = 0;

		for (request, sender) in requests {
			let tracker_index = trackers.len();
			trackers.push(RequestTracker {
				remaining: 0,
				response: Ok(()),
				sender: Some(sender),
			});
			for (request, size) in Self::split_request(request, write_batch_size) {
				if !current.requests.is_empty()
					&& current_size.saturating_add(size) > write_batch_size
				{
					batches.push(current);
					current = Batch {
						requests: Vec::new(),
						tracker_indices: Vec::new(),
					};
					current_size = 0;
				}
				current.requests.push(request);
				current.tracker_indices.push(tracker_index);
				trackers[tracker_index].remaining += 1;
				current_size = current_size.saturating_add(size);
				if current_size >= write_batch_size {
					batches.push(current);
					current = Batch {
						requests: Vec::new(),
						tracker_indices: Vec::new(),
					};
					current_size = 0;
				}
			}
		}

		if !current.requests.is_empty() {
			batches.push(current);
		}

		batches
	}

	fn split_request(request: Request, write_batch_size: usize) -> Vec<(Request, usize)> {
		match request {
			Request::DeleteLog(arg) => vec![(Request::DeleteLog(arg), 1)],
			Request::DeleteObject(request) => vec![(Request::DeleteObject(request), 1)],
			Request::DeleteObjectArchiveOutboxEntries(arg) => {
				Self::split_items(arg.entries, write_batch_size, |entries| {
					Request::DeleteObjectArchiveOutboxEntries(
						crate::object::archive::outbox::delete::Arg { entries },
					)
				})
			},
			Request::DeleteObjectBatch(requests) => {
				Self::split_items(requests, write_batch_size, Request::DeleteObjectBatch)
			},
			Request::DeleteObjectIndexOutboxFragments(arg) => {
				Self::split_items(arg.fragments, write_batch_size, |fragments| {
					Request::DeleteObjectIndexOutboxFragments(
						crate::object::index::outbox::fragment::delete::Arg { fragments },
					)
				})
			},
			Request::EnqueueObjectIndexOutboxBatch(batch) => {
				let size = batch.fragments.len().max(1);
				vec![(Request::EnqueueObjectIndexOutboxBatch(batch), size)]
			},
			Request::PutLogBatch(args) => {
				Self::split_items(args, write_batch_size, Request::PutLogBatch)
			},
			Request::PutObject(request) => vec![(Request::PutObject(request), 1)],
			Request::PutObjectArchiveOutboxEntries(arg) => {
				Self::split_items(arg.entries, write_batch_size, |entries| {
					Request::PutObjectArchiveOutboxEntries(
						crate::object::archive::outbox::put::Arg { entries },
					)
				})
			},
			Request::PutObjectBatch(requests) => {
				Self::split_items(requests, write_batch_size, Request::PutObjectBatch)
			},
		}
	}

	fn split_items<T>(
		items: Vec<T>,
		write_batch_size: usize,
		create_request: impl Fn(Vec<T>) -> Request,
	) -> Vec<(Request, usize)> {
		let mut items = items.into_iter();
		let mut requests = Vec::new();
		loop {
			let items = items.by_ref().take(write_batch_size).collect::<Vec<_>>();
			if items.is_empty() {
				break;
			}
			let size = items.len();
			requests.push((create_request(items), size));
		}
		if requests.is_empty() {
			requests.push((create_request(Vec::new()), 1));
		}

		requests
	}

	fn execute_request(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		request: Request,
	) -> tg::Result<()> {
		match request {
			Request::DeleteLog(arg) => Self::delete_log_with_transaction(db, transaction, &arg),
			Request::DeleteObject(request) => {
				Self::delete_inner_with_transaction(db, transaction, request)
			},
			Request::DeleteObjectArchiveOutboxEntries(arg) => {
				Self::delete_object_archive_outbox_entries_with_transaction(db, transaction, arg)
			},
			Request::DeleteObjectBatch(requests) => requests.into_iter().try_for_each(|request| {
				Self::delete_inner_with_transaction(db, transaction, request)
			}),
			Request::DeleteObjectIndexOutboxFragments(arg) => {
				Self::delete_object_index_outbox_fragments_with_transaction(db, transaction, arg)
			},
			Request::EnqueueObjectIndexOutboxBatch(batch) => {
				Self::enqueue_object_index_outbox_batch_with_transaction(db, transaction, batch)
			},
			Request::PutLogBatch(args) => args
				.iter()
				.try_for_each(|arg| Self::put_log_with_transaction(db, transaction, arg)),
			Request::PutObject(request) => {
				Self::put_inner_with_transaction(db, transaction, request)
			},
			Request::PutObjectArchiveOutboxEntries(arg) => {
				Self::put_object_archive_outbox_entries_with_transaction(db, transaction, arg)
			},
			Request::PutObjectBatch(requests) => requests
				.into_iter()
				.try_for_each(|request| Self::put_inner_with_transaction(db, transaction, request)),
		}
	}

	fn complete_batch(
		trackers: &mut [RequestTracker],
		tracker_indices: &[usize],
		result: &tg::Result<()>,
	) {
		for &tracker_index in tracker_indices {
			Self::complete_tracker(&mut trackers[tracker_index], result.clone());
		}
	}

	fn complete_tracker(tracker: &mut RequestTracker, result: tg::Result<()>) {
		tracker.remaining -= 1;
		if tracker.response.is_ok() && result.is_err() {
			tracker.response = result;
		}
		if tracker.remaining == 0 {
			let response = std::mem::replace(&mut tracker.response, Ok(()));
			tracker.sender.take().unwrap().send(response).ok();
		}
	}

	fn remove_completed_trackers(trackers: &mut Vec<RequestTracker>) {
		while trackers
			.last()
			.is_some_and(|tracker| tracker.remaining == 0 && tracker.sender.is_none())
		{
			trackers.pop();
		}
	}
}
