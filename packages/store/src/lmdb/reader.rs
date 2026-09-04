use {
	super::{Db, Store},
	heed as lmdb,
	std::sync::{Arc, Mutex},
	tangram_client::prelude::*,
};

#[cfg(test)]
pub(super) struct TestHook {
	pub continue_receiver: std::sync::mpsc::Receiver<()>,
	pub started_sender: std::sync::mpsc::Sender<()>,
	pub transactions: Arc<std::sync::atomic::AtomicUsize>,
}

pub(super) struct Arg {
	pub db: Db,
	pub env: lmdb::Env,
	pub read_batch_size: usize,
	pub receiver: Arc<Mutex<crate::read::Receiver>>,
	#[cfg(test)]
	pub test_hook: Option<TestHook>,
}

impl Store {
	pub(super) fn reader_task(arg: &Arg) {
		loop {
			// Freeze the batch before opening its transaction.
			let Some(requests) = Self::receive_read_batch(&arg.receiver, arg.read_batch_size)
			else {
				break;
			};
			let requests = requests
				.into_iter()
				.filter(|(_, sender)| !sender.is_closed())
				.collect::<Vec<_>>();
			if requests.is_empty() {
				continue;
			}

			// Open one transaction for the entire batch.
			let transaction = arg
				.env
				.read_txn()
				.map_err(|error| tg::error!(!error, "failed to begin a transaction"));
			let transaction = match transaction {
				Ok(transaction) => transaction,
				Err(error) => {
					for (_, sender) in requests {
						sender.send(Err(error.clone())).ok();
					}
					continue;
				},
			};
			#[cfg(test)]
			if let Some(test_hook) = &arg.test_hook {
				let transaction_index = test_hook
					.transactions
					.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
				if transaction_index == 0 {
					test_hook.started_sender.send(()).unwrap();
					test_hook.continue_receiver.recv().unwrap();
				}
			}

			// Execute the requests sequentially against the shared snapshot.
			for (request, sender) in requests {
				let response = Self::execute_read_request(&arg.db, &transaction, request);
				sender.send(response).ok();
			}
		}
	}

	pub(super) async fn send_read_request(
		&self,
		request: crate::read::Request,
	) -> tg::Result<crate::read::Response> {
		let (sender, receiver) = tokio::sync::oneshot::channel();
		self.reader_sender
			.as_ref()
			.unwrap()
			.send((request, sender))
			.await
			.map_err(|error| tg::error!(!error, "failed to send the read request"))?;
		let response = receiver
			.await
			.map_err(|error| tg::error!(!error, "failed to receive the read response"))??;

		Ok(response)
	}

	fn receive_read_batch(
		receiver: &Mutex<crate::read::Receiver>,
		read_batch_size: usize,
	) -> Option<Vec<(crate::read::Request, crate::read::ResponseSender)>> {
		let mut receiver = receiver.lock().unwrap();
		let request = receiver.blocking_recv()?;
		let mut requests = Vec::with_capacity(read_batch_size);
		requests.push(request);
		while requests.len() < read_batch_size {
			let Ok(request) = receiver.try_recv() else {
				break;
			};
			requests.push(request);
		}

		Some(requests)
	}

	fn execute_read_request(
		db: &Db,
		transaction: &lmdb::RoTxn<'_>,
		request: crate::read::Request,
	) -> tg::Result<crate::read::Response> {
		let response = match request {
			crate::read::Request::GetIndexers => {
				let output = Self::get_indexers_with_transaction(db, transaction)?;
				crate::read::Response::GetIndexers(output)
			},
			crate::read::Request::GetObjectCacheEntries(arg) => {
				let output =
					Self::get_object_cache_entries_with_transaction(db, transaction, &arg)?;
				crate::read::Response::GetObjectCacheEntries(output)
			},
			#[cfg(test)]
			crate::read::Request::GetTransactionId => {
				crate::read::Response::GetTransactionId(transaction.id() as u64)
			},
			crate::read::Request::TryGetLogLength(arg) => {
				let output = Self::try_get_log_length_with_transaction(db, transaction, &arg)?;
				crate::read::Response::TryGetLogLength(output)
			},
			crate::read::Request::TryGetIndexer(arg) => {
				let output = Self::try_get_indexer_with_transaction(db, transaction, &arg)?;
				crate::read::Response::TryGetIndexer(output)
			},
			crate::read::Request::TryGetObject(arg) => {
				let output = Self::try_get_object_with_arg_with_transaction(db, transaction, &arg)?;
				crate::read::Response::TryGetObject(output)
			},
			crate::read::Request::TryGetObjectArchiveQueueEntry(arg) => {
				let output = Self::try_get_object_archive_queue_entry_with_transaction(
					db,
					transaction,
					&arg,
				)?;
				crate::read::Response::TryGetObjectArchiveQueueEntry(output)
			},
			crate::read::Request::TryGetObjectBatch(arg) => {
				let output = Self::try_get_object_batch_with_transaction(db, transaction, &arg)?;
				crate::read::Response::TryGetObjectBatch(output)
			},
			crate::read::Request::TryGetObjectIndexQueueFragment(arg) => {
				let output = Self::try_get_object_index_queue_fragment_with_transaction(
					db,
					transaction,
					&arg,
				)?;
				crate::read::Response::TryGetObjectIndexQueueFragment(output)
			},
			crate::read::Request::TryReadLog(arg) => {
				let output = Self::try_read_log_with_transaction(db, transaction, &arg)?;
				crate::read::Response::TryReadLog(output)
			},
		};

		Ok(response)
	}
}
