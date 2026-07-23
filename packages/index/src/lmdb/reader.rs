use {
	super::{Db, Index},
	foundationdb_tuple as fdbt, heed as lmdb,
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
	pub authorize: super::AuthorizeConfig,
	pub db: Db,
	pub env: lmdb::Env,
	pub read_batch_size: usize,
	pub receiver: Arc<Mutex<crate::read::Receiver>>,
	pub subspace: fdbt::Subspace,
	#[cfg(test)]
	pub test_hook: Option<TestHook>,
}

impl Index {
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
				let response = Self::execute_read_request(
					arg.authorize,
					&arg.db,
					&arg.subspace,
					&transaction,
					request,
				);
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
		authorize: super::AuthorizeConfig,
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		request: crate::read::Request,
	) -> tg::Result<crate::read::Response> {
		let response = match request {
			crate::read::Request::AuthorizeBatch { args, principal } => {
				let output = Self::authorize_batch_with_transaction(
					authorize,
					db,
					subspace,
					transaction,
					&args,
					&principal,
				)?;
				crate::read::Response::AuthorizeBatch(output)
			},
			crate::read::Request::FinalizationBatch {
				batch_size,
				kind,
				partition_end,
				partition_start,
			} => {
				let output = Self::finalization_batch_with_transaction(
					db,
					subspace,
					transaction,
					kind,
					batch_size,
					partition_start,
					partition_end,
				)?;
				crate::read::Response::FinalizationBatch(output)
			},
			crate::read::Request::GetProcessDepthDetections { limit } => {
				let output = Self::get_process_depth_detections_with_transaction(
					db,
					subspace,
					transaction,
					limit,
				)?;
				crate::read::Response::GetProcessDepthDetections(output)
			},
			crate::read::Request::GetRequesterPrincipals { principal } => {
				let output = Self::requester_principals_with_transaction(
					db,
					subspace,
					transaction,
					&principal,
				)?;
				crate::read::Response::GetRequesterPrincipals(output)
			},
			crate::read::Request::GetRunnerSandboxes { runner } => {
				let output = Self::get_runner_sandboxes_with_transaction(
					db,
					subspace,
					transaction,
					&runner,
				)?;
				crate::read::Response::GetRunnerSandboxes(output)
			},
			crate::read::Request::GetSandboxProcesses { sandbox } => {
				let output = Self::get_sandbox_processes_with_transaction(
					db,
					subspace,
					transaction,
					&sandbox,
				)?;
				crate::read::Response::GetSandboxProcesses(output)
			},
			crate::read::Request::GetSchedulerRunners { scheduler } => {
				let output = Self::get_scheduler_runners_with_transaction(
					db,
					subspace,
					transaction,
					&scheduler,
				)?;
				crate::read::Response::GetSchedulerRunners(output)
			},
			crate::read::Request::GetTransactionId => {
				crate::read::Response::GetTransactionId(transaction.id() as u64)
			},
			crate::read::Request::ListSandboxes => {
				let output = Self::list_sandboxes_with_transaction(db, subspace, transaction)?;
				crate::read::Response::ListSandboxes(output)
			},
			crate::read::Request::ListSandboxesForCreator { creator } => {
				let output = Self::list_sandboxes_for_principal_with_transaction(
					db,
					subspace,
					transaction,
					&creator,
					super::Kind::CreatorSandbox,
				)?;
				crate::read::Response::ListSandboxes(output)
			},
			crate::read::Request::ListSandboxesForOwner { owner } => {
				let output = Self::list_sandboxes_for_principal_with_transaction(
					db,
					subspace,
					transaction,
					&owner,
					super::Kind::OwnerSandbox,
				)?;
				crate::read::Response::ListSandboxes(output)
			},
			crate::read::Request::ProcessHasAncestor { ancestor, process } => {
				let output = Self::process_has_ancestor_with_transaction(
					db,
					subspace,
					transaction,
					&process,
					&ancestor,
				)?;
				crate::read::Response::ProcessHasAncestor(output)
			},
			crate::read::Request::TryGetCacheEntries { ids } => {
				let output =
					Self::try_get_cache_entries_with_transaction(db, subspace, transaction, &ids)?;
				crate::read::Response::TryGetCacheEntries(output)
			},
			crate::read::Request::TryGetCachedProcesses { command } => {
				let output = Self::try_get_cached_processes_with_transaction(
					db,
					subspace,
					transaction,
					&command,
				)?;
				crate::read::Response::TryGetCachedProcesses(output)
			},
			crate::read::Request::TryGetObjects { ids } => {
				let output =
					Self::try_get_objects_with_transaction(db, subspace, transaction, &ids)?;
				crate::read::Response::TryGetObjects(output)
			},
			crate::read::Request::TryGetOldestFinalizationTransactionId { kind } => {
				let output = Self::try_get_oldest_finalization_transaction_id_with_transaction(
					db,
					subspace,
					transaction,
					kind,
				)?;
				crate::read::Response::TryGetOldestFinalizationTransactionId(output)
			},
			crate::read::Request::TryGetOldestUpdateTransactionId => {
				let output = Self::try_get_oldest_update_transaction_id_with_transaction(
					db,
					subspace,
					transaction,
				)?;
				crate::read::Response::TryGetOldestUpdateTransactionId(output)
			},
			crate::read::Request::TryGetProcesses { ids } => {
				let output =
					Self::try_get_processes_with_transaction(db, subspace, transaction, &ids)?;
				crate::read::Response::TryGetProcesses(output)
			},
			crate::read::Request::TryGetRunners { ids } => {
				let output =
					Self::try_get_runners_with_transaction(db, subspace, transaction, &ids)?;
				crate::read::Response::TryGetRunners(output)
			},
			crate::read::Request::TryGetSandboxes { ids } => {
				let output =
					Self::try_get_sandboxes_with_transaction(db, subspace, transaction, &ids)?;
				crate::read::Response::TryGetSandboxes(output)
			},
			crate::read::Request::Visible { ids, principal } => {
				let output =
					Self::visible_with_transaction(db, subspace, transaction, &ids, &principal)?;
				crate::read::Response::Visible(output)
			},
		};

		Ok(response)
	}
}
