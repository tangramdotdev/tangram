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
	pub db: Db,
	pub env: lmdb::Env,
	pub read_request_batch_size: usize,
	pub receiver: Arc<Mutex<tangram_index::read::Receiver>>,
	pub subspace: fdbt::Subspace,
	#[cfg(test)]
	pub test_hook: Option<TestHook>,
}

impl Index {
	pub(super) fn reader_task(arg: &Arg) {
		loop {
			// Freeze the batch before opening its transaction.
			let Some(requests) =
				Self::receive_read_batch(&arg.receiver, arg.read_request_batch_size)
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
			let verification_fact_cache = tangram_index::verify::facts::Cache::new();
			for (request, sender) in requests {
				let response = Self::execute_read_request(
					verification_fact_cache.clone(),
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
		request: tangram_index::read::Request,
	) -> tg::Result<tangram_index::read::Response> {
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
		receiver: &Mutex<tangram_index::read::Receiver>,
		read_request_batch_size: usize,
	) -> Option<
		Vec<(
			tangram_index::read::Request,
			tangram_index::read::ResponseSender,
		)>,
	> {
		let mut receiver = receiver.lock().unwrap();
		let request = receiver.blocking_recv()?;
		let mut requests = Vec::with_capacity(read_request_batch_size);
		requests.push(request);
		while requests.len() < read_request_batch_size {
			let Ok(request) = receiver.try_recv() else {
				break;
			};
			requests.push(request);
		}

		Some(requests)
	}

	fn execute_read_request(
		verification_fact_cache: tangram_index::verify::facts::Cache<std::convert::Infallible>,
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		request: tangram_index::read::Request,
	) -> tg::Result<tangram_index::read::Response> {
		let response = match request {
			tangram_index::read::Request::VerifyBatch {
				args,
				config,
				principal,
			} => {
				let output = Self::verify_batch_with_transaction(
					verification_fact_cache,
					config,
					db,
					subspace,
					transaction,
					&args,
					&principal,
				)?;
				tangram_index::read::Response::VerifyBatch(output)
			},
			tangram_index::read::Request::PermissionCaptureBatch {
				batch_size,
				partition_end,
				partition_start,
			} => {
				let entries = Self::permission_capture_batch_with_transaction(
					db,
					subspace,
					transaction,
					batch_size,
					partition_start,
					partition_end,
				)?;
				tangram_index::read::Response::PermissionCaptureBatch(entries)
			},
			tangram_index::read::Request::ContainsIds { ids } => {
				let output = Self::contains_ids_with_transaction(db, subspace, transaction, &ids)?;
				tangram_index::read::Response::ContainsIds(output)
			},
			tangram_index::read::Request::GetIndexers => {
				let output = Self::get_indexers_with_transaction(db, subspace, transaction)?;
				tangram_index::read::Response::GetIndexers(output)
			},
			tangram_index::read::Request::TryGetProcessChildrenCount { id } => {
				let output = Self::try_get_process_children_count_with_transaction(
					db,
					subspace,
					transaction,
					&id,
				)?;
				tangram_index::read::Response::TryGetProcessChildrenCount(output)
			},
			tangram_index::read::Request::TryGetSandboxProcessesCount { id } => {
				let output = Self::try_get_sandbox_processes_count_with_transaction(
					db,
					subspace,
					transaction,
					&id,
				)?;
				tangram_index::read::Response::TryGetSandboxProcessesCount(output)
			},
			tangram_index::read::Request::TryGetProcessChildren {
				id,
				length,
				position,
			} => {
				let output = Self::try_get_process_children_page_with_transaction(
					db,
					subspace,
					transaction,
					&id,
					position,
					length,
				)?;
				tangram_index::read::Response::TryGetProcessChildren(output)
			},
			tangram_index::read::Request::TryGetSandboxProcesses {
				id,
				length,
				position,
			} => {
				let output = Self::try_get_sandbox_processes_page_with_transaction(
					db,
					subspace,
					transaction,
					&id,
					position,
					length,
				)?;
				tangram_index::read::Response::TryGetSandboxProcesses(output)
			},
			tangram_index::read::Request::TryGetProcessChildrenAndObjects { id } => {
				let output = Self::try_get_process_children_and_objects_with_transaction(
					db,
					subspace,
					transaction,
					&id,
				)?;
				tangram_index::read::Response::TryGetProcessChildrenAndObjects(output)
			},
			tangram_index::read::Request::GetRequesterSubjects { principal } => {
				let output = Self::requester_subjects_with_transaction(
					db,
					subspace,
					transaction,
					&principal,
				)?;
				tangram_index::read::Response::GetRequesterSubjects(output)
			},
			tangram_index::read::Request::GetRunnerSandboxes { runner } => {
				let output = Self::get_runner_sandboxes_with_transaction(
					db,
					subspace,
					transaction,
					&runner,
				)?;
				tangram_index::read::Response::GetRunnerSandboxes(output)
			},

			tangram_index::read::Request::GetTransactionId => {
				tangram_index::read::Response::GetTransactionId(transaction.id() as u64)
			},
			tangram_index::read::Request::ListSandboxes => {
				let output = Self::list_sandboxes_with_transaction(db, subspace, transaction)?;
				tangram_index::read::Response::ListSandboxes(output)
			},
			tangram_index::read::Request::ListSandboxesForCreator { creator } => {
				let output = Self::list_sandboxes_for_principal_with_transaction(
					db,
					subspace,
					transaction,
					&creator,
					super::Kind::CreatorSandbox,
				)?;
				tangram_index::read::Response::ListSandboxes(output)
			},
			tangram_index::read::Request::ListSandboxesForOwner { owner } => {
				let output = Self::list_sandboxes_for_principal_with_transaction(
					db,
					subspace,
					transaction,
					&owner,
					super::Kind::OwnerSandbox,
				)?;
				tangram_index::read::Response::ListSandboxes(output)
			},
			tangram_index::read::Request::ProcessHasAncestor { ancestor, process } => {
				let output = Self::process_has_ancestor_with_transaction(
					db,
					subspace,
					transaction,
					&process,
					&ancestor,
				)?;
				tangram_index::read::Response::ProcessHasAncestor(output)
			},
			tangram_index::read::Request::TryGetAncestors { id } => {
				let output =
					Self::try_get_ancestors_with_transaction(db, subspace, transaction, &id)?;
				tangram_index::read::Response::TryGetAncestors(output)
			},
			tangram_index::read::Request::TryGetCheckouts { ids } => {
				let output =
					Self::try_get_checkouts_with_transaction(db, subspace, transaction, &ids)?;
				tangram_index::read::Response::TryGetCheckouts(output)
			},
			tangram_index::read::Request::TryGetCachedProcesses { command } => {
				let output = Self::try_get_cached_processes_with_transaction(
					db,
					subspace,
					transaction,
					&command,
				)?;
				tangram_index::read::Response::TryGetCachedProcesses(output)
			},
			tangram_index::read::Request::TryGetGroups { ids } => {
				let output =
					Self::try_get_groups_with_transaction(db, subspace, transaction, &ids)?;
				tangram_index::read::Response::TryGetGroups(output)
			},
			tangram_index::read::Request::TryGetIdsForSpecifiers { specifiers } => {
				let output = Self::try_get_ids_for_specifiers_with_transaction(
					db,
					subspace,
					transaction,
					&specifiers,
				)?;
				tangram_index::read::Response::TryGetIdsForSpecifiers(output)
			},
			tangram_index::read::Request::TryGetIndexer(arg) => {
				let output =
					Self::try_get_indexer_with_transaction(db, subspace, transaction, &arg)?;
				tangram_index::read::Response::TryGetIndexer(output)
			},
			tangram_index::read::Request::TryGetObjectChildren { id } => {
				let output =
					Self::try_get_object_children_with_transaction(db, subspace, transaction, &id)?;
				tangram_index::read::Response::TryGetObjectChildren(output)
			},
			tangram_index::read::Request::TryGetObjects { ids } => {
				let output =
					Self::try_get_objects_with_transaction(db, subspace, transaction, &ids)?;
				tangram_index::read::Response::TryGetObjects(output)
			},
			tangram_index::read::Request::TryGetOldestUpdateTransactionId { kind } => {
				let output = Self::try_get_oldest_update_transaction_id_with_transaction(
					db,
					subspace,
					transaction,
					kind,
				)?;
				tangram_index::read::Response::TryGetOldestUpdateTransactionId(output)
			},
			tangram_index::read::Request::TryGetOrganizations { ids } => {
				let output =
					Self::try_get_organizations_with_transaction(db, subspace, transaction, &ids)?;
				tangram_index::read::Response::TryGetOrganizations(output)
			},
			tangram_index::read::Request::TryGetProcesses { ids } => {
				let output =
					Self::try_get_processes_with_transaction(db, subspace, transaction, &ids)?;
				tangram_index::read::Response::TryGetProcesses(output)
			},
			tangram_index::read::Request::TryGetSandboxes { ids } => {
				let output =
					Self::try_get_sandboxes_with_transaction(db, subspace, transaction, &ids)?;
				tangram_index::read::Response::TryGetSandboxes(output)
			},
			tangram_index::read::Request::TryGetSpecifiersForIds { ids } => {
				let output = Self::try_get_specifiers_for_ids_with_transaction(
					db,
					subspace,
					transaction,
					&ids,
				)?;
				tangram_index::read::Response::TryGetSpecifiersForIds(output)
			},
			tangram_index::read::Request::TryGetTags { ids } => {
				let output = Self::try_get_tags_with_transaction(db, subspace, transaction, &ids)?;
				tangram_index::read::Response::TryGetTags(output)
			},
			tangram_index::read::Request::TryGetUsers { ids } => {
				let output = Self::try_get_users_with_transaction(db, subspace, transaction, &ids)?;
				tangram_index::read::Response::TryGetUsers(output)
			},
			tangram_index::read::Request::Visible { ids, principal } => {
				let output =
					Self::visible_with_transaction(db, subspace, transaction, &ids, &principal)?;
				tangram_index::read::Response::Visible(output)
			},
		};

		Ok(response)
	}
}
