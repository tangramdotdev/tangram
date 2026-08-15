use {
	super::Index,
	foundationdb as fdb, foundationdb_tuple as fdbt,
	futures::{
		StreamExt as _,
		stream::{self, FuturesUnordered},
	},
	std::{ops::ControlFlow, sync::Arc},
	tangram_client::prelude::*,
};

pub(super) struct Arg {
	pub authorize: super::AuthorizeConfig,
	pub database: Arc<fdb::Database>,
	pub partition_total: u64,
	pub read_batch_size: usize,
	pub read_concurrency: usize,
	pub receiver: crate::read::Receiver,
	pub subspace: fdbt::Subspace,
}

impl Index {
	pub(super) async fn reader_task(arg: Arg) {
		let Arg {
			authorize,
			database,
			partition_total,
			read_batch_size,
			read_concurrency,
			receiver,
			subspace,
		} = arg;
		stream::unfold(receiver, |mut receiver| async move {
			// Freeze the batch before opening its transaction.
			let request = receiver.recv().await?;
			let mut requests = Vec::with_capacity(read_batch_size);
			requests.push(request);
			while requests.len() < read_batch_size {
				let Ok(request) = receiver.try_recv() else {
					break;
				};
				requests.push(request);
			}

			Some((requests, receiver))
		})
		.for_each_concurrent(read_concurrency, |requests| {
			Self::execute_read_batch(authorize, &database, partition_total, &subspace, requests)
		})
		.await;
	}

	pub(super) async fn send_read_request(
		&self,
		request: crate::read::Request,
	) -> tg::Result<crate::read::Response> {
		let (sender, receiver) = tokio::sync::oneshot::channel();
		self.reader_sender
			.send((request, sender))
			.await
			.map_err(|error| tg::error!(!error, "failed to send the read request"))?;
		let response = receiver
			.await
			.map_err(|error| tg::error!(!error, "failed to receive the read response"))??;

		Ok(response)
	}

	async fn execute_read_batch(
		authorize: super::AuthorizeConfig,
		database: &fdb::Database,
		partition_total: u64,
		subspace: &fdbt::Subspace,
		requests: Vec<(crate::read::Request, crate::read::ResponseSender)>,
	) {
		// Remove the closed requests.
		let mut requests = requests
			.into_iter()
			.filter(|(_, sender)| !sender.is_closed())
			.collect::<Vec<_>>();
		if requests.is_empty() {
			return;
		}

		// Create the transaction.
		let mut transaction = match database.create_trx() {
			Err(error) => {
				let error = tg::error!(!error, "failed to create a read transaction");
				for (_, sender) in requests {
					sender.send(Err(error.clone())).ok();
				}

				return;
			},
			Ok(transaction) => transaction,
		};
		loop {
			let (retry_error, retry_requests) = {
				// Execute the pending requests concurrently.
				let transaction = &transaction;
				let mut futures = requests
					.into_iter()
					.map(|(request, sender)| {
						let read_request = request.clone();
						async move {
							let result = Self::execute_read_request(
								authorize,
								partition_total,
								transaction,
								subspace,
								read_request,
							)
							.await;

							(result, request, sender)
						}
					})
					.collect::<FuturesUnordered<_>>();

				// Send each completed response and collect the retryable requests.
				let mut retry_error = None;
				let mut retry_requests = Vec::new();
				while let Some((result, request, sender)) = futures.next().await {
					match result {
						Err(error) => {
							sender.send(Err(error)).ok();
						},
						Ok(ControlFlow::Break(response)) => {
							sender.send(Ok(response)).ok();
						},
						Ok(ControlFlow::Continue(error)) if error.is_retryable() => {
							if !sender.is_closed() {
								retry_error.get_or_insert(error);
								retry_requests.push((request, sender));
							}
						},
						Ok(ControlFlow::Continue(error)) => {
							let error = tg::error!(!error, "failed to execute a read request");
							sender.send(Err(error)).ok();
						},
					}
				}

				(retry_error, retry_requests)
			};
			let Some(error) = retry_error else {
				return;
			};

			// Reset the transaction for the retryable requests.
			transaction = match transaction.on_error(error).await {
				Err(error) => {
					let error = tg::error!(!error, "failed to retry a read transaction");
					for (_, sender) in retry_requests {
						sender.send(Err(error.clone())).ok();
					}

					return;
				},
				Ok(transaction) => transaction,
			};
			requests = retry_requests;
		}
	}

	async fn execute_read_request(
		authorize: super::AuthorizeConfig,
		partition_total: u64,
		transaction: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		request: crate::read::Request,
	) -> tg::Result<ControlFlow<crate::read::Response, fdb::FdbError>> {
		let response = match request {
			crate::read::Request::AuthorizeBatch { args, principal } => {
				let result = Self::authorize_batch_with_transaction(
					authorize,
					transaction,
					subspace,
					&args,
					&principal,
				)
				.await;
				let output = crate::fdb::propagate!(result);
				crate::read::Response::AuthorizeBatch(output)
			},
			crate::read::Request::ContainsIds { ids } => {
				let result = Self::contains_ids_with_transaction(transaction, subspace, &ids).await;
				let output = crate::fdb::propagate!(result);
				crate::read::Response::ContainsIds(output)
			},
			crate::read::Request::FdbLogCompactionBatch {
				batch_size,
				partition_end,
				partition_start,
			} => {
				let result = Self::log_compaction_batch_with_transaction(
					transaction,
					subspace,
					batch_size,
					partition_start,
					partition_end,
				)
				.await;
				let output = crate::fdb::propagate!(result);
				crate::read::Response::LogCompactionBatch(output)
			},
			crate::read::Request::TryGetProcessChildren {
				id,
				length,
				position,
			} => {
				let result = Self::try_get_process_children_page_with_transaction(
					transaction,
					subspace,
					&id,
					position,
					length,
				)
				.await;
				let output = crate::fdb::propagate!(result);
				crate::read::Response::TryGetProcessChildren(output)
			},
			crate::read::Request::LmdbLogCompactionBatch { .. } => {
				return Err(tg::error!("unexpected LMDB read request"));
			},
			crate::read::Request::GetRequesterSubjects { principal } => {
				let result =
					Self::requester_subjects_with_transaction(transaction, subspace, &principal)
						.await;
				let output = crate::fdb::propagate!(result);
				crate::read::Response::GetRequesterSubjects(output)
			},
			crate::read::Request::GetRunnerSandboxes { runner } => {
				let result =
					Self::get_runner_sandboxes_with_transaction(transaction, subspace, &runner)
						.await;
				let output = crate::fdb::propagate!(result);
				crate::read::Response::GetRunnerSandboxes(output)
			},
			crate::read::Request::GetSandboxProcesses { sandbox } => {
				let result =
					Self::get_sandbox_processes_with_transaction(transaction, subspace, &sandbox)
						.await;
				let output = crate::fdb::propagate!(result);
				crate::read::Response::GetSandboxProcesses(output)
			},
			crate::read::Request::GetTransactionId => {
				let result = transaction.get_read_version().await;
				let output = crate::fdb::retry!(result).cast_unsigned();
				crate::read::Response::GetTransactionId(output)
			},
			crate::read::Request::ListSandboxes => {
				let result = Self::list_sandboxes_with_transaction(transaction, subspace).await;
				let output = crate::fdb::propagate!(result);
				crate::read::Response::ListSandboxes(output)
			},
			crate::read::Request::ListSandboxesForCreator { creator } => {
				let result = Self::list_sandboxes_for_principal_with_transaction(
					transaction,
					subspace,
					&creator,
					super::Kind::CreatorSandbox,
				)
				.await;
				let output = crate::fdb::propagate!(result);
				crate::read::Response::ListSandboxes(output)
			},
			crate::read::Request::ListSandboxesForOwner { owner } => {
				let result = Self::list_sandboxes_for_principal_with_transaction(
					transaction,
					subspace,
					&owner,
					super::Kind::OwnerSandbox,
				)
				.await;
				let output = crate::fdb::propagate!(result);
				crate::read::Response::ListSandboxes(output)
			},
			crate::read::Request::ProcessHasAncestor { ancestor, process } => {
				let result = Self::process_has_ancestor_with_transaction(
					transaction,
					subspace,
					&process,
					&ancestor,
				)
				.await;
				let output = crate::fdb::propagate!(result);
				crate::read::Response::ProcessHasAncestor(output)
			},
			crate::read::Request::TryGetAncestors { id } => {
				let result =
					Self::try_get_ancestors_with_transaction(transaction, subspace, &id).await;
				let output = crate::fdb::propagate!(result);
				crate::read::Response::TryGetAncestors(output)
			},
			crate::read::Request::TryGetCacheEntries { ids } => {
				let result =
					Self::try_get_cache_entries_with_transaction(transaction, subspace, &ids).await;
				let output = crate::fdb::propagate!(result);
				crate::read::Response::TryGetCacheEntries(output)
			},
			crate::read::Request::TryGetCachedProcesses { command } => {
				let result = Self::try_get_cached_processes_with_transaction(
					transaction,
					subspace,
					&command,
				)
				.await;
				let output = crate::fdb::propagate!(result);
				crate::read::Response::TryGetCachedProcesses(output)
			},
			crate::read::Request::TryGetGroups { ids } => {
				let result =
					Self::try_get_groups_with_transaction(transaction, subspace, &ids).await;
				let output = crate::fdb::propagate!(result);
				crate::read::Response::TryGetGroups(output)
			},
			crate::read::Request::TryGetIdsForSpecifiers { specifiers } => {
				let result = Self::try_get_ids_for_specifiers_with_transaction(
					transaction,
					subspace,
					&specifiers,
				)
				.await;
				let output = crate::fdb::propagate!(result);
				crate::read::Response::TryGetIdsForSpecifiers(output)
			},
			crate::read::Request::TryGetObjects { ids } => {
				let result =
					Self::try_get_objects_with_transaction(transaction, subspace, &ids).await;
				let output = crate::fdb::propagate!(result);
				crate::read::Response::TryGetObjects(output)
			},
			crate::read::Request::TryGetOldestLogCompactionTransactionId => {
				let result = Self::try_get_oldest_log_compaction_transaction_id_with_transaction(
					transaction,
					subspace,
					partition_total,
				)
				.await;
				let output = crate::fdb::propagate!(result);
				crate::read::Response::TryGetOldestLogCompactionTransactionId(output)
			},
			crate::read::Request::TryGetOldestUpdateTransactionId { kind } => {
				let result = Self::try_get_oldest_update_transaction_id_with_transaction(
					transaction,
					subspace,
					kind,
					partition_total,
				)
				.await;
				let output = crate::fdb::propagate!(result);
				crate::read::Response::TryGetOldestUpdateTransactionId(output)
			},
			crate::read::Request::TryGetOrganizations { ids } => {
				let result =
					Self::try_get_organizations_with_transaction(transaction, subspace, &ids).await;
				let output = crate::fdb::propagate!(result);
				crate::read::Response::TryGetOrganizations(output)
			},
			crate::read::Request::TryGetProcesses { ids } => {
				let result =
					Self::try_get_processes_with_transaction(transaction, subspace, &ids).await;
				let output = crate::fdb::propagate!(result);
				crate::read::Response::TryGetProcesses(output)
			},
			crate::read::Request::TryGetSandboxes { ids } => {
				let result =
					Self::try_get_sandboxes_with_transaction(transaction, subspace, &ids).await;
				let output = crate::fdb::propagate!(result);
				crate::read::Response::TryGetSandboxes(output)
			},
			crate::read::Request::TryGetSpecifiersForIds { ids } => {
				let result =
					Self::try_get_specifiers_for_ids_with_transaction(transaction, subspace, &ids)
						.await;
				let output = crate::fdb::propagate!(result);
				crate::read::Response::TryGetSpecifiersForIds(output)
			},
			crate::read::Request::TryGetUsers { ids } => {
				let result =
					Self::try_get_users_with_transaction(transaction, subspace, &ids).await;
				let output = crate::fdb::propagate!(result);
				crate::read::Response::TryGetUsers(output)
			},
			crate::read::Request::Visible { ids, principal } => {
				let result =
					Self::visible_with_transaction(transaction, subspace, &ids, &principal).await;
				let output = crate::fdb::propagate!(result);
				crate::read::Response::Visible(output)
			},
		};

		Ok(ControlFlow::Break(response))
	}
}
