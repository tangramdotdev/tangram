use {
	super::Index,
	foundationdb as fdb, foundationdb_tuple as fdbt,
	futures::{StreamExt as _, future, stream},
	std::sync::Arc,
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
		let requests = requests
			.into_iter()
			.filter(|(_, sender)| !sender.is_closed())
			.collect::<Vec<_>>();
		if requests.is_empty() {
			return;
		}
		let transaction = database
			.create_trx()
			.map_err(|error| tg::error!(!error, "failed to create the transaction"));
		let transaction = match transaction {
			Ok(transaction) => transaction,
			Err(error) => {
				for (_, sender) in requests {
					sender.send(Err(error.clone())).ok();
				}
				return;
			},
		};
		future::join_all(requests.into_iter().map(|(request, sender)| {
			let transaction = &transaction;
			async move {
				let response = Self::execute_read_request(
					authorize,
					partition_total,
					transaction,
					subspace,
					request,
				)
				.await;
				sender.send(response).ok();
			}
		}))
		.await;
	}

	async fn execute_read_request(
		authorize: super::AuthorizeConfig,
		partition_total: u64,
		transaction: &fdb::Transaction,
		subspace: &fdbt::Subspace,
		request: crate::read::Request,
	) -> tg::Result<crate::read::Response> {
		let response = match request {
			crate::read::Request::AuthorizeBatch { args, principal } => {
				let output = Self::authorize_batch_with_transaction(
					authorize,
					transaction,
					subspace,
					&args,
					&principal,
				)
				.await?;
				crate::read::Response::AuthorizeBatch(output)
			},
			crate::read::Request::ContainsIds { ids } => {
				let output =
					Self::contains_ids_with_transaction(transaction, subspace, &ids).await?;
				crate::read::Response::ContainsIds(output)
			},
			crate::read::Request::FdbLogCompactionBatch {
				batch_size,
				partition_end,
				partition_start,
			} => {
				let output = Self::log_compaction_batch_with_transaction(
					transaction,
					subspace,
					batch_size,
					partition_start,
					partition_end,
				)
				.await?;
				crate::read::Response::LogCompactionBatch(output)
			},
			crate::read::Request::LmdbLogCompactionBatch { .. } => {
				return Err(tg::error!("unexpected LMDB read request"));
			},
			crate::read::Request::GetRequesterPrincipals { principal } => {
				let output =
					Self::requester_principals_with_transaction(transaction, subspace, &principal)
						.await?;
				crate::read::Response::GetRequesterPrincipals(output)
			},
			crate::read::Request::GetRunnerSandboxes { runner } => {
				let output =
					Self::get_runner_sandboxes_with_transaction(transaction, subspace, &runner)
						.await?;
				crate::read::Response::GetRunnerSandboxes(output)
			},
			crate::read::Request::GetSandboxProcesses { sandbox } => {
				let output =
					Self::get_sandbox_processes_with_transaction(transaction, subspace, &sandbox)
						.await?;
				crate::read::Response::GetSandboxProcesses(output)
			},
			crate::read::Request::GetTransactionId => {
				let output = transaction
					.get_read_version()
					.await
					.map_err(|error| tg::error!(!error, "failed to get the read version"))?
					.cast_unsigned();
				crate::read::Response::GetTransactionId(output)
			},
			crate::read::Request::ListSandboxes => {
				let output = Self::list_sandboxes_with_transaction(transaction, subspace).await?;
				crate::read::Response::ListSandboxes(output)
			},
			crate::read::Request::ListSandboxesForCreator { creator } => {
				let output = Self::list_sandboxes_for_principal_with_transaction(
					transaction,
					subspace,
					&creator,
					super::Kind::CreatorSandbox,
				)
				.await?;
				crate::read::Response::ListSandboxes(output)
			},
			crate::read::Request::ListSandboxesForOwner { owner } => {
				let output = Self::list_sandboxes_for_principal_with_transaction(
					transaction,
					subspace,
					&owner,
					super::Kind::OwnerSandbox,
				)
				.await?;
				crate::read::Response::ListSandboxes(output)
			},
			crate::read::Request::ProcessHasAncestor { ancestor, process } => {
				let output = Self::process_has_ancestor_with_transaction(
					transaction,
					subspace,
					&process,
					&ancestor,
				)
				.await?;
				crate::read::Response::ProcessHasAncestor(output)
			},
			crate::read::Request::TryGetAncestors { id } => {
				let output =
					Self::try_get_ancestors_with_transaction(transaction, subspace, &id).await?;
				crate::read::Response::TryGetAncestors(output)
			},
			crate::read::Request::TryGetCacheEntries { ids } => {
				let output =
					Self::try_get_cache_entries_with_transaction(transaction, subspace, &ids)
						.await?;
				crate::read::Response::TryGetCacheEntries(output)
			},
			crate::read::Request::TryGetCachedProcesses { command } => {
				let output = Self::try_get_cached_processes_with_transaction(
					transaction,
					subspace,
					&command,
				)
				.await?;
				crate::read::Response::TryGetCachedProcesses(output)
			},
			crate::read::Request::TryGetGroups { ids } => {
				let output =
					Self::try_get_groups_with_transaction(transaction, subspace, &ids).await?;
				crate::read::Response::TryGetGroups(output)
			},
			crate::read::Request::TryGetIdsForSpecifiers { specifiers } => {
				let output = Self::try_get_ids_for_specifiers_with_transaction(
					transaction,
					subspace,
					&specifiers,
				)
				.await?;
				crate::read::Response::TryGetIdsForSpecifiers(output)
			},
			crate::read::Request::TryGetObjects { ids } => {
				let output =
					Self::try_get_objects_with_transaction(transaction, subspace, &ids).await?;
				crate::read::Response::TryGetObjects(output)
			},
			crate::read::Request::TryGetOldestLogCompactionTransactionId => {
				let output = Self::try_get_oldest_log_compaction_transaction_id_with_transaction(
					transaction,
					subspace,
					partition_total,
				)
				.await?;
				crate::read::Response::TryGetOldestLogCompactionTransactionId(output)
			},
			crate::read::Request::TryGetOldestUpdateTransactionId { kind } => {
				let output = Self::try_get_oldest_update_transaction_id_with_transaction(
					transaction,
					subspace,
					kind,
					partition_total,
				)
				.await?;
				crate::read::Response::TryGetOldestUpdateTransactionId(output)
			},
			crate::read::Request::TryGetOrganizations { ids } => {
				let output =
					Self::try_get_organizations_with_transaction(transaction, subspace, &ids)
						.await?;
				crate::read::Response::TryGetOrganizations(output)
			},
			crate::read::Request::TryGetProcesses { ids } => {
				let output =
					Self::try_get_processes_with_transaction(transaction, subspace, &ids).await?;
				crate::read::Response::TryGetProcesses(output)
			},
			crate::read::Request::TryGetSandboxes { ids } => {
				let output =
					Self::try_get_sandboxes_with_transaction(transaction, subspace, &ids).await?;
				crate::read::Response::TryGetSandboxes(output)
			},
			crate::read::Request::TryGetSpecifiersForIds { ids } => {
				let output =
					Self::try_get_specifiers_for_ids_with_transaction(transaction, subspace, &ids)
						.await?;
				crate::read::Response::TryGetSpecifiersForIds(output)
			},
			crate::read::Request::TryGetUsers { ids } => {
				let output =
					Self::try_get_users_with_transaction(transaction, subspace, &ids).await?;
				crate::read::Response::TryGetUsers(output)
			},
			crate::read::Request::Visible { ids, principal } => {
				let output =
					Self::visible_with_transaction(transaction, subspace, &ids, &principal).await?;
				crate::read::Response::Visible(output)
			},
		};

		Ok(response)
	}
}
