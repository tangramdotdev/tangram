use {
	super::Index,
	foundationdb as fdb, foundationdb_tuple as fdbt,
	futures::{StreamExt as _, stream::FuturesUnordered},
	std::{ops::ControlFlow, sync::Arc},
	tangram_client::prelude::*,
	tokio::task::JoinSet,
};

#[cfg(test)]
mod tests;

type Batch = Vec<(
	tangram_index::read::Request,
	tangram_index::read::ResponseSender,
)>;

pub(super) struct Arg {
	pub database: Arc<fdb::Database>,
	pub partition_totals: crate::PartitionTotals,
	pub read_request_batch_size: usize,
	pub read_task_concurrency: usize,
	pub read_task_count: usize,
	pub receiver: tangram_index::read::Receiver,
	pub subspace: fdbt::Subspace,
	pub verification_concurrency: usize,
}

impl Index {
	pub(super) async fn reader_task(arg: Arg) {
		let Arg {
			database,
			partition_totals,
			read_request_batch_size,
			read_task_concurrency,
			read_task_count,
			receiver,
			subspace,
			verification_concurrency,
		} = arg;

		// Spawn the readers with a shared batch channel.
		let (sender, batches) = async_channel::bounded(read_task_count);
		let mut tasks = JoinSet::new();
		for _ in 0..read_task_count {
			let database = database.clone();
			let receiver = batches.clone();
			let subspace = subspace.clone();
			tasks.spawn(async move {
				Self::reader_worker_task(
					database,
					partition_totals,
					read_task_concurrency,
					receiver,
					subspace,
					verification_concurrency,
				)
				.await;
			});
		}
		drop(batches);

		// Produce the batches and close the channel when the requests end.
		Self::reader_batch_task(receiver, sender, read_request_batch_size).await;

		// Drain the readers before exiting.
		while let Some(result) = tasks.join_next().await {
			if let Err(error) = result {
				tracing::error!(%error, "an index reader task failed");
			}
		}
	}

	async fn reader_worker_task(
		database: Arc<fdb::Database>,
		partition_totals: crate::PartitionTotals,
		read_task_concurrency: usize,
		receiver: async_channel::Receiver<Batch>,
		subspace: fdbt::Subspace,
		verification_concurrency: usize,
	) {
		receiver
			.for_each_concurrent(read_task_concurrency, |requests| {
				Self::execute_read_batch(
					verification_concurrency,
					&database,
					partition_totals,
					&subspace,
					requests,
				)
			})
			.await;
	}

	async fn reader_batch_task(
		mut receiver: tangram_index::read::Receiver,
		sender: async_channel::Sender<Batch>,
		read_request_batch_size: usize,
	) {
		while let Some(request) = receiver.recv().await {
			// Freeze the batch before opening its transaction.
			let mut requests = Vec::with_capacity(read_request_batch_size);
			requests.push(request);
			while requests.len() < read_request_batch_size {
				let Ok(request) = receiver.try_recv() else {
					break;
				};
				requests.push(request);
			}
			if sender.send(requests).await.is_err() {
				return;
			}
		}
	}

	pub(super) async fn send_read_request(
		&self,
		request: tangram_index::read::Request,
	) -> tg::Result<tangram_index::read::Response> {
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
		verification_concurrency: usize,
		database: &fdb::Database,
		partition_totals: crate::PartitionTotals,
		subspace: &fdbt::Subspace,
		requests: Vec<(
			tangram_index::read::Request,
			tangram_index::read::ResponseSender,
		)>,
	) {
		// Remove the closed requests.
		let mut requests = requests
			.into_iter()
			.filter(|(_, sender)| !sender.is_closed())
			.map(|(request, sender)| (request, sender, None::<tangram_index::verify::Batch>))
			.collect::<Vec<_>>();
		if requests.is_empty() {
			return;
		}

		// Create the transaction.
		let transaction = match database.create_trx() {
			Err(error) => {
				let error = tg::error!(!error, "failed to create a read transaction");
				for (_, sender, _) in requests {
					sender.send(Err(error.clone())).ok();
				}

				return;
			},
			Ok(transaction) => transaction,
		};
		let mut transaction = crate::Transaction::new(transaction);
		loop {
			let started = std::time::Instant::now();
			let request_count = requests.len();
			tracing::debug!(request_count, "starting an index read transaction");
			let verification_fact_cache = tangram_index::verify::facts::Cache::new();
			let (retry_error, mut retry_requests) = {
				// Execute the pending requests concurrently.
				let transaction = &transaction;
				let request_count = requests.len();
				let mut futures = requests
					.into_iter()
					.map(|(request, sender, mut batch)| {
						let verification_fact_cache = verification_fact_cache.clone();
						async move {
							let result = Self::execute_read_request(
								verification_fact_cache,
								verification_concurrency,
								partition_totals,
								transaction,
								subspace,
								&request,
								&mut batch,
							)
							.await;

							(result, request, sender, batch)
						}
					})
					.collect::<FuturesUnordered<_>>();

				// Send each completed response and collect the retryable requests.
				let mut retry_error = None;
				let mut retry_requests = Vec::with_capacity(request_count);
				while let Some((result, request, sender, batch)) = futures.next().await {
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
								retry_requests.push((request, sender, batch));
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
				tracing::debug!(
					request_count,
					elapsed_ms = started.elapsed().as_millis(),
					"finished an index read transaction"
				);
				return;
			};
			retry_requests.retain(|(_, sender, _)| !sender.is_closed());
			if retry_requests.is_empty() {
				return;
			}

			tracing::warn!(
				code = error.code(),
				request_count,
				retry_requests = retry_requests.len(),
				elapsed_ms = started.elapsed().as_millis(),
				"retrying an index read transaction"
			);

			// Reset the transaction for the retryable requests.
			let inner = match transaction.take() {
				Err(error) => {
					for (_, sender, _) in retry_requests {
						sender.send(Err(error.clone())).ok();
					}

					return;
				},
				Ok(transaction) => transaction,
			};
			transaction = match inner.on_error(error).await {
				Err(error) => {
					let error = tg::error!(!error, "failed to retry a read transaction");
					for (_, sender, _) in retry_requests {
						sender.send(Err(error.clone())).ok();
					}

					return;
				},
				Ok(transaction) => crate::Transaction::new(transaction),
			};
			requests = retry_requests;
		}
	}

	async fn execute_read_request(
		verification_fact_cache: tangram_index::verify::facts::Cache<fdb::FdbError>,
		verification_concurrency: usize,
		partition_totals: crate::PartitionTotals,
		transaction: &crate::Transaction,
		subspace: &fdbt::Subspace,
		request: &tangram_index::read::Request,
		batch: &mut Option<tangram_index::verify::Batch>,
	) -> tg::Result<ControlFlow<tangram_index::read::Response, fdb::FdbError>> {
		let response = match request {
			tangram_index::read::Request::VerifyBatch {
				args,
				config,
				principal,
			} => {
				let batch = match batch {
					Some(batch) => batch,
					None => {
						batch.insert(tangram_index::verify::Batch::new(args, *config, principal)?)
					},
				};
				let result = Self::verify_batch_with_transaction(
					verification_fact_cache,
					verification_concurrency,
					transaction,
					subspace,
					batch,
				)
				.await;
				let output = crate::propagate!(result);
				tangram_index::read::Response::VerifyBatch(output)
			},
			tangram_index::read::Request::PermissionCaptureBatch {
				batch_size,
				partition_end,
				partition_start,
			} => {
				let entries = crate::propagate!(
					Self::permission_capture_batch_with_transaction(
						transaction,
						subspace,
						*batch_size,
						*partition_start,
						*partition_end,
					)
					.await
				);
				tangram_index::read::Response::PermissionCaptureBatch(entries)
			},
			tangram_index::read::Request::ContainsIds { ids } => {
				let result = Self::contains_ids_with_transaction(transaction, subspace, ids).await;
				let output = crate::propagate!(result);
				tangram_index::read::Response::ContainsIds(output)
			},
			tangram_index::read::Request::GetIndexers => {
				let result = Self::get_indexers_with_transaction(transaction, subspace).await;
				let output = crate::propagate!(result);
				tangram_index::read::Response::GetIndexers(output)
			},
			tangram_index::read::Request::TryGetProcessChildrenCount { id } => {
				let result = Self::try_get_process_children_count_with_transaction(
					transaction,
					subspace,
					id,
				)
				.await?;
				let output = match result {
					ControlFlow::Break(output) => output,
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				};
				tangram_index::read::Response::TryGetProcessChildrenCount(output)
			},
			tangram_index::read::Request::TryGetSandboxProcessesCount { id } => {
				let result = Self::try_get_sandbox_processes_count_with_transaction(
					transaction,
					subspace,
					id,
				)
				.await?;
				let output = match result {
					ControlFlow::Break(output) => output,
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				};
				tangram_index::read::Response::TryGetSandboxProcessesCount(output)
			},
			tangram_index::read::Request::TryGetProcessChildren {
				id,
				length,
				position,
			} => {
				let result = Self::try_get_process_children_page_with_transaction(
					transaction,
					subspace,
					id,
					*position,
					*length,
				)
				.await;
				let output = crate::propagate!(result);
				tangram_index::read::Response::TryGetProcessChildren(output)
			},
			tangram_index::read::Request::TryGetSandboxProcesses {
				id,
				length,
				position,
			} => {
				let result = Self::try_get_sandbox_processes_page_with_transaction(
					transaction,
					subspace,
					id,
					*position,
					*length,
				)
				.await;
				let output = crate::propagate!(result);
				tangram_index::read::Response::TryGetSandboxProcesses(output)
			},
			tangram_index::read::Request::TryGetProcessChildrenAndObjects { id } => {
				let result = Self::try_get_process_children_and_objects_with_transaction(
					transaction,
					subspace,
					id,
				)
				.await;
				let output = crate::propagate!(result);
				tangram_index::read::Response::TryGetProcessChildrenAndObjects(output)
			},
			tangram_index::read::Request::GetRunnerSandboxes { runner } => {
				let result =
					Self::get_runner_sandboxes_with_transaction(transaction, subspace, runner)
						.await;
				let output = crate::propagate!(result);
				tangram_index::read::Response::GetRunnerSandboxes(output)
			},
			tangram_index::read::Request::GetSubjectsForPrincipal { principal } => {
				let result = Self::get_subjects_for_principal_with_transaction(
					transaction,
					subspace,
					principal,
				)
				.await;
				let output = crate::propagate!(result);
				tangram_index::read::Response::GetSubjectsForPrincipal(output)
			},

			tangram_index::read::Request::GetTransactionId => {
				let result = transaction.get_read_version().await;
				let output = crate::retry!(result).cast_unsigned();
				tangram_index::read::Response::GetTransactionId(output)
			},
			tangram_index::read::Request::ListSandboxes => {
				let result = Self::list_sandboxes_with_transaction(transaction, subspace).await;
				let output = crate::propagate!(result);
				tangram_index::read::Response::ListSandboxes(output)
			},
			tangram_index::read::Request::ListSandboxesForCreator { creator } => {
				let result = Self::list_sandboxes_for_principal_with_transaction(
					transaction,
					subspace,
					creator,
					super::Kind::CreatorSandbox,
				)
				.await;
				let output = crate::propagate!(result);
				tangram_index::read::Response::ListSandboxes(output)
			},
			tangram_index::read::Request::ListSandboxesForOwner { owner } => {
				let result = Self::list_sandboxes_for_principal_with_transaction(
					transaction,
					subspace,
					owner,
					super::Kind::OwnerSandbox,
				)
				.await;
				let output = crate::propagate!(result);
				tangram_index::read::Response::ListSandboxes(output)
			},
			tangram_index::read::Request::ProcessHasAncestor { ancestor, process } => {
				let result = Self::process_has_ancestor_with_transaction(
					transaction,
					subspace,
					process,
					ancestor,
				)
				.await;
				let output = crate::propagate!(result);
				tangram_index::read::Response::ProcessHasAncestor(output)
			},
			tangram_index::read::Request::TryGetAncestors { id } => {
				let result =
					Self::try_get_ancestors_with_transaction(transaction, subspace, id).await;
				let output = crate::propagate!(result);
				tangram_index::read::Response::TryGetAncestors(output)
			},
			tangram_index::read::Request::TryGetCheckouts { ids } => {
				let result =
					Self::try_get_checkouts_with_transaction(transaction, subspace, ids).await;
				let output = crate::propagate!(result);
				tangram_index::read::Response::TryGetCheckouts(output)
			},
			tangram_index::read::Request::TryGetCachedProcesses { command } => {
				let result =
					Self::try_get_cached_processes_with_transaction(transaction, subspace, command)
						.await;
				let output = crate::propagate!(result);
				tangram_index::read::Response::TryGetCachedProcesses(output)
			},
			tangram_index::read::Request::TryGetGroups { ids } => {
				let result =
					Self::try_get_groups_with_transaction(transaction, subspace, ids).await;
				let output = crate::propagate!(result);
				tangram_index::read::Response::TryGetGroups(output)
			},
			tangram_index::read::Request::TryGetIdsForSpecifiers { specifiers } => {
				let result = Self::try_get_ids_for_specifiers_with_transaction(
					transaction,
					subspace,
					specifiers,
				)
				.await;
				let output = crate::propagate!(result);
				tangram_index::read::Response::TryGetIdsForSpecifiers(output)
			},
			tangram_index::read::Request::TryGetIndexer(arg) => {
				let result =
					Self::try_get_indexer_with_transaction(transaction, subspace, arg).await;
				let output = crate::propagate!(result);
				tangram_index::read::Response::TryGetIndexer(output)
			},
			tangram_index::read::Request::TryGetObjectChildren { id } => {
				let object = crate::propagate!(
					Self::try_get_object_with_transaction(transaction, subspace, id).await
				);
				let output = if object.is_some() {
					let children = crate::propagate!(
						crate::page::collect(|after| async move {
							Self::get_object_children_page_with_transaction(
								transaction,
								subspace,
								id,
								after.as_deref(),
								crate::page::PAGE_SIZE,
							)
							.await
						})
						.await
					);
					Some(children)
				} else {
					None
				};
				tangram_index::read::Response::TryGetObjectChildren(output)
			},
			tangram_index::read::Request::TryGetObjects { ids } => {
				let result =
					Self::try_get_objects_with_transaction(transaction, subspace, ids).await;
				let output = crate::propagate!(result);
				tangram_index::read::Response::TryGetObjects(output)
			},
			tangram_index::read::Request::TryGetOldestUpdateTransactionId { kind } => {
				let result = Self::try_get_oldest_update_transaction_id_with_transaction(
					transaction,
					subspace,
					*kind,
					partition_totals.update(*kind),
				)
				.await;
				let output = crate::propagate!(result);
				tangram_index::read::Response::TryGetOldestUpdateTransactionId(output)
			},
			tangram_index::read::Request::TryGetOrganizations { ids } => {
				let result =
					Self::try_get_organizations_with_transaction(transaction, subspace, ids).await;
				let output = crate::propagate!(result);
				tangram_index::read::Response::TryGetOrganizations(output)
			},
			tangram_index::read::Request::TryGetProcesses { ids } => {
				let result =
					Self::try_get_processes_with_transaction(transaction, subspace, ids).await;
				let output = crate::propagate!(result);
				tangram_index::read::Response::TryGetProcesses(output)
			},
			tangram_index::read::Request::TryGetSandboxes { ids } => {
				let result =
					Self::try_get_sandboxes_with_transaction(transaction, subspace, ids).await;
				let output = crate::propagate!(result);
				tangram_index::read::Response::TryGetSandboxes(output)
			},
			tangram_index::read::Request::TryGetSpecifiersForIds { ids } => {
				let result =
					Self::try_get_specifiers_for_ids_with_transaction(transaction, subspace, ids)
						.await;
				let output = crate::propagate!(result);
				tangram_index::read::Response::TryGetSpecifiersForIds(output)
			},
			tangram_index::read::Request::TryGetTags { ids } => {
				let result = Self::try_get_tags_with_transaction(transaction, subspace, ids).await;
				let output = crate::propagate!(result);
				tangram_index::read::Response::TryGetTags(output)
			},
			tangram_index::read::Request::TryGetUsers { ids } => {
				let result = Self::try_get_users_with_transaction(transaction, subspace, ids).await;
				let output = crate::propagate!(result);
				tangram_index::read::Response::TryGetUsers(output)
			},
			tangram_index::read::Request::Visible { ids, principal } => {
				let result =
					Self::visible_with_transaction(transaction, subspace, ids, principal).await;
				let output = crate::propagate!(result);
				tangram_index::read::Response::Visible(output)
			},
		};

		Ok(ControlFlow::Break(response))
	}
}
