use {
	super::{ClientMessage, Response, ResponseOutput, ServerMessage},
	crate::Server,
	crate::indexer::Indexer,
	futures::{FutureExt as _, future, stream::FuturesUnordered},
	std::collections::BTreeMap,
	tangram_client::prelude::*,
	tangram_index::prelude::*,
};

type TaskWaits = FuturesUnordered<futures::future::BoxFuture<'static, Vec<String>>>;
type Sender = crate::control::Sender<ServerMessage, ClientMessage>;

pub(super) struct State {
	database_index_outbox_batch_id: Option<crate::database::index::outbox::BatchId>,
	pub(super) queues: crate::indexer::queue::Queues,
	pub(super) task_waits: TaskWaits,
	pub(super) waits: BTreeMap<String, WaitRequest>,
}

pub(super) struct WaitRequest {
	pub(super) state: WaitRequestState,
}

pub(super) enum WaitRequestState {
	DatabaseIndexOutbox,
	DatabaseIndexOutboxPending,
	LogCompactions { transaction_id: Option<u64> },
	ObjectIndexQueue,
	ObjectIndexQueuePending { sequence: u64 },
	Tasks,
	Updates { transaction_id: Option<u64> },
}

impl State {
	pub(super) fn new(queues: crate::indexer::queue::Queues) -> Self {
		Self {
			database_index_outbox_batch_id: None,
			queues,
			task_waits: TaskWaits::new(),
			waits: BTreeMap::new(),
		}
	}

	pub(super) async fn poll(&mut self, indexer: &Indexer, sender: &Sender) -> tg::Result<()> {
		let server = &indexer.server;

		// Wait for the object index queue.
		self.poll_object_index_queue(indexer);

		// Wait for the database index outbox.
		self.poll_database_index_outbox(server).await?;

		// Wait for the log compaction queue.
		self.set_log_compaction_targets(server).await?;
		self.poll_log_compactions(server).await?;

		// Wait for the index update queue.
		self.set_update_targets(server).await?;
		self.poll_updates(server, sender).await?;

		Ok(())
	}

	fn poll_object_index_queue(&mut self, indexer: &Indexer) {
		if indexer.server.config.advanced.single_process {
			for request in self.waits.values_mut() {
				if matches!(request.state, WaitRequestState::ObjectIndexQueue) {
					request.state = WaitRequestState::DatabaseIndexOutbox;
				}
			}
			return;
		}

		// Snapshot the next cohort.
		if self
			.waits
			.values()
			.any(|request| matches!(request.state, WaitRequestState::ObjectIndexQueue))
		{
			let sequence = self.queues.index_target();
			for request in self.waits.values_mut() {
				if matches!(request.state, WaitRequestState::ObjectIndexQueue) {
					request.state = WaitRequestState::ObjectIndexQueuePending { sequence };
				}
			}
		}

		// Poll the active cohort.
		let read_sequence = self.queues.index_read_sequence();
		for request in self.waits.values_mut() {
			let WaitRequestState::ObjectIndexQueuePending { sequence } = request.state else {
				continue;
			};
			if read_sequence >= sequence {
				request.state = WaitRequestState::DatabaseIndexOutbox;
			}
		}
	}

	async fn poll_database_index_outbox(&mut self, server: &Server) -> tg::Result<()> {
		let region = server.config.region.clone().unwrap_or_default();

		// Poll the active cohort.
		if let Some(batch) = self.database_index_outbox_batch_id {
			let arg = crate::database::index::outbox::TryGetBatchArg {
				batch: Some(batch),
				region,
			};
			let batch = server
				.database
				.try_get_index_outbox_batch_at_or_before(arg)
				.await
				.map_err(|error| tg::error!(!error, "failed to poll the database index outbox"))?;
			if batch.is_some() {
				return Ok(());
			}
			for request in self.waits.values_mut() {
				if matches!(request.state, WaitRequestState::DatabaseIndexOutboxPending) {
					request.state = if server.config.indexer.log_compaction.enabled {
						WaitRequestState::LogCompactions {
							transaction_id: None,
						}
					} else {
						WaitRequestState::Updates {
							transaction_id: None,
						}
					};
				}
			}
			self.database_index_outbox_batch_id = None;

			return Ok(());
		}

		// Snapshot the next cohort.
		let snapshot = self
			.waits
			.values()
			.any(|request| matches!(request.state, WaitRequestState::DatabaseIndexOutbox));
		if !snapshot {
			return Ok(());
		}
		let arg = crate::database::index::outbox::TryGetBatchArg {
			batch: None,
			region,
		};
		let batch = server
			.database
			.try_get_index_outbox_batch_at_or_before(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to snapshot the database index outbox"))?;
		for request in self.waits.values_mut() {
			if !matches!(request.state, WaitRequestState::DatabaseIndexOutbox) {
				continue;
			}
			request.state = if batch.is_some() {
				WaitRequestState::DatabaseIndexOutboxPending
			} else if server.config.indexer.log_compaction.enabled {
				WaitRequestState::LogCompactions {
					transaction_id: None,
				}
			} else {
				WaitRequestState::Updates {
					transaction_id: None,
				}
			};
		}
		self.database_index_outbox_batch_id = batch;

		Ok(())
	}

	async fn set_log_compaction_targets(&mut self, server: &Server) -> tg::Result<()> {
		let set_target = self.waits.values().any(|request| {
			matches!(
				request.state,
				WaitRequestState::LogCompactions {
					transaction_id: None
				}
			)
		});
		if !set_target {
			return Ok(());
		}
		let transaction_id = server.index.get_transaction_id().await?;
		for request in self.waits.values_mut() {
			if let WaitRequestState::LogCompactions {
				transaction_id: target @ None,
			} = &mut request.state
			{
				*target = Some(transaction_id);
			}
		}

		Ok(())
	}

	async fn poll_log_compactions(&mut self, server: &Server) -> tg::Result<()> {
		let poll = self.waits.values().any(|request| {
			matches!(
				request.state,
				WaitRequestState::LogCompactions {
					transaction_id: Some(_)
				}
			)
		});
		if !poll {
			return Ok(());
		}
		let oldest = server
			.index
			.try_get_oldest_log_compaction_transaction_id()
			.await?;
		for request in self.waits.values_mut() {
			let WaitRequestState::LogCompactions {
				transaction_id: Some(transaction_id),
			} = request.state
			else {
				continue;
			};
			if oldest.is_none_or(|oldest| oldest > transaction_id) {
				request.state = WaitRequestState::Updates {
					transaction_id: None,
				};
			}
		}

		Ok(())
	}

	pub(super) fn start_task_wait(&mut self, server: &Server) {
		if !self.task_waits.is_empty() {
			return;
		}
		let ids = self
			.waits
			.iter()
			.filter(|(_, request)| matches!(request.state, WaitRequestState::Tasks))
			.map(|(id, _)| id.clone())
			.collect::<Vec<_>>();
		if ids.is_empty() {
			return;
		}
		let server = server.clone();
		self.task_waits.push(
			async move {
				let request = ids.first().unwrap().clone();
				crate::checkpoint!(server, "indexer.request.wait", request,).await;
				server.remote_object_put_tasks.wait().await;
				server.index_tasks.wait().await;

				ids
			}
			.boxed(),
		);
	}

	pub(super) fn handle_task_wait(&mut self, ids: Vec<String>) {
		for id in ids {
			let Some(request) = self.waits.get_mut(&id) else {
				continue;
			};
			if matches!(request.state, WaitRequestState::Tasks) {
				request.state = WaitRequestState::ObjectIndexQueue;
			}
		}
	}

	async fn set_update_targets(&mut self, server: &Server) -> tg::Result<()> {
		let set_target = self.waits.values().any(|request| {
			matches!(
				request.state,
				WaitRequestState::Updates {
					transaction_id: None
				}
			)
		});
		if !set_target {
			return Ok(());
		}
		let transaction_id = server.index.get_transaction_id().await?;
		for request in self.waits.values_mut() {
			if let WaitRequestState::Updates {
				transaction_id: target @ None,
			} = &mut request.state
			{
				*target = Some(transaction_id);
			}
		}

		Ok(())
	}

	async fn poll_updates(&mut self, server: &Server, sender: &Sender) -> tg::Result<()> {
		let poll = self.waits.values().any(|request| {
			matches!(
				request.state,
				WaitRequestState::Updates {
					transaction_id: Some(_)
				}
			)
		});
		if !poll {
			return Ok(());
		}
		let oldests = future::try_join3(
			server
				.index
				.try_get_oldest_update_transaction_id(tangram_index::update::Kind::Grant),
			server
				.index
				.try_get_oldest_update_transaction_id(tangram_index::update::Kind::Node),
			server
				.index
				.try_get_oldest_update_transaction_id(tangram_index::update::Kind::Storage),
		)
		.await?;
		let ids = self
			.waits
			.iter()
			.filter_map(|(id, request)| {
				let WaitRequestState::Updates {
					transaction_id: Some(transaction_id),
				} = request.state
				else {
					return None;
				};
				[oldests.0, oldests.1, oldests.2]
					.into_iter()
					.all(|oldest| oldest.is_none_or(|oldest| oldest > transaction_id))
					.then(|| id.clone())
			})
			.collect::<Vec<_>>();
		for id in ids {
			self.waits.remove(&id);
			Self::send_response(id, Ok(ResponseOutput::Wait), sender);
		}

		Ok(())
	}

	pub(super) fn fail(&mut self, error: &tg::Error, sender: &Sender) {
		let error = error.to_string();
		self.database_index_outbox_batch_id = None;
		let ids = std::mem::take(&mut self.waits).into_keys();
		for id in ids {
			Self::send_response(
				id,
				Err(tg::error!(error = %error, "failed to wait for indexing")),
				sender,
			);
		}
	}

	pub(super) fn send_response(id: String, result: tg::Result<ResponseOutput>, sender: &Sender) {
		let response = match result {
			Ok(output) => Response {
				error: None,
				id,
				output: Some(output),
			},
			Err(error) => Response {
				error: Some(tg::error::Data {
					message: Some(error.to_string()),
					..Default::default()
				}),
				id,
				output: None,
			},
		};
		let sender = sender.clone();
		tokio::spawn(async move {
			if let Err(error) = sender.send(ClientMessage::Response(response)).await {
				tracing::error!(error = %error.trace(), "failed to send an indexer response");
			}
		});
	}

	pub(super) fn needs_poll(&self) -> bool {
		self.waits.values().any(|request| {
			matches!(
				request.state,
				WaitRequestState::DatabaseIndexOutbox
					| WaitRequestState::DatabaseIndexOutboxPending
					| WaitRequestState::LogCompactions { .. }
					| WaitRequestState::ObjectIndexQueue
					| WaitRequestState::ObjectIndexQueuePending { .. }
					| WaitRequestState::Updates { .. }
			)
		})
	}
}
