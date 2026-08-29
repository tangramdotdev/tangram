use {
	super::{ClientMessage, Response, ResponseOutput, ServerMessage},
	crate::Server,
	futures::{FutureExt as _, future, stream::FuturesUnordered},
	std::collections::BTreeMap,
	tangram_client::prelude::*,
	tangram_index::prelude::*,
	tangram_store::Store as _,
};

type Barriers = FuturesUnordered<futures::future::BoxFuture<'static, Vec<String>>>;
type Sender = crate::control::Sender<ServerMessage, ClientMessage>;

pub(super) struct State {
	pub(super) barriers: Barriers,
	database_index_outbox_batch_id: Option<crate::database::index::outbox::BatchId>,
	object_index_outbox_batch_id: Option<crate::store::object::index::outbox::batch::Id>,
	pub(super) requests: BTreeMap<String, IndexRequest>,
}

pub(super) struct IndexRequest {
	pub(super) state: IndexRequestState,
}

pub(super) enum IndexRequestState {
	DatabaseIndexOutbox,
	DatabaseIndexOutboxPending,
	LogCompactions { transaction_id: Option<u64> },
	ObjectIndexOutbox,
	ObjectIndexOutboxPending,
	Tasks,
	Updates { transaction_id: Option<u64> },
}

impl State {
	pub(super) fn new() -> Self {
		Self {
			barriers: Barriers::new(),
			database_index_outbox_batch_id: None,
			object_index_outbox_batch_id: None,
			requests: BTreeMap::new(),
		}
	}

	pub(super) async fn poll(&mut self, server: &Server, sender: &Sender) -> tg::Result<()> {
		// Wait for the object index outbox.
		self.poll_object_index_outbox(server).await?;

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

	async fn poll_object_index_outbox(&mut self, server: &Server) -> tg::Result<()> {
		if server.config.advanced.single_process {
			return Ok(());
		}
		let config = &server.config.object.index_outbox;

		// Poll the active cohort.
		if let Some(batch) = self.object_index_outbox_batch_id {
			let arg = crate::store::object::index::outbox::batch::get::Arg {
				batch: Some(batch),
				partition_end: config.partition_total,
				partition_start: 0,
			};
			let batch = server
				.store
				.try_get_object_index_outbox_batch_at_or_before(arg)
				.await
				.map_err(|error| tg::error!(!error, "failed to poll the object index outbox"))?;
			if batch.is_some() {
				return Ok(());
			}
			for request in self.requests.values_mut() {
				if matches!(request.state, IndexRequestState::ObjectIndexOutboxPending) {
					request.state = IndexRequestState::DatabaseIndexOutbox;
				}
			}
			self.object_index_outbox_batch_id = None;

			return Ok(());
		}

		// Snapshot the next cohort.
		let snapshot = self
			.requests
			.values()
			.any(|request| matches!(request.state, IndexRequestState::ObjectIndexOutbox));
		if !snapshot {
			return Ok(());
		}
		let arg = crate::store::object::index::outbox::batch::get::Arg {
			batch: None,
			partition_end: config.partition_total,
			partition_start: 0,
		};
		let batch = server
			.store
			.try_get_object_index_outbox_batch_at_or_before(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to snapshot the object index outbox"))?;
		for request in self.requests.values_mut() {
			if !matches!(request.state, IndexRequestState::ObjectIndexOutbox) {
				continue;
			}
			request.state = if batch.is_some() {
				IndexRequestState::ObjectIndexOutboxPending
			} else {
				IndexRequestState::DatabaseIndexOutbox
			};
		}
		self.object_index_outbox_batch_id = batch;

		Ok(())
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
			for request in self.requests.values_mut() {
				if matches!(request.state, IndexRequestState::DatabaseIndexOutboxPending) {
					request.state = if server.config.indexer.log_compaction.enabled {
						IndexRequestState::LogCompactions {
							transaction_id: None,
						}
					} else {
						IndexRequestState::Updates {
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
			.requests
			.values()
			.any(|request| matches!(request.state, IndexRequestState::DatabaseIndexOutbox));
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
		for request in self.requests.values_mut() {
			if !matches!(request.state, IndexRequestState::DatabaseIndexOutbox) {
				continue;
			}
			request.state = if batch.is_some() {
				IndexRequestState::DatabaseIndexOutboxPending
			} else if server.config.indexer.log_compaction.enabled {
				IndexRequestState::LogCompactions {
					transaction_id: None,
				}
			} else {
				IndexRequestState::Updates {
					transaction_id: None,
				}
			};
		}
		self.database_index_outbox_batch_id = batch;

		Ok(())
	}

	async fn set_log_compaction_targets(&mut self, server: &Server) -> tg::Result<()> {
		let set_target = self.requests.values().any(|request| {
			matches!(
				request.state,
				IndexRequestState::LogCompactions {
					transaction_id: None
				}
			)
		});
		if !set_target {
			return Ok(());
		}
		let transaction_id = server.index.get_transaction_id().await?;
		for request in self.requests.values_mut() {
			if let IndexRequestState::LogCompactions {
				transaction_id: target @ None,
			} = &mut request.state
			{
				*target = Some(transaction_id);
			}
		}

		Ok(())
	}

	async fn poll_log_compactions(&mut self, server: &Server) -> tg::Result<()> {
		let poll = self.requests.values().any(|request| {
			matches!(
				request.state,
				IndexRequestState::LogCompactions {
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
		for request in self.requests.values_mut() {
			let IndexRequestState::LogCompactions {
				transaction_id: Some(transaction_id),
			} = request.state
			else {
				continue;
			};
			if oldest.is_none_or(|oldest| oldest > transaction_id) {
				request.state = IndexRequestState::Updates {
					transaction_id: None,
				};
			}
		}

		Ok(())
	}

	pub(super) fn start_barrier(&mut self, server: &Server) {
		if !self.barriers.is_empty() {
			return;
		}
		let ids = self
			.requests
			.iter()
			.filter(|(_, request)| matches!(request.state, IndexRequestState::Tasks))
			.map(|(id, _)| id.clone())
			.collect::<Vec<_>>();
		if ids.is_empty() {
			return;
		}
		let server = server.clone();
		self.barriers.push(
			async move {
				let request = ids.first().unwrap().clone();
				crate::checkpoint!(server, "indexer.request.barrier", request,).await;
				server.remote_object_put_tasks.wait().await;
				server.index_tasks.wait().await;

				ids
			}
			.boxed(),
		);
	}

	pub(super) fn handle_barrier(&mut self, ids: Vec<String>, object_index_outbox: bool) {
		for id in ids {
			let Some(request) = self.requests.get_mut(&id) else {
				continue;
			};
			if matches!(request.state, IndexRequestState::Tasks) {
				request.state = if object_index_outbox {
					IndexRequestState::ObjectIndexOutbox
				} else {
					IndexRequestState::DatabaseIndexOutbox
				};
			}
		}
	}

	async fn set_update_targets(&mut self, server: &Server) -> tg::Result<()> {
		let set_target = self.requests.values().any(|request| {
			matches!(
				request.state,
				IndexRequestState::Updates {
					transaction_id: None
				}
			)
		});
		if !set_target {
			return Ok(());
		}
		let transaction_id = server.index.get_transaction_id().await?;
		for request in self.requests.values_mut() {
			if let IndexRequestState::Updates {
				transaction_id: target @ None,
			} = &mut request.state
			{
				*target = Some(transaction_id);
			}
		}

		Ok(())
	}

	async fn poll_updates(&mut self, server: &Server, sender: &Sender) -> tg::Result<()> {
		let poll = self.requests.values().any(|request| {
			matches!(
				request.state,
				IndexRequestState::Updates {
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
			.requests
			.iter()
			.filter_map(|(id, request)| {
				let IndexRequestState::Updates {
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
			self.requests.remove(&id);
			Self::send_response(id, Ok(ResponseOutput::Index), sender);
		}

		Ok(())
	}

	pub(super) fn fail(&mut self, error: &tg::Error, sender: &Sender) {
		let error = error.to_string();
		self.database_index_outbox_batch_id = None;
		self.object_index_outbox_batch_id = None;
		let ids = std::mem::take(&mut self.requests).into_keys();
		for id in ids {
			Self::send_response(
				id,
				Err(tg::error!(error = %error, "failed to wait for indexing")),
				sender,
			);
		}
	}

	fn send_response(id: String, result: tg::Result<ResponseOutput>, sender: &Sender) {
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
		self.requests.values().any(|request| {
			matches!(
				request.state,
				IndexRequestState::DatabaseIndexOutbox
					| IndexRequestState::DatabaseIndexOutboxPending
					| IndexRequestState::LogCompactions { .. }
					| IndexRequestState::ObjectIndexOutbox
					| IndexRequestState::ObjectIndexOutboxPending
					| IndexRequestState::Updates { .. }
			)
		})
	}
}
