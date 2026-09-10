use {
	crate::{Server, indexer::Indexer},
	futures::{FutureExt as _, StreamExt as _, future, stream::FuturesUnordered},
	std::{collections::BTreeMap, sync::Mutex},
	tangram_client::prelude::*,
	tangram_futures::task::Stopper,
	tangram_index::prelude::*,
};

#[cfg(test)]
mod tests;

pub(super) type Receiver = tokio::sync::mpsc::Receiver<Request>;
pub(super) type Sender = tokio::sync::mpsc::Sender<Request>;

type TaskWaits = FuturesUnordered<futures::future::BoxFuture<'static, Vec<String>>>;

struct State {
	database_index_outbox_batch_id: Option<crate::database::index::outbox::BatchId>,
	task_waits: TaskWaits,
	waits: BTreeMap<String, Request>,
}

pub(super) struct Request {
	id: String,
	sender: tokio::sync::oneshot::Sender<tg::Result<()>>,
	state: RequestState,
}

#[derive(Clone, Copy)]
pub(super) enum RequestState {
	DatabaseIndexOutbox,
	DatabaseIndexOutboxPending,
	IndexQueue,
	IndexQueuePending { sequence: u64 },
	LogCompactions { transaction_id: Option<u64> },
	Tasks,
	Updates { transaction_id: Option<u64> },
}

impl Server {
	pub(crate) async fn wait_for_indexing_local(&self) -> tg::Result<()> {
		self.send_indexer_request(None, super::RequestArg::Wait)
			.await??
			.try_unwrap_wait()
			.map_err(|_| tg::error!("expected a wait response"))?;
		Ok(())
	}
}

impl Indexer {
	pub(in crate::indexer) async fn wait_for_indexing(
		&self,
		wait_sender: &Sender,
		id: String,
	) -> tg::Result<()> {
		crate::checkpoint!(self.server, "indexer.request.receive", request = id.clone()).await;
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request {
			id,
			sender,
			state: RequestState::Tasks,
		};
		wait_sender
			.send(request)
			.await
			.map_err(|_| tg::error!("the indexer wait task stopped"))?;
		receiver
			.await
			.map_err(|_| tg::error!("the indexer wait task stopped"))??;
		Ok(())
	}

	pub(super) async fn wait_task(
		&self,
		queues: &Mutex<super::State>,
		mut receiver: Receiver,
		stopper: Stopper,
	) -> tg::Result<()> {
		let mut state = State::new();
		let mut interval = tokio::time::interval(self.server.config.indexer.request.poll_interval);
		interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
		loop {
			tokio::select! {
				() = stopper.wait() => return Ok(()),
				request = receiver.recv(), if state.waits.len() < self.server.config.indexer.request.wait_concurrency => {
					let request = request.ok_or_else(|| tg::error!("the indexer wait request channel closed"))?;
					state.waits.insert(request.id.clone(), request);
				},
				Some(ids) = state.task_waits.next(), if !state.task_waits.is_empty() => {
					state.handle_task_wait(ids);
					state.start_task_wait(&self.server);
				},
				_ = interval.tick(), if !state.waits.is_empty() => {
					state.waits.retain(|_, request| !request.sender.is_closed());
					state.start_task_wait(&self.server);
					let result = tokio::select! {
						() = stopper.wait() => return Ok(()),
						result = state.poll(&self.server, queues) => result,
					};
					if let Err(error) = result {
						state.fail(&error);
					}
				},
			}
		}
	}
}

impl State {
	fn fail(&mut self, error: &tg::Error) {
		self.database_index_outbox_batch_id = None;
		self.task_waits.clear();
		for (_, request) in std::mem::take(&mut self.waits) {
			let error = error.clone();
			request
				.sender
				.send(Err(tg::error!(!error, "failed to await indexing")))
				.ok();
		}
	}

	fn new() -> Self {
		Self {
			database_index_outbox_batch_id: None,
			task_waits: TaskWaits::new(),
			waits: BTreeMap::new(),
		}
	}
	async fn poll(&mut self, server: &Server, queues: &Mutex<super::State>) -> tg::Result<()> {
		// Wait for the index queue.
		let (read_sequence, target_sequence) = {
			let state = queues.lock().unwrap();
			(
				state.queues.index_read_sequence(),
				state.queues.index_target_sequence(),
			)
		};
		self.poll_index_queue(
			server.config.advanced.single_process,
			read_sequence,
			target_sequence,
		);

		// Wait for the database index outbox.
		let region = server.config.region.clone().unwrap_or_default();
		self.poll_database_index_outbox(server.config.indexer.log_compaction.enabled, |batch| {
			let arg = crate::database::index::outbox::TryGetBatchArg {
				batch,
				region: region.clone(),
			};
			server.database.try_get_index_outbox_batch_at_or_before(arg)
		})
		.await?;

		// Wait for the log compaction queue.
		self.set_log_compaction_target_transactions(server.index.get_transaction_id())
			.await?;
		self.poll_log_compactions(server.index.try_get_oldest_log_compaction_transaction_id())
			.await?;

		// Wait for the index update queue.
		self.set_update_target_transactions(server.index.get_transaction_id())
			.await?;
		self.poll_updates(|kind| server.index.try_get_oldest_update_transaction_id(kind))
			.await?;

		Ok(())
	}

	fn poll_index_queue(&mut self, single_process: bool, read_sequence: u64, target_sequence: u64) {
		if single_process {
			for request in self.waits.values_mut() {
				if matches!(request.state, RequestState::IndexQueue) {
					request.state = RequestState::DatabaseIndexOutbox;
				}
			}
			return;
		}

		// Snapshot the next batch of requests.
		if self
			.waits
			.values()
			.any(|request| matches!(request.state, RequestState::IndexQueue))
		{
			let sequence = target_sequence;
			for request in self.waits.values_mut() {
				if matches!(request.state, RequestState::IndexQueue) {
					request.state = RequestState::IndexQueuePending { sequence };
				}
			}
		}

		// Poll the active batch of requests.
		for request in self.waits.values_mut() {
			let RequestState::IndexQueuePending { sequence } = request.state else {
				continue;
			};
			if read_sequence >= sequence {
				request.state = RequestState::DatabaseIndexOutbox;
			}
		}
	}

	async fn poll_database_index_outbox<F>(
		&mut self,
		log_compaction: bool,
		read: impl Fn(Option<crate::database::index::outbox::BatchId>) -> F,
	) -> tg::Result<()>
	where
		F: Future<Output = tg::Result<Option<crate::database::index::outbox::BatchId>>>,
	{
		// Poll the active batch of requests.
		if let Some(batch) = self.database_index_outbox_batch_id {
			let batch = read(Some(batch))
				.await
				.map_err(|error| tg::error!(!error, "failed to poll the database index outbox"))?;
			if batch.is_some() {
				return Ok(());
			}
			for request in self.waits.values_mut() {
				if matches!(request.state, RequestState::DatabaseIndexOutboxPending) {
					request.state = if log_compaction {
						RequestState::LogCompactions {
							transaction_id: None,
						}
					} else {
						RequestState::Updates {
							transaction_id: None,
						}
					};
				}
			}
			self.database_index_outbox_batch_id = None;

			return Ok(());
		}

		// Snapshot the next batch of requests.
		let snapshot = self
			.waits
			.values()
			.any(|request| matches!(request.state, RequestState::DatabaseIndexOutbox));
		if !snapshot {
			return Ok(());
		}
		let batch = read(None)
			.await
			.map_err(|error| tg::error!(!error, "failed to snapshot the database index outbox"))?;
		for request in self.waits.values_mut() {
			if !matches!(request.state, RequestState::DatabaseIndexOutbox) {
				continue;
			}
			request.state = if batch.is_some() {
				RequestState::DatabaseIndexOutboxPending
			} else if log_compaction {
				RequestState::LogCompactions {
					transaction_id: None,
				}
			} else {
				RequestState::Updates {
					transaction_id: None,
				}
			};
		}
		self.database_index_outbox_batch_id = batch;

		Ok(())
	}

	async fn set_log_compaction_target_transactions(
		&mut self,
		read: impl Future<Output = tg::Result<u64>>,
	) -> tg::Result<()> {
		let set_transaction = self.waits.values().any(|request| {
			matches!(
				request.state,
				RequestState::LogCompactions {
					transaction_id: None
				}
			)
		});
		if !set_transaction {
			return Ok(());
		}
		let transaction_id = read.await?;
		for request in self.waits.values_mut() {
			if let RequestState::LogCompactions {
				transaction_id: transaction @ None,
			} = &mut request.state
			{
				*transaction = Some(transaction_id);
			}
		}

		Ok(())
	}

	async fn poll_log_compactions(
		&mut self,
		read: impl Future<Output = tg::Result<Option<u64>>>,
	) -> tg::Result<()> {
		let poll = self.waits.values().any(|request| {
			matches!(
				request.state,
				RequestState::LogCompactions {
					transaction_id: Some(_)
				}
			)
		});
		if !poll {
			return Ok(());
		}
		let oldest = read.await?;
		for request in self.waits.values_mut() {
			let RequestState::LogCompactions {
				transaction_id: Some(transaction_id),
			} = request.state
			else {
				continue;
			};
			if oldest.is_none_or(|oldest| oldest > transaction_id) {
				request.state = RequestState::Updates {
					transaction_id: None,
				};
			}
		}

		Ok(())
	}

	fn start_task_wait(&mut self, server: &Server) {
		if !self.task_waits.is_empty() {
			return;
		}
		let ids = self
			.waits
			.iter()
			.filter(|(_, request)| matches!(request.state, RequestState::Tasks))
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
				server.archive_tasks.wait().await;

				ids
			}
			.boxed(),
		);
	}

	fn handle_task_wait(&mut self, ids: Vec<String>) {
		for id in ids {
			let Some(request) = self.waits.get_mut(&id) else {
				continue;
			};
			if matches!(request.state, RequestState::Tasks) {
				request.state = RequestState::IndexQueue;
			}
		}
	}

	async fn set_update_target_transactions(
		&mut self,
		read: impl Future<Output = tg::Result<u64>>,
	) -> tg::Result<()> {
		let set_transaction = self.waits.values().any(|request| {
			matches!(
				request.state,
				RequestState::Updates {
					transaction_id: None
				}
			)
		});
		if !set_transaction {
			return Ok(());
		}
		let transaction_id = read.await?;
		for request in self.waits.values_mut() {
			if let RequestState::Updates {
				transaction_id: transaction @ None,
			} = &mut request.state
			{
				*transaction = Some(transaction_id);
			}
		}

		Ok(())
	}

	async fn poll_updates<F>(
		&mut self,
		read: impl Fn(tangram_index::update::Kind) -> F,
	) -> tg::Result<()>
	where
		F: Future<Output = tg::Result<Option<u64>>>,
	{
		let poll = self.waits.values().any(|request| {
			matches!(
				request.state,
				RequestState::Updates {
					transaction_id: Some(_)
				}
			)
		});
		if !poll {
			return Ok(());
		}
		let oldests = future::try_join3(
			read(tangram_index::update::Kind::Grant),
			read(tangram_index::update::Kind::Node),
			read(tangram_index::update::Kind::Storage),
		)
		.await?;
		let ids = self
			.waits
			.iter()
			.filter_map(|(id, request)| {
				let RequestState::Updates {
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
			if let Some(request) = self.waits.remove(&id) {
				request.sender.send(Ok(())).ok();
			}
		}

		Ok(())
	}
}
