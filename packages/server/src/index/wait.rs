use {
	crate::Server,
	futures::{FutureExt as _, StreamExt as _, future, stream::FuturesUnordered},
	std::collections::BTreeMap,
	tangram_client::prelude::*,
	tangram_futures::task::{Stopper, Task},
	tangram_index::{self as index, prelude::*},
};

#[cfg(test)]
mod tests;

pub(crate) type Receiver = tokio::sync::mpsc::Receiver<Request>;
pub(crate) type Sender = tokio::sync::mpsc::Sender<Request>;

type IndexerWaits =
	FuturesUnordered<futures::future::BoxFuture<'static, (Vec<String>, tg::Result<()>)>>;

struct State {
	database_index_queue_batch_id: Option<crate::database::index::queue::BatchId>,
	indexer_waits: IndexerWaits,
	waits: BTreeMap<String, Request>,
}

pub(crate) struct Request {
	id: String,
	sender: tokio::sync::oneshot::Sender<tg::Result<()>>,
	state: RequestState,
}

#[derive(Clone, Copy)]
enum RequestState {
	Inputs {
		database_index_queue: Progress<()>,
		indexers: Progress<()>,
		log_compactions: Progress<u64>,
	},
	Updates {
		transaction_id: Option<u64>,
	},
}

#[derive(Clone, Copy)]
enum Progress<T> {
	Complete,
	Pending(T),
	Ready,
}

impl Server {
	pub(crate) async fn wait_for_indexing(&self) -> tg::Result<()> {
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request {
			id: crate::control::id(),
			sender,
			state: RequestState::new(self.config.indexer.log_compaction.enabled),
		};
		self.index_wait_sender
			.send(request)
			.await
			.map_err(|error| tg::error!(!error, "the index wait task stopped"))?;
		receiver
			.await
			.map_err(|error| tg::error!(!error, "the index wait task stopped"))??;
		Ok(())
	}

	pub(crate) async fn index_wait_task(&self, mut receiver: Receiver, stopper: Stopper) {
		let mut state = State::new();
		let mut interval = tokio::time::interval(self.config.indexer.request.poll_interval);
		interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
		loop {
			tokio::select! {
				() = stopper.wait() => return,
				request = receiver.recv(), if state.waits.len() < self.config.indexer.request.wait_concurrency => {
					let Some(request) = request else { return; };
					state.waits.insert(request.id.clone(), request);
				},
				Some((ids, result)) = state.indexer_waits.next(), if !state.indexer_waits.is_empty() => {
					state.handle_indexer_wait(ids, &result);
					let server = self.clone();
					state.start_indexer_wait(async move { server.wait_for_indexers().await });
				},
				_ = interval.tick(), if !state.waits.is_empty() => {
					state.remove_closed();
					let server = self.clone();
					state.start_indexer_wait(async move { server.wait_for_indexers().await });
					let result = tokio::select! {
						() = stopper.wait() => return,
						result = state.poll(self) => result,
					};
					if let Err(error) = result {
						state.fail(&error);
					}
				},
			}
		}
	}

	async fn wait_for_indexers(&self) -> tg::Result<()> {
		if self.config.advanced.single_process {
			self.send_indexer_request(None, crate::indexer::RequestArg::Wait)
				.await??
				.try_unwrap_wait()
				.map_err(|_| tg::error!("expected a wait response"))?;
			return Ok(());
		}
		let indexers = self.get_indexers().await?;
		if indexers.is_empty() {
			return Err(tg::error!("no indexers are available"));
		}
		future::try_join_all(
			indexers
				.iter()
				.map(|indexer| self.wait_for_indexer(&indexer.id)),
		)
		.await?;
		Ok(())
	}

	async fn wait_for_indexer(&self, indexer: &tg::indexer::Id) -> tg::Result<()> {
		let mut indexer = indexer.clone();
		loop {
			let result = {
				let request =
					self.send_indexer_request(Some(&indexer), crate::indexer::RequestArg::Wait);
				tokio::pin!(request);
				let mut interval = tokio::time::interval(self.config.indexer.cache.poll_interval);
				interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
				loop {
					tokio::select! {
						result = &mut request => break Some(result),
						_ = interval.tick() => {
							let arg = index::indexer::get::Arg { id: indexer.clone() };
							if self.index.try_get_indexer(arg).await?.is_none() {
								break None;
							}
						},
					}
				}
			};
			if let Some(Ok(Ok(output))) = result {
				output
					.try_unwrap_wait()
					.map_err(|_| tg::error!("expected a wait response"))?;
				crate::checkpoint!(self, "indexer.wait.complete", %indexer).await;
				return Ok(());
			}
			let arg = index::indexer::get::Arg {
				id: indexer.clone(),
			};
			if self.index.try_get_indexer(arg).await?.is_none() {
				// Retry the local wait through a surviving indexer.
				indexer = self
					.get_indexers()
					.await?
					.into_iter()
					.next()
					.ok_or_else(|| tg::error!("no indexers are available"))?
					.id;
				continue;
			}
			tokio::time::sleep(self.config.indexer.cache.poll_interval).await;
		}
	}
}

impl State {
	fn new() -> Self {
		Self {
			database_index_queue_batch_id: None,
			indexer_waits: IndexerWaits::new(),
			waits: BTreeMap::new(),
		}
	}

	fn fail(&mut self, error: &tg::Error) {
		self.database_index_queue_batch_id = None;
		self.indexer_waits.clear();
		for (_, request) in std::mem::take(&mut self.waits) {
			let error = error.clone();
			request
				.sender
				.send(Err(tg::error!(!error, "failed to await indexing")))
				.ok();
		}
	}

	fn remove_closed(&mut self) {
		self.waits.retain(|_, request| !request.sender.is_closed());
		if !self.waits.values().any(|request| {
			matches!(
				request.state,
				RequestState::Inputs {
					indexers: Progress::Pending(()),
					..
				}
			)
		}) {
			self.indexer_waits.clear();
		}
		if !self.waits.values().any(|request| {
			matches!(
				request.state,
				RequestState::Inputs {
					database_index_queue: Progress::Pending(()),
					..
				}
			)
		}) {
			self.database_index_queue_batch_id = None;
		}
	}

	fn start_indexer_wait(&mut self, wait: impl Future<Output = tg::Result<()>> + Send + 'static) {
		if !self.indexer_waits.is_empty() {
			return;
		}
		let ids = self
			.waits
			.iter_mut()
			.filter_map(|(id, request)| {
				let RequestState::Inputs {
					indexers: indexers @ Progress::Ready,
					..
				} = &mut request.state
				else {
					return None;
				};
				*indexers = Progress::Pending(());
				Some(id.clone())
			})
			.collect::<Vec<_>>();
		if ids.is_empty() {
			return;
		}
		// Keep the shared indexer requests running while the coordinator reads the databases.
		let task = Task::spawn(|_| wait);
		self.indexer_waits.push(
			async move {
				let result = task
					.wait()
					.await
					.map_err(|error| tg::error!(!error, "the indexer wait task failed"))
					.and_then(std::convert::identity);
				(ids, result)
			}
			.boxed(),
		);
	}

	fn handle_indexer_wait(&mut self, ids: Vec<String>, result: &tg::Result<()>) {
		for id in ids {
			if let Err(error) = result {
				if let Some(request) = self.waits.remove(&id) {
					request.sender.send(Err(error.clone())).ok();
				}
			} else if let Some(request) = self.waits.get_mut(&id)
				&& let RequestState::Inputs { indexers, .. } = &mut request.state
			{
				*indexers = Progress::Complete;
			}
		}
	}

	async fn poll(&mut self, server: &Server) -> tg::Result<()> {
		// These sources can create updates, but do not enqueue work for one another.
		let region = server.config.region.clone().unwrap_or_default();
		self.poll_inputs(
			|batch| async move {
				crate::checkpoint!(server, "index.wait.database_index_queue", ?batch).await;
				let arg = crate::database::index::queue::TryGetBatchArg { batch, region };
				server
					.database
					.try_get_index_queue_batch_at_or_before(arg)
					.await
			},
			async {
				crate::checkpoint!(server, "index.wait.compactions").await;
				server.index.get_transaction_id().await
			},
			server.index.try_get_oldest_log_compaction_transaction_id(),
		)
		.await?;

		// Share each global progress read across all active wait requests.
		self.set_update_target_transactions(server.index.get_transaction_id())
			.await?;
		self.poll_updates(|kind| async move {
			crate::checkpoint!(server, "index.wait.updates", ?kind).await;
			server
				.index
				.try_get_oldest_update_transaction_id(kind)
				.await
		})
		.await?;

		// Pending shared work needs a surviving indexer after the private waits finish.
		if !server.config.advanced.single_process
			&& self.waits.values().any(|request| {
				matches!(
					request.state,
					RequestState::Inputs {
						indexers: Progress::Complete,
						..
					} | RequestState::Updates { .. }
				)
			}) && server.indexers.is_empty()
			&& server.get_indexers().await?.is_empty()
		{
			return Err(tg::error!("no indexers are available"));
		}

		Ok(())
	}

	async fn poll_inputs<F>(
		&mut self,
		read_database_index_queue: impl FnOnce(Option<crate::database::index::queue::BatchId>) -> F,
		read_transaction_id: impl Future<Output = tg::Result<u64>>,
		read_log_compactions: impl Future<Output = tg::Result<Option<u64>>>,
	) -> tg::Result<()>
	where
		F: Future<Output = tg::Result<Option<crate::database::index::queue::BatchId>>>,
	{
		// Share the queue target with the active batch, and give later requests a fresh snapshot.
		let database_index_queue = async {
			let poll = self.waits.values().any(|request| {
				matches!(
					request.state,
					RequestState::Inputs {
						database_index_queue: Progress::Ready | Progress::Pending(()),
						..
					}
				)
			});
			if !poll {
				return Ok::<_, tg::Error>(None);
			}
			let batch = read_database_index_queue(self.database_index_queue_batch_id)
				.await
				.map_err(|error| tg::error!(!error, "failed to read the database index queue"))?;
			Ok(Some(batch))
		};

		// Share the compaction snapshot and progress read across all eligible requests.
		let log_compactions = async {
			let snapshot = self.waits.values().any(|request| {
				matches!(
					request.state,
					RequestState::Inputs {
						log_compactions: Progress::Ready,
						..
					}
				)
			});
			let poll = snapshot
				|| self.waits.values().any(|request| {
					matches!(
						request.state,
						RequestState::Inputs {
							log_compactions: Progress::Pending(_),
							..
						}
					)
				});
			if !poll {
				return Ok::<_, tg::Error>(None);
			}
			let transaction_id = if snapshot {
				Some(read_transaction_id.await?)
			} else {
				None
			};
			let oldest = read_log_compactions.await?;
			Ok(Some((transaction_id, oldest)))
		};
		let (database_index_queue, log_compactions) =
			future::try_join(database_index_queue, log_compactions).await?;

		// Advance each input independently without admitting later requests to an older snapshot.
		if let Some(batch) = database_index_queue {
			let pending = self.database_index_queue_batch_id.is_some();
			for request in self.waits.values_mut() {
				let RequestState::Inputs {
					database_index_queue,
					..
				} = &mut request.state
				else {
					continue;
				};
				if matches!(
					(*database_index_queue, pending),
					(Progress::Ready, false) | (Progress::Pending(()), true)
				) {
					*database_index_queue = if batch.is_some() {
						Progress::Pending(())
					} else {
						Progress::Complete
					};
				}
			}
			// A progress read may return an older batch, but the active cutoff must stay fixed.
			if !pending || batch.is_none() {
				self.database_index_queue_batch_id = batch;
			}
		}
		if let Some((transaction_id, oldest)) = log_compactions {
			for request in self.waits.values_mut() {
				let RequestState::Inputs {
					log_compactions, ..
				} = &mut request.state
				else {
					continue;
				};
				if matches!(log_compactions, Progress::Ready) {
					*log_compactions = Progress::Pending(transaction_id.unwrap());
				}
				if let Progress::Pending(transaction_id) = *log_compactions
					&& oldest.is_none_or(|oldest| oldest > transaction_id)
				{
					*log_compactions = Progress::Complete;
				}
			}
		}

		Ok(())
	}

	async fn set_update_target_transactions(
		&mut self,
		read: impl Future<Output = tg::Result<u64>>,
	) -> tg::Result<()> {
		// Capture the update cutoff only after all three inputs have completed.
		for request in self.waits.values_mut() {
			if matches!(
				request.state,
				RequestState::Inputs {
					database_index_queue: Progress::Complete,
					indexers: Progress::Complete,
					log_compactions: Progress::Complete,
				}
			) {
				request.state = RequestState::Updates {
					transaction_id: None,
				};
			}
		}
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

impl RequestState {
	fn new(log_compaction: bool) -> Self {
		Self::Inputs {
			database_index_queue: Progress::Ready,
			indexers: Progress::Ready,
			log_compactions: if log_compaction {
				Progress::Ready
			} else {
				Progress::Complete
			},
		}
	}
}
