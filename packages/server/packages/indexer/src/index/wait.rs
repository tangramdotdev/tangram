use {
	crate::Component,
	futures::{FutureExt as _, StreamExt as _, future, stream::FuturesUnordered},
	std::collections::BTreeMap,
	tangram_client::prelude::*,
	tangram_futures::task::{Stopper, Task},
	tangram_index::prelude::*,
	tangram_server_core::Server,
};

#[cfg(test)]
mod tests;

const SLOW_WAIT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

pub(crate) type Receiver = tokio::sync::mpsc::Receiver<WaitArg>;

pub(crate) type Sender = tokio::sync::mpsc::Sender<WaitArg>;

type IndexerWaits =
	FuturesUnordered<futures::future::BoxFuture<'static, (Vec<String>, tg::Result<()>)>>;

pub(crate) struct WaitArg {
	database: bool,
	sender: tokio::sync::oneshot::Sender<tg::Result<()>>,
}

struct State {
	database_index_queue_batch_id: Option<tangram_server_core::database::index::queue::BatchId>,

	#[allow(
		clippy::option_option,
		reason = "the outer option marks whether the database index queue progress has been read and the inner option is the batch that was read"
	)]
	database_index_queue_progress:
		Option<Option<tangram_server_core::database::index::queue::BatchId>>,

	indexer_waits: IndexerWaits,

	last_report: Option<tokio::time::Instant>,

	oldests: Option<Oldests>,

	// The time of the last completed wait, or of the first request since the waits were last empty.
	progressed: Option<tokio::time::Instant>,

	waits: BTreeMap<String, Request>,
}

pub(crate) struct Request {
	id: String,
	sender: tokio::sync::oneshot::Sender<tg::Result<()>>,
	started: tokio::time::Instant,
	state: RequestState,
}

#[derive(Clone, Copy, Debug)]
enum RequestState {
	Inputs {
		database_index_queue: Progress<()>,
		indexers: Progress<()>,
	},
	Updates {
		transaction_id: Option<u64>,
	},
}

#[derive(Clone, Copy, Debug)]
enum Progress<T> {
	Complete,
	Pending(T),
	Ready,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Oldests {
	permission: Option<u64>,
	storage_and_metadata: Option<u64>,
	usage: Option<u64>,
}

impl Component {
	#[tracing::instrument(name = "index.wait", level = "info", skip_all, err)]
	pub(crate) async fn index_inner(&self, database: bool) -> tg::Result<()> {
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let arg = WaitArg { database, sender };
		self.index_wait_sender
			.send(arg)
			.await
			.map_err(|error| tg::error!(!error, "the index wait task stopped"))?;
		receiver
			.await
			.map_err(|error| tg::error!(!error, "the index wait task stopped"))??;
		Ok(())
	}

	pub(crate) async fn index_wait_task(
		&self,
		server: &Server,
		mut receiver: Receiver,
		stopper: Stopper,
	) {
		let mut state = State::new();
		let mut interval = tokio::time::interval(server.config.indexer.request.poll_interval);
		interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
		loop {
			// Register before reading progress so changes during the reads wake the next pass.
			let changed = self.index_changed.notified();
			tokio::pin!(changed);
			changed.as_mut().enable();

			// Advance every request as far as the current index state allows.
			if !state.waits.is_empty() {
				state.remove_closed();
				state.start_indexer_wait({
					let component = self.clone();
					let server = server.clone();
					async move { component.wait_for_indexers(&server).await }
				});
				let result = {
					let started = tokio::time::Instant::now();
					let poll = state.poll(server, &self.indexers);
					tokio::pin!(poll);
					loop {
						// Warn while the progress reads run longer than the slow wait interval, since the waits cannot be reported until the reads return.
						tokio::select! {
							() = stopper.wait() => return,
							result = &mut poll => break result,
							() = tokio::time::sleep(SLOW_WAIT_INTERVAL) => {
								tracing::warn!(elapsed = ?started.elapsed(), "the index wait progress reads are slow");
							},
						}
					}
				};
				if let Err(error) = result {
					state.fail(&error);
				}
			}

			// Wake on new requests, indexer completion, index changes, the next slow wait warning, or the fallback tick.
			let deadline = state.stall().map(|(_, deadline)| deadline);
			let slow = async move {
				match deadline {
					Some(deadline) => tokio::time::sleep_until(deadline).await,
					None => future::pending().await,
				}
			};
			tokio::select! {
				() = stopper.wait() => return,
				request = receiver.recv(), if state.waits.len() < server.config.indexer.request.wait_concurrency => {
					let Some(WaitArg { database, sender }) = request else { return; };
					let request = Request {
						id: tangram_server_core::control::id(),
						sender,
						started: tokio::time::Instant::now(),
						state: RequestState::new(database),
					};
					tracing::debug!(id = %request.id, state = ?request.state, "started an index wait");
					if state.waits.is_empty() {
						state.progressed = Some(request.started);
					}
					state.waits.insert(request.id.clone(), request);
				},
				Some((ids, result)) = state.indexer_waits.next(), if !state.indexer_waits.is_empty() => {
					state.handle_indexer_wait(ids, &result);
				},
				() = &mut changed, if !state.waits.is_empty() => {},
				() = slow => {},
				_ = interval.tick(), if !state.waits.is_empty() => {},
			}
		}
	}

	async fn wait_for_indexers(&self, server: &Server) -> tg::Result<()> {
		let indexers = Component::get_indexers(server).await?;
		if indexers.is_empty() {
			return Err(tg::error!("no indexers are available"));
		}
		future::try_join_all(
			indexers
				.iter()
				.map(|indexer| self.wait_for_indexer(server, &indexer.id)),
		)
		.await?;
		Ok(())
	}

	async fn wait_for_indexer(&self, server: &Server, indexer: &tg::indexer::Id) -> tg::Result<()> {
		// Send the wait with one request ID until the indexer answers, so that a resend does not replace an active wait with a later barrier.
		let config = &server.config.indexer.request;
		let retry = config.retry.clone().into();
		let output = tangram_server_core::service::retry_to(
			&retry,
			config.response_time_to_live,
			|id| async move {
				self.send_indexer_request(server, indexer, &id, crate::request::RequestArg::Wait)
					.await
			},
		)
		.await?;
		output
			.map_err(|source| tg::error!(!source, %indexer, "the indexer failed to wait"))?
			.try_unwrap_wait()
			.map_err(|_| tg::error!("expected a wait response"))?;
		tangram_server_core::checkpoint!(server, "indexer.wait.complete", %indexer).await;

		Ok(())
	}
}

impl State {
	fn new() -> Self {
		Self {
			database_index_queue_batch_id: None,
			database_index_queue_progress: None,
			indexer_waits: IndexerWaits::new(),
			last_report: None,
			oldests: None,
			progressed: None,
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

	async fn poll(
		&mut self,
		server: &Server,
		indexers: &tangram_server_core::service::Cache<crate::request::Kind>,
	) -> tg::Result<()> {
		let region = server.config.region.clone().unwrap_or_default();
		self.poll_inputs(|batch| async move {
			tangram_server_core::checkpoint!(server, "index.wait.database_index_queue", ?batch)
				.await;
			let arg = tangram_server_core::database::index::queue::TryGetBatchArg { batch, region };
			server
				.database
				.try_get_index_queue_batch_at_or_before(arg)
				.await
		})
		.await?;

		// Share each global progress read across all active wait requests.
		self.set_update_target_transactions(server.index.get_transaction_id())
			.await?;
		self.poll_updates(|kind| async move {
			tangram_server_core::checkpoint!(server, "index.wait.updates", ?kind).await;
			server
				.index
				.try_get_oldest_update_transaction_id(kind)
				.await
		})
		.await?;

		self.warn_slow_waits(tokio::time::Instant::now());

		// Pending shared work needs a surviving indexer after the private waits finish.
		if server.config.advanced.multi_server
			&& self.waits.values().any(|request| {
				matches!(
					request.state,
					RequestState::Inputs {
						indexers: Progress::Complete,
						..
					} | RequestState::Updates { .. }
				)
			})
			&& indexers.is_empty()
			&& Component::get_indexers(server).await?.is_empty()
		{
			return Err(tg::error!("no indexers are available"));
		}

		Ok(())
	}

	async fn poll_inputs<F>(
		&mut self,
		read_database_index_queue: impl FnOnce(
			Option<tangram_server_core::database::index::queue::BatchId>,
		) -> F,
	) -> tg::Result<()>
	where
		F: Future<
			Output = tg::Result<Option<tangram_server_core::database::index::queue::BatchId>>,
		>,
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

		let database_index_queue = database_index_queue.await?;

		// Advance each input independently without admitting later requests to an older snapshot.
		if let Some(batch) = database_index_queue {
			// Log the database index queue progress only when it changes.
			if self.database_index_queue_progress != Some(batch) {
				tracing::debug!(?batch, "read the database index queue progress");
				self.database_index_queue_progress = Some(batch);
			}

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

		Ok(())
	}

	async fn set_update_target_transactions(
		&mut self,
		read: impl Future<Output = tg::Result<u64>>,
	) -> tg::Result<()> {
		// Capture the update cutoff only after both inputs have completed.
		for request in self.waits.values_mut() {
			if matches!(
				request.state,
				RequestState::Inputs {
					database_index_queue: Progress::Complete,
					indexers: Progress::Complete,
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
				tracing::debug!(id = %request.id, transaction_id, "waiting for index updates");
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
		let (permission, storage_and_metadata, usage) = future::try_join3(
			read(tangram_index::update::Kind::Permission),
			read(tangram_index::update::Kind::StorageAndMetadata),
			read(tangram_index::update::Kind::Usage),
		)
		.await?;
		let oldests = Oldests {
			permission,
			storage_and_metadata,
			usage,
		};

		// Log the oldest updates only when they change, so that the log follows indexing progress rather than the poll rate.
		if self.oldests != Some(oldests) {
			tracing::debug!(?oldests, "read the oldest index updates");
			self.oldests = Some(oldests);
		}

		// Complete the requests whose target transactions have been indexed.
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
				[
					oldests.permission,
					oldests.storage_and_metadata,
					oldests.usage,
				]
				.into_iter()
				.all(|oldest| oldest.is_none_or(|oldest| oldest > transaction_id))
				.then(|| id.clone())
			})
			.collect::<Vec<_>>();
		for id in ids {
			// Record the completion only when the caller receives it, so that an abandoned wait does not restart the stall clock.
			if let Some(request) = self.waits.remove(&id)
				&& request.sender.send(Ok(())).is_ok()
			{
				tracing::debug!(%id, elapsed = ?request.started.elapsed(), "finished an index wait");
				self.progressed = Some(tokio::time::Instant::now());
			}
		}

		Ok(())
	}

	fn stall(&self) -> Option<(tokio::time::Instant, tokio::time::Instant)> {
		// Measure from the oldest wait, or from the last completed wait if the waits have gone without one completing since before then.
		let started = self.waits.values().map(|request| request.started).min()?;
		let started = self
			.progressed
			.map_or(started, |progressed| progressed.min(started));

		// Warn once the interval has passed, and at most once per interval.
		let deadline = started + SLOW_WAIT_INTERVAL;
		let deadline = self.last_report.map_or(deadline, |last_report| {
			deadline.max(last_report + SLOW_WAIT_INTERVAL)
		});

		Some((started, deadline))
	}

	fn warn_slow_waits(&mut self, now: tokio::time::Instant) -> bool {
		let Some((started, deadline)) = self.stall() else {
			return false;
		};
		if now < deadline {
			return false;
		}
		let elapsed = now.duration_since(started);
		self.last_report = Some(now);

		// Count the waits at each stage, and find the earliest update target.
		let mut database_index_queue = 0;
		let mut indexers = 0;
		let mut updates = 0;
		let mut transaction_id = None::<u64>;
		for request in self.waits.values() {
			match request.state {
				RequestState::Inputs {
					database_index_queue: database_index_queue_progress,
					indexers: indexers_progress,
				} => {
					database_index_queue +=
						usize::from(!matches!(database_index_queue_progress, Progress::Complete));
					indexers += usize::from(!matches!(indexers_progress, Progress::Complete));
				},
				RequestState::Updates {
					transaction_id: target,
				} => {
					updates += 1;
					transaction_id = transaction_id.into_iter().chain(target).min();
				},
			}
		}

		// Include the oldest updates only when a request is waiting on them, since they are read only for those requests.
		let oldests = self.oldests.filter(|_| updates > 0);
		tracing::warn!(
			database_index_queue,
			?elapsed,
			indexers,
			?oldests,
			?transaction_id,
			updates,
			waits = self.waits.len(),
			"the index waits are slow",
		);

		true
	}
}

impl RequestState {
	fn new(database: bool) -> Self {
		let database_index_queue = if database {
			Progress::Ready
		} else {
			Progress::Complete
		};
		Self::Inputs {
			database_index_queue,
			indexers: Progress::Ready,
		}
	}
}
