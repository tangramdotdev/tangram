use {
	crate::{Server, indexer::Indexer},
	futures::{FutureExt as _, StreamExt as _, stream::FuturesUnordered},
	std::{collections::BTreeMap, sync::Mutex},
	tangram_client::prelude::*,
	tangram_futures::task::Stopper,
};

#[cfg(test)]
mod tests;

pub(super) type Receiver = tokio::sync::mpsc::Receiver<Request>;
pub(super) type Sender = tokio::sync::mpsc::Sender<Request>;

type TaskWaits = FuturesUnordered<futures::future::BoxFuture<'static, Vec<String>>>;

struct State {
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
	Queues,
	QueuesPending {
		archive_sequence: u64,
		index_sequence: u64,
	},
	Tasks,
	TasksPending,
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
					if !state.waits.values().any(|request| matches!(request.state, RequestState::TasksPending)) {
						state.task_waits.clear();
					}
					state.start_task_wait(&self.server);
					let (read, target) = {
						let state = queues.lock().unwrap();
						(state.queues.read_sequences(), state.queues.target_sequences())
					};
					state.poll_queues(
						self.server.config.advanced.single_process,
						read,
						target,
					);
				},
			}
		}
	}
}

impl State {
	fn new() -> Self {
		Self {
			task_waits: TaskWaits::new(),
			waits: BTreeMap::new(),
		}
	}

	fn poll_queues(&mut self, single_process: bool, read: (u64, u64), target: (u64, u64)) {
		// Capture both private queue cutoffs for the next batch of requests.
		for request in self.waits.values_mut() {
			if matches!(request.state, RequestState::Queues) {
				request.state = RequestState::QueuesPending {
					archive_sequence: target.0,
					index_sequence: target.1,
				};
			}
		}

		// Finish each request once both queues have passed its cutoffs.
		let ids = self
			.waits
			.iter()
			.filter_map(|(id, request)| {
				let RequestState::QueuesPending {
					archive_sequence,
					index_sequence,
				} = request.state
				else {
					return None;
				};
				(single_process || (read.0 >= archive_sequence && read.1 >= index_sequence))
					.then(|| id.clone())
			})
			.collect::<Vec<_>>();
		for id in ids {
			if let Some(request) = self.waits.remove(&id) {
				request.sender.send(Ok(())).ok();
			}
		}
	}

	fn start_task_wait(&mut self, server: &Server) {
		if !self.task_waits.is_empty() {
			return;
		}
		let ids = self
			.waits
			.iter_mut()
			.filter_map(|(id, request)| {
				if !matches!(request.state, RequestState::Tasks) {
					return None;
				}
				request.state = RequestState::TasksPending;
				Some(id.clone())
			})
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
			if matches!(request.state, RequestState::TasksPending) {
				request.state = RequestState::Queues;
			}
		}
	}
}
