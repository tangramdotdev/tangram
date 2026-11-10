use {
	super::{Indexer, IndexerState, queue, wait},
	crate::Component,
	std::{
		ops::ControlFlow,
		sync::{Arc, Mutex},
	},
	tangram_client::prelude::*,
	tangram_server_core::{Server, service},
};

#[cfg(test)]
mod tests;

pub(crate) type Request = service::Request<RequestArg>;

type Response = service::Response<ResponseOutput>;

type Sender = service::Sender<Kind>;

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub(crate) enum RequestArg {
	#[tangram_serialize(id = 0)]
	Enqueue(EnqueueRequestArg),

	#[tangram_serialize(id = 1)]
	Wait,
}

/// A serialized archive arg and a serialized index write arg to write to the cache's queues as one entry each.
#[derive(Clone, Debug, Default, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub(crate) struct EnqueueRequestArg {
	#[tangram_serialize(default, id = 0, skip_serializing_if = "Option::is_none")]
	pub archive: Option<bytes::Bytes>,

	#[tangram_serialize(default, id = 1, skip_serializing_if = "Option::is_none")]
	pub index: Option<bytes::Bytes>,
}

#[derive(
	Clone,
	Debug,
	derive_more::TryUnwrap,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub(crate) enum ResponseOutput {
	#[tangram_serialize(id = 0)]
	Enqueue,

	#[tangram_serialize(id = 1)]
	Wait,
}

/// The indexer kind of server.
pub(crate) struct Kind;

/// The handler that runs each accepted request concurrently.
pub(super) struct Handler {
	pub changed: Arc<tokio::sync::Notify>,
	pub indexer: Indexer,
	pub requests: tokio::task::JoinSet<tg::Result<()>>,
	pub state: Arc<Mutex<IndexerState>>,
	pub wait_sender: wait::Sender,
	pub write_sender: queue::write::Sender,
}

struct HandleIndexerRequestArgs {
	guard: Guard,
	request: Request,
	sender: Sender,
	wait_sender: wait::Sender,
	write_sender: queue::write::Sender,
}

pub(super) struct Guard {
	changed: Arc<tokio::sync::Notify>,
	enqueue: bool,
	state: Arc<Mutex<IndexerState>>,
}

impl Component {
	pub(crate) async fn send_indexer_request(
		&self,
		server: &Server,
		indexer: &tg::indexer::Id,
		arg: RequestArg,
	) -> tg::Result<tg::Result<ResponseOutput>> {
		// Reuse the ID so a retry does not replace an active wait with a later barrier.
		let id = tangram_server_core::control::id();
		let retry = server.config.indexer.request.retry.clone().into();
		let timeout =
			matches!(arg, RequestArg::Wait).then_some(server.config.indexer.request.response_ttl);
		tangram_futures::retry(&retry, || {
			let arg = arg.clone();
			let id = &id;
			async move {
				let request = self.send_indexer_request_inner(server, indexer, id, arg);
				let response = if let Some(timeout) = timeout {
					match tokio::time::timeout(timeout, request).await {
						Ok(response) => response?,
						Err(source) => {
							return Ok(ControlFlow::Continue(tg::error!(
								!source,
								"timed out waiting for the indexer response"
							)));
						},
					}
				} else {
					request.await?
				};
				let Some(response) = response else {
					return Err(tg::error!(%indexer, "the indexer is unavailable"));
				};
				Ok(ControlFlow::Break(response))
			}
		})
		.await
	}

	async fn send_indexer_request_inner(
		&self,
		server: &Server,
		indexer: &tg::indexer::Id,
		id: &str,
		arg: RequestArg,
	) -> tg::Result<Option<tg::Result<ResponseOutput>>> {
		let config = &server.config.indexer.request;
		let options = tangram_server_core::control::Options {
			retry: config.retry.clone().into(),
			timeout: config.timeout,
		};
		service::send::<Kind>(server, indexer, id, arg, options).await
	}
}

impl Indexer {
	async fn handle_indexer_request(&self, args: HandleIndexerRequestArgs) -> tg::Result<()> {
		let HandleIndexerRequestArgs {
			mut guard,
			request,
			sender,
			wait_sender,
			write_sender,
		} = args;
		let result = match request.arg {
			RequestArg::Enqueue(arg) => {
				Self::handle_enqueue_request(&guard.state, &guard.changed, &write_sender, arg)
					.await
					.map(|()| ResponseOutput::Enqueue)
			},
			RequestArg::Wait => self
				.wait_for_indexing(&wait_sender, request.id.clone())
				.await
				.map(|()| ResponseOutput::Wait),
		};
		guard.finish();
		sender.send_now(service::ClientMessage::Response(Response::new(
			request.id, result,
		)));
		Ok(())
	}

	#[tracing::instrument(name = "indexer.enqueue", level = "debug", skip_all, fields(archive = arg.archive.as_ref().map_or(0, bytes::Bytes::len), index = arg.index.as_ref().map_or(0, bytes::Bytes::len)), err(level = "debug"))]
	async fn handle_enqueue_request(
		state: &Mutex<IndexerState>,
		changed: &tokio::sync::Notify,
		write_sender: &queue::write::Sender,
		arg: EnqueueRequestArg,
	) -> tg::Result<()> {
		// Send the request to the write task, which records its entries for processing and writes them to the cache's queues.
		let (assigned, assigned_receiver) = tokio::sync::oneshot::channel();
		let (written, written_receiver) = tokio::sync::oneshot::channel();
		let request = queue::write::Request {
			arg,
			assigned,
			written,
		};
		write_sender
			.send(request)
			.await
			.map_err(|_| tg::error!("the indexer write task stopped"))?;
		let sequences = assigned_receiver
			.await
			.map_err(|_| tg::error!("the indexer write task stopped"))?;

		// Accept the request when its entries are either written to the cache's queues or processed.
		let queued = async {
			written_receiver
				.await
				.map_err(|_| tg::error!("the indexer write task stopped"))?
		};
		let handled = Self::wait_for_processing(state, changed, &sequences);
		tangram_server_core::control::accept::accept(queued, handled).await?;

		Ok(())
	}

	async fn wait_for_processing(
		state: &Mutex<IndexerState>,
		changed: &tokio::sync::Notify,
		sequences: &[(queue::Kind, u64)],
	) -> tg::Result<()> {
		loop {
			let notified = changed.notified();
			let processed = {
				let state = state.lock().unwrap();
				sequences
					.iter()
					.all(|(kind, sequence)| state.queues.processing_finished(*kind, *sequence))
			};
			if processed {
				return Ok(());
			}
			notified.await;
		}
	}
}

impl service::Kind for Kind {
	type Arg = RequestArg;

	type Id = tg::indexer::Id;

	type Output = ResponseOutput;

	const NAME: &'static str = "indexer";

	async fn available(server: &Server) -> tg::Result<Vec<Self::Id>> {
		let indexers = Component::get_indexers(server).await?;
		let indexers = indexers
			.into_iter()
			.filter_map(|indexer| indexer.available.then_some(indexer.id))
			.collect();

		Ok(indexers)
	}
}

impl service::Handler<Kind> for Handler {
	type Accepted = Guard;

	fn accept(&mut self, request: &Request) -> Option<Self::Accepted> {
		// Accept a wait at any time, and accept an enqueue only while the indexer is available.
		let enqueue = !matches!(request.arg, RequestArg::Wait);
		let mut state = self.state.lock().unwrap();
		if enqueue && !state.available {
			return None;
		}
		if enqueue {
			state.enqueues += 1;
		}
		let guard = Guard {
			changed: self.changed.clone(),
			enqueue,
			state: self.state.clone(),
		};

		Some(guard)
	}

	async fn handle(&mut self, request: Request, guard: Guard, sender: Sender) -> tg::Result<()> {
		// Propagate the errors of the handlers that finished.
		while let Some(result) = self.requests.try_join_next() {
			result
				.map_err(|source| tg::error!(!source, "an indexer request handler panicked"))??;
		}

		// Spawn the handler.
		let indexer = self.indexer.clone();
		let args = HandleIndexerRequestArgs {
			guard,
			request,
			sender,
			wait_sender: self.wait_sender.clone(),
			write_sender: self.write_sender.clone(),
		};
		self.requests
			.spawn(async move { indexer.handle_indexer_request(args).await });

		Ok(())
	}

	async fn shutdown(&mut self) {
		// The queues are drained and indexing has finished when the shutdown task stops reception.
		self.requests.shutdown().await;
	}
}

impl Guard {
	fn finish(&mut self) {
		if self.enqueue {
			self.state
				.lock()
				.unwrap_or_else(std::sync::PoisonError::into_inner)
				.enqueues -= 1;
			self.enqueue = false;
			self.changed.notify_waiters();
		}
	}
}

impl Drop for Guard {
	fn drop(&mut self) {
		self.finish();
	}
}
