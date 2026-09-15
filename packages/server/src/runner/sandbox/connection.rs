use {
	crate::{Context, Session},
	futures::{FutureExt as _, StreamExt as _, stream::BoxStream},
	std::{collections::VecDeque, sync::Mutex},
	tangram_client::prelude::*,
	tangram_futures::task::Task,
};

/// A pool of sandbox control connections whose IDs the control server has assigned but not indexed.
pub(in crate::runner) struct Pool {
	inner: Mutex<VecDeque<Task<tg::Result<Entry>>>>,
	size: usize,
}

pub(in crate::runner) struct Entry {
	pub id: tg::sandbox::Id,
	pub requests:
		BoxStream<'static, tg::Result<tg::control::Event<tg::sandbox::control::ServerMessage>>>,
	pub sender: tokio::sync::mpsc::Sender<tg::sandbox::control::ClientMessage>,
	pub token: String,
}

impl Pool {
	#[must_use]
	pub(in crate::runner) fn new(size: usize) -> Self {
		Self {
			inner: Mutex::new(VecDeque::with_capacity(size)),
			size,
		}
	}

	pub(in crate::runner) fn start(&self, session: &Session) {
		if session.server.config.runner.remote.is_none() {
			return;
		}
		let mut tasks = self.inner.lock().unwrap();
		assert!(
			tasks.is_empty(),
			"the sandbox control pool was already started"
		);
		tasks.extend((0..self.size).map(|_| Self::spawn(session)));
		if self.size > 0 {
			tracing::debug!(size = self.size, "started the sandbox control pool");
		}
	}

	pub(in crate::runner) async fn take(&self, session: &Session) -> Option<Entry> {
		for _ in 0..self.size {
			let task = {
				let mut tasks = self.inner.lock().unwrap();
				let task = tasks.pop_front()?;
				tasks.push_back(Self::spawn(session));
				task
			};
			let entry = match task.wait().await {
				Ok(Ok(entry)) => entry,
				Ok(Err(error)) => {
					tracing::error!(error = %error.trace(), "failed to reserve a sandbox control connection");
					continue;
				},
				Err(error) => {
					tracing::error!(?error, "a sandbox control pool task panicked");
					continue;
				},
			};
			return Some(entry);
		}
		None
	}

	pub(in crate::runner) async fn shutdown(&self) {
		let tasks = self.inner.lock().unwrap().drain(..).collect::<Vec<_>>();
		for task in &tasks {
			task.abort();
		}
		for task in tasks {
			task.wait().await.ok();
		}
	}

	fn spawn(session: &Session) -> Task<tg::Result<Entry>> {
		Task::spawn({
			let session = session.clone();
			move |_| async move {
				session.reserve_sandbox_control().await.map_err(|error| {
					tg::error!(!error, "failed to reserve a sandbox control connection")
				})
			}
		})
	}
}

impl Session {
	async fn reserve_sandbox_control(&self) -> tg::Result<Entry> {
		let runner = self
			.server
			.runner
			.state()
			.id()
			.ok_or_else(|| tg::error!("missing the runner id"))?;
		let remote = self
			.server
			.config
			.runner
			.remote
			.clone()
			.ok_or_else(|| tg::error!("the sandbox control pool requires a remote"))?;
		let location = tg::Location::Remote(tg::location::Remote {
			name: remote,
			region: None,
		});
		let context = Context {
			principal: tg::Principal::Runner(runner.clone()),
			token: self.server.config.runner.token.clone(),
			..self.context.clone()
		};
		let session = self.server.session(&context);
		let (input, input_receiver) =
			tokio::sync::mpsc::channel::<tg::sandbox::control::ClientMessage>(256);
		let input_stream = tokio_stream::wrappers::ReceiverStream::new(input_receiver)
			.map(Ok)
			.boxed();
		let arg = tg::sandbox::control::Arg {
			created_at: None,
			data: None,
			id: None,
			location: Some(location.into()),
			runner: Some(runner),
		};
		let reconnect_context = session.context.clone();
		let reconnect_server = session.server.clone();
		let reconnect = move |output: &tg::sandbox::control::Output| {
			let token = output.token.clone().or(reconnect_context.token.clone());
			let context = Context {
				principal: tg::Principal::Sandbox(output.id.clone()),
				token,
				..reconnect_context
			};

			reconnect_server.session(&context)
		};
		let (output, control) = session
			.get_sandbox_control_stream_all(arg, input_stream, reconnect)
			.boxed()
			.await?;
		let id = output.id;
		let token = output
			.token
			.ok_or_else(|| tg::error!(%id, "missing the sandbox authentication token"))?;
		crate::checkpoint!(self.server, "runner.sandbox.control.reserved", sandbox = %id).await;
		let entry = Entry {
			id,
			requests: control.boxed(),
			sender: input,
			token,
		};

		Ok(entry)
	}
}
