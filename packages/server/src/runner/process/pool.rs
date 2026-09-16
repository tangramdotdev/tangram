use {
	crate::{Context, Session},
	futures::{StreamExt as _, stream::BoxStream},
	std::{
		collections::VecDeque,
		sync::{Arc, Mutex},
		time::Duration,
	},
	tangram_client::prelude::*,
	tangram_futures::task::Task,
};

/// A pool of process control connections whose IDs the control server has assigned but not indexed.
pub(in crate::runner) struct Pool {
	inner: Mutex<VecDeque<Task<tg::Result<Entry>>>>,
	refill_interval: Duration,
	refill_task: Mutex<Option<Task<()>>>,
	size: usize,
}

pub(in crate::runner) struct Entry {
	pub id: tg::process::Id,
	pub requests:
		BoxStream<'static, tg::Result<tg::control::Event<tg::process::control::ServerMessage>>>,
	pub sender_high: tokio::sync::mpsc::Sender<tg::process::control::ClientMessage>,
	pub sender_low: tokio::sync::mpsc::Sender<tg::process::control::ClientMessage>,
	pub sync: Option<tg::sync::Token>,
	pub token: String,
}

impl Pool {
	#[must_use]
	pub(in crate::runner) fn new(size: usize, refill_interval: Duration) -> Self {
		Self {
			inner: Mutex::new(VecDeque::with_capacity(size)),
			refill_interval,
			refill_task: Mutex::new(None),
			size,
		}
	}

	pub(in crate::runner) fn start(self: &Arc<Self>, session: &Session) {
		if session.server.config.runner.remote.is_none() || self.size == 0 {
			return;
		}
		let mut refill_task = self.refill_task.lock().unwrap();
		assert!(
			refill_task.is_none(),
			"the process control pool was already started"
		);
		self.refill(session);
		let task = Task::spawn({
			let pool = self.clone();
			let session = session.clone();
			move |_| async move {
				let mut interval = tokio::time::interval(pool.refill_interval);
				loop {
					interval.tick().await;
					pool.refill(&session);
				}
			}
		});
		refill_task.replace(task);
		tracing::debug!(size = self.size, "started the process control pool");
	}

	fn refill(&self, session: &Session) {
		let mut tasks = self.inner.lock().unwrap();
		while tasks.len() < self.size {
			tasks.push_back(Self::spawn(session));
		}
	}

	pub(in crate::runner) async fn take(&self) -> Option<Entry> {
		loop {
			let task = self.inner.lock().unwrap().pop_front()?;
			match task.wait().await {
				Ok(Ok(entry)) => return Some(entry),
				Ok(Err(error)) => {
					tracing::error!(error = %error.trace(), "failed to reserve a process control connection");
				},
				Err(error) => {
					tracing::error!(?error, "a process control pool task panicked");
				},
			}
		}
	}

	pub(in crate::runner) async fn shutdown(&self) {
		if let Some(task) = self.refill_task.lock().unwrap().take() {
			task.abort();
		}
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
				session.reserve_process_control().await.map_err(|error| {
					tg::error!(!error, "failed to reserve a process control connection")
				})
			}
		})
	}
}

impl Session {
	async fn reserve_process_control(&self) -> tg::Result<Entry> {
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
			.ok_or_else(|| tg::error!("the process control pool requires a remote"))?;
		let location = tg::Location::Remote(tg::location::Remote {
			name: remote,
			region: None,
		});
		let context = Context {
			principal: tg::Principal::Runner(runner),
			token: self.server.config.runner.token.clone(),
			..self.context.clone()
		};
		let session = self.server.session(&context);
		let (sender_high, receiver_high) = tokio::sync::mpsc::channel(512);
		let (sender_low, receiver_low) = tokio::sync::mpsc::channel(512);
		let stream = crate::control::priority_stream(receiver_high, receiver_low)
			.map(Ok)
			.boxed();
		let arg = tg::process::control::Arg {
			data: None,
			id: None,
			lease: None,
			location: Some(location.into()),
			options: tg::referent::Options::default(),
			parent: None,
			reserved: true,
			sync: None,
		};
		let reconnect_context = session.context.clone();
		let reconnect_server = session.server.clone();
		let reconnect = move |output: &tg::process::control::Output| {
			let token = output.token.clone().or(reconnect_context.token.clone());
			let context = Context {
				principal: tg::Principal::Process(output.process.node.clone()),
				token,
				..reconnect_context
			};

			reconnect_server.session(&context)
		};
		let (output, requests) = session
			.try_get_process_control_stream_all(arg, stream, reconnect)
			.await?
			.ok_or_else(|| tg::error!("expected a control stream"))?;
		let id = output.process.node;
		let token = output
			.token
			.ok_or_else(|| tg::error!(%id, "missing the process authentication token"))?;
		crate::checkpoint!(self.server, "runner.process.control.reserved", process = %id).await;
		let entry = Entry {
			id,
			requests: requests.boxed(),
			sender_high,
			sender_low,
			sync: output.sync,
			token,
		};

		Ok(entry)
	}
}
