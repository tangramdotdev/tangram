use {
	self::cache::Cache,
	futures::{FutureExt as _, future::BoxFuture},
	std::sync::{Arc, Mutex},
	tangram_client::prelude::*,
	tangram_futures::task::Task,
	tangram_server_core::Server,
};

mod authenticate;
mod cache;
mod token;

/// The component.
#[derive(Clone)]
pub struct Component(Arc<State>);

pub struct State {
	pub cache: Arc<Cache>,
	pub cache_events: tokio::sync::broadcast::Sender<()>,
	pub cache_subscriber: Mutex<Option<Task<()>>>,
}

impl Component {
	#[must_use]
	pub fn new(arg: &tangram_server_core::component::Arg<'_>) -> Self {
		// Create the cache.
		let cache = Arc::new(Cache::new(&arg.config.authentication.cache));
		let (cache_events, _) = tokio::sync::broadcast::channel(16);

		// Listen for the messenger's connection events.
		arg.messenger.add_listener({
			let cache = cache.clone();
			let checkpoints = arg.checkpoints.cloned();
			let events = cache_events.clone();
			move |event| {
				let cache = cache.clone();
				let checkpoints = checkpoints.clone();
				let events = events.clone();
				Box::pin(async move {
					// Mark the authentication cache not ready and notify its subscriber.
					cache.set_ready(false);
					let _ = events.send(());

					// Hit the checkpoint for the authentication cache becoming not ready.
					if let Some(checkpoints) = &checkpoints {
						let mut params = tg::checkpoint::Params::new();
						params.insert("event".to_owned(), event.into());
						checkpoints
							.hit("authentication.cache.unready", params)
							.await;
					}
				})
			}
		});

		// Create the state.
		let cache_subscriber = Mutex::new(None);
		let state = State {
			cache,
			cache_events,
			cache_subscriber,
		};

		Self(Arc::new(state))
	}

	async fn spawn_authentication_cache_task(&self, server: &Server) -> Task<()> {
		let receiver = self.cache_events.subscribe();
		let (started_sender, started) = tokio::sync::oneshot::channel();
		let component = self.clone();
		let server = server.clone();
		let task = Task::spawn(|_| async move {
			component
				.authentication_cache_task(&server, receiver, started_sender)
				.await;
		});
		let _ = started.await;

		task
	}
}

impl std::ops::Deref for Component {
	type Target = State;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl tangram_server_core::component::Lifecycle for Component {
	fn start<'a>(&'a self, server: &'a Server) -> BoxFuture<'a, tg::Result<()>> {
		async move {
			// Subscribe to authentication invalidations before accepting requests.
			let task = self.spawn_authentication_cache_task(server).await;
			self.cache_subscriber.lock().unwrap().replace(task);

			Ok(())
		}
		.boxed()
	}

	fn stop<'a>(&'a self, _server: &'a Server) -> BoxFuture<'a, ()> {
		async move {
			// Stop the cache subscriber and mark the cache as not ready.
			let task = self.cache_subscriber.lock().unwrap().take();
			if let Some(task) = task {
				task.abort();
			}
			self.cache.set_ready(false);
		}
		.boxed()
	}

	fn abort(&self) {
		let task = self.cache_subscriber.lock().unwrap().take();
		if let Some(task) = task {
			task.abort();
		}
	}
}

impl tangram_server_core::authentication::Component for Component {
	fn try_get_authenticated_principal_remote_token<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		remote: &'a str,
	) -> BoxFuture<'a, tg::Result<tangram_server_core::authentication::RemoteToken>> {
		self.try_get_authenticated_principal_remote_token(session, remote)
			.boxed()
	}

	fn try_get_authenticated_process<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		id: &'a tg::process::Id,
	) -> BoxFuture<'a, tg::Result<Option<tangram_server_core::authentication::Process>>> {
		self.try_get_authenticated_process(session, id).boxed()
	}

	fn authenticate<'a>(
		&'a self,
		server: &'a tangram_server_core::Server,
		origin: tangram_server_core::context::Origin,
		token: Option<&'a str>,
	) -> BoxFuture<'a, tg::Result<tangram_server_core::authentication::Entry>> {
		self.authenticate(server, origin, token).boxed()
	}

	fn authenticate_with_claim<'a>(
		&'a self,
		server: &'a tangram_server_core::Server,
		context: &'a tangram_server_core::context::Context,
		token: Option<&'a str>,
		process: Option<tg::process::Id>,
		sandbox: Option<tg::sandbox::Id>,
	) -> BoxFuture<'a, tg::Result<tangram_server_core::authentication::Entry>> {
		self.authenticate_with_claim(server, context, token, process, sandbox)
			.boxed()
	}

	fn create_process_authentication_token<'a>(
		&'a self,
		server: &'a tangram_server_core::Server,
		id: tg::process::Id,
	) -> tg::Result<String> {
		self.create_process_authentication_token(server, id)
	}

	fn create_sandbox_authentication_token<'a>(
		&'a self,
		server: &'a tangram_server_core::Server,
		id: tg::sandbox::Id,
	) -> tg::Result<String> {
		self.create_sandbox_authentication_token(server, id)
	}

	fn invalidate_authentication<'a>(
		&'a self,
		server: &'a tangram_server_core::Server,
		invalidation: tangram_server_core::authentication::cache::Invalidation,
	) -> BoxFuture<'a, ()> {
		self.invalidate_authentication(server, invalidation).boxed()
	}

	fn invalidate_user_authentication<'a>(
		&'a self,
		server: &'a tangram_server_core::Server,
		users: &'a [tg::user::Id],
	) -> BoxFuture<'a, ()> {
		self.invalidate_user_authentication(server, users).boxed()
	}
}
