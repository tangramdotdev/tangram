use {
	crate::{Server, context::Context, handle::ServerWithContext},
	futures::FutureExt as _,
	std::sync::Arc,
	tangram_client::prelude::*,
	tokio_util::task::AbortOnDropHandle,
};

impl Server {
	pub(crate) async fn run_js(&self, process: &tg::Process) -> tg::Result<super::Output> {
		let main_runtime_handle = tokio::runtime::Handle::current();

		// Get the JS engine.
		let engine = self
			.config
			.runner
			.as_ref()
			.map_or(crate::config::JsEngine::Auto, |runner| runner.js.engine);

		// Get the args, cwd, env, and executable.
		let state = process
			.load(self)
			.await
			.map_err(|source| tg::error!(!source, "failed to load the process"))?;
		let data = state
			.command
			.data(self)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the command data"))?;
		let args = data.args;
		let cwd = data
			.cwd
			.clone()
			.unwrap_or_else(|| std::path::PathBuf::from("/"));
		let mut env = data.env;
		env.insert(
			"TANGRAM_PROCESS".to_owned(),
			process.id().to_string().into(),
		);
		let executable = data.executable;

		// Create the logger.
		let logger = Arc::new({
			let server = self.clone();
			let process = process.clone();
			move |stream, string| {
				let server = server.clone();
				let process = process.clone();
				async move { crate::run::util::log(&server, &process, stream, string).await }
					.boxed()
			}
		});

		// Create a channel to receive the abort handle.
		let (abort_sender, abort_receiver) =
			tokio::sync::watch::channel::<Option<tangram_js::Abort>>(None);

		// Spawn the task.
		let local_pool_handle = self.local_pool_handle.get_or_init(|| {
			let parallelism = std::thread::available_parallelism()
				.map(std::num::NonZeroUsize::get)
				.unwrap_or(1);
			let concurrency = self.config.runner.as_ref().map_or(parallelism, |config| {
				config.concurrency.unwrap_or(parallelism)
			});
			tokio_util::task::LocalPoolHandle::new(concurrency)
		});

		let task = match engine {
			#[cfg(feature = "v8")]
			crate::config::JsEngine::Auto | crate::config::JsEngine::V8 => {
				// Create a channel for the engine-specific abort.
				let (engine_abort_sender, mut engine_abort_receiver) =
					tokio::sync::watch::channel::<Option<tangram_js::v8::Abort>>(None);

				// Forward abort from engine-specific to unified.
				let abort_sender_clone = abort_sender.clone();
				tokio::spawn(async move {
					while engine_abort_receiver.changed().await.is_ok() {
						if let Some(abort) = engine_abort_receiver.borrow().clone() {
							abort_sender_clone
								.send(Some(tangram_js::Abort::V8(abort)))
								.ok();
							break;
						}
					}
				});

				AbortOnDropHandle::new(local_pool_handle.spawn_pinned({
					let server = self.clone();
					let process = process.clone();
					move || async move {
						let process = crate::context::Process {
							id: process.id().clone(),
							paths: None,
							remote: process.remote().cloned(),
							retry: *process.retry(&server).await?,
						};
						let context = Context {
							process: Some(Arc::new(process)),
							..Default::default()
						};
						let handle = ServerWithContext(server, context);
						tangram_js::v8::run(
							&handle,
							args,
							cwd,
							env,
							executable,
							logger,
							main_runtime_handle,
							Some(engine_abort_sender),
						)
						.boxed_local()
						.await
					}
				}))
			},

			#[allow(unreachable_patterns)]
			#[cfg(feature = "quickjs")]
			crate::config::JsEngine::Auto | crate::config::JsEngine::QuickJs => {
				// Create a channel for the engine-specific abort.
				let (engine_abort_sender, mut engine_abort_receiver) =
					tokio::sync::watch::channel::<Option<tangram_js::quickjs::Abort>>(None);

				// Forward abort from engine-specific to unified.
				let abort_sender_clone = abort_sender.clone();
				tokio::spawn(async move {
					while engine_abort_receiver.changed().await.is_ok() {
						if let Some(abort) = engine_abort_receiver.borrow().clone() {
							abort_sender_clone
								.send(Some(tangram_js::Abort::QuickJs(abort)))
								.ok();
							break;
						}
					}
				});

				AbortOnDropHandle::new(local_pool_handle.spawn_pinned({
					let server = self.clone();
					let process = process.clone();
					move || async move {
						let process = crate::context::Process {
							id: process.id().clone(),
							paths: None,
							remote: process.remote().cloned(),
							retry: *process.retry(&server).await?,
						};
						let context = Context {
							process: Some(Arc::new(process)),
							..Default::default()
						};
						let handle = ServerWithContext(server, context);
						tangram_js::quickjs::run(
							&handle,
							args,
							cwd,
							env,
							executable,
							logger,
							main_runtime_handle,
							Some(engine_abort_sender),
						)
						.boxed_local()
						.await
					}
				}))
			},

			#[allow(unreachable_patterns)]
			_ => return Err(tg::error!("the requested JS engine is not available")),
		};

		// If this future is dropped before the task is done, then abort execution.
		let mut done = scopeguard::guard(false, |done| {
			if !done && let Some(abort) = abort_receiver.borrow().as_ref() {
				tracing::trace!("aborting execution");
				abort.abort();
			}
		});

		// Await the task and get the output.
		let output = match task
			.await
			.map_err(|source| tg::error!(!source, "the task panicked"))?
		{
			Ok(output) => super::Output {
				checksum: output.checksum,
				error: output.error,
				exit: output.exit,
				output: output.output,
			},
			Err(error) => super::Output {
				checksum: None,
				error: Some(error),
				exit: 1,
				output: None,
			},
		};

		// Mark the task as done.
		*done = true;

		Ok(output)
	}
}
