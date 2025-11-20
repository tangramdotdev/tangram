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

		// Get the args, cwd, env, and executable.
		let state = process.load(self).await?;
		let data = state.command.data(self).await?;
		let args = data.args;
		let cwd = data
			.cwd
			.clone()
			.unwrap_or_else(|| std::path::PathBuf::from("/"));
		let env = data.env;
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

		// Create a channel to receive the isolate handle.
		let (isolate_handle_sender, isolate_handle_receiver) = tokio::sync::watch::channel(None);

		// Spawn the task.
		let local_pool_handle = self.local_pool_handle.get_or_init(|| {
			let concurrency = self
				.config
				.runner
				.as_ref()
				.map_or(1, |config| config.concurrency);
			tokio_util::task::LocalPoolHandle::new(concurrency)
		});
		let task = AbortOnDropHandle::new(local_pool_handle.spawn_pinned({
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
					token: None,
				};
				let handle = ServerWithContext(server, context);
				tangram_js::run(
					&handle,
					args,
					cwd,
					env,
					executable,
					logger,
					main_runtime_handle,
					Some(isolate_handle_sender),
				)
				.boxed_local()
				.await
			}
		}));

		// If this future is dropped before the task is done, then terminate the execution of the isolate.
		let mut done = scopeguard::guard(false, |done| {
			if !done && let Some(isolate_handle) = isolate_handle_receiver.borrow().as_ref() {
				tracing::trace!("terminating execution");
				isolate_handle.terminate_execution();
			}
		});

		// Await the task and get the output.
		let output = match task.await.unwrap() {
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
