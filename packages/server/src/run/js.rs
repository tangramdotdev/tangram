use {
	crate::{Server, context::Context, handle::ServerWithContext},
	futures::FutureExt as _,
	std::sync::Arc,
	tangram_client::prelude::*,
};

impl Server {
	pub(crate) async fn run_js(&self, process: &tg::Process) -> tg::Result<super::Output> {
		let main_runtime_handle = tokio::runtime::Handle::current();

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

		// Extract parameters from the process.
		let command = process.command(self).await?;

		let args = command
			.args(self)
			.await?
			.iter()
			.map(tg::Value::to_data)
			.collect::<Vec<_>>();

		let cwd = std::env::current_dir()
			.map_err(|source| tg::error!(!source, "failed to get current directory"))?;

		let env = command
			.env(self)
			.await?
			.iter()
			.map(|(key, value)| (key.clone(), value.to_data()))
			.collect();

		let executable = command.executable(self).await?.to_data();

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
		let task = local_pool_handle.spawn_pinned({
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
		});

		let abort_handle = task.abort_handle();
		scopeguard::defer! {
			abort_handle.abort();
			if let Some(isolate_handle) = isolate_handle_receiver.borrow().as_ref() {
				tracing::trace!("terminating execution");
				isolate_handle.terminate_execution();
			}
		};

		// Get the output.
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

		Ok(output)
	}
}
