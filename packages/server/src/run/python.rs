use {
	crate::{Server, context::Context, handle::ServerWithContext},
	futures::FutureExt as _,
	std::sync::Arc,
	tangram_client::prelude::*,
};

impl Server {
	pub(crate) async fn run_python(&self, process: &tg::Process) -> tg::Result<super::Output> {
		let main_runtime_handle = tokio::runtime::Handle::current();

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
		let (abort_sender, _abort_receiver) = tokio::sync::watch::channel(None);

		// Run Python in a dedicated thread (Python with GIL is !Send).
		let (result_sender, result_receiver) = std::sync::mpsc::channel();
		let server = self.clone();
		let process = process.clone();
		let handle = std::thread::spawn(move || {
			let runtime = tokio::runtime::Builder::new_current_thread()
				.enable_all()
				.build()
				.expect("failed to create runtime");

			let local = tokio::task::LocalSet::new();
			let result = local.block_on(&runtime, async move {
				let process_data = crate::context::Process {
					id: process.id().clone(),
					paths: None,
					remote: process.remote().cloned(),
					retry: *process.retry(&server).await?,
				};
				let context = Context {
					process: Some(Arc::new(process_data)),
					token: None,
				};
				let handle = ServerWithContext(server, context);
				tangram_python::run(
					&handle,
					args,
					cwd,
					env,
					executable,
					logger,
					main_runtime_handle,
					Some(abort_sender),
				)
				.await
			});
			result_sender.send(result).expect("failed to send result");
		});

		// Wait for the result by blocking in a spawned task.
		let output = tokio::task::spawn_blocking(move || {
			let result = result_receiver.recv().expect("failed to receive result");
			handle.join().expect("Python thread panicked");
			result
		})
		.await
		.map_err(|source| tg::error!(!source, "the Python runtime task panicked"))?;

		let output = match output {
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
