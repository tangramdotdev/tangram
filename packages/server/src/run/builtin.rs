use {crate::Server, futures::FutureExt as _, tangram_client::prelude::*};

impl Server {
	pub(crate) async fn run_builtin(&self, process: &tg::Process) -> tg::Result<super::Output> {
		// Get the args, cwd, env, executable, and checksum.
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
		let env = data.env;
		let executable = data.executable;

		// Create the logger.
		let logger = std::sync::Arc::new({
			let server = self.clone();
			let process = process.clone();
			move |stream: tg::process::log::Stream, message: Vec<u8>| {
				let server = server.clone();
				let process = process.clone();
				async move { crate::run::util::log(&server, &process, stream, message).await }
					.boxed()
			}
		});

		let temp_path = self.temp_path();
		let output =
			tangram_builtin::run(self, args, cwd, env, executable, logger, Some(&temp_path))
				.await
				.map_err(|source| tg::error!(!source, "failed to run the builtin"))?;
		let output = super::Output {
			checksum: output.checksum,
			error: output.error,
			exit: output.exit,
			output: output.output,
		};

		Ok(output)
	}
}
