use {crate::Server, futures::FutureExt as _, tangram_client::prelude::*};

impl Server {
	pub(crate) async fn run_builtin(&self, process: &tg::Process) -> tg::Result<super::Output> {
		let logger = std::sync::Arc::new({
			let server = self.clone();
			let process = process.clone();
			move |stream: tg::process::log::Stream, message: String| {
				let server = server.clone();
				let process = process.clone();
				async move { crate::run::util::log(&server, &process, stream, message).await }
					.boxed()
			}
		});

		// Extract the command data.
		let command = process.command(self).await?;
		let args = command
			.args(self)
			.await?
			.iter()
			.map(tg::Value::to_data)
			.collect();
		let cwd = command
			.cwd(self)
			.await?
			.clone()
			.unwrap_or_else(|| std::path::PathBuf::from("/"));
		let env = command
			.env(self)
			.await?
			.iter()
			.map(|(key, value)| (key.clone(), value.to_data()))
			.collect();
		let executable = command.executable(self).await?.to_data();
		let checksum = process
			.load(self)
			.await?
			.expected_checksum
			.as_ref()
			.map(tg::Checksum::algorithm);

		let output = tangram_builtin::run(
			self,
			args,
			cwd,
			env,
			executable,
			logger,
			checksum,
			&self.temp_path(),
		)
		.await?;
		let output = super::Output {
			checksum: output.checksum,
			error: output.error,
			exit: output.exit,
			output: output.output,
		};
		Ok(output)
	}
}
