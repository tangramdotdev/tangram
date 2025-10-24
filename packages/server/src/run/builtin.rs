use {crate::Server, futures::FutureExt as _, tangram_client as tg};

impl Server {
	pub(crate) async fn run_builtin(&self, process: &tg::Process) -> tg::Result<super::Output> {
		let server = self.clone();
		let logger = std::sync::Arc::new(
			move |process: &tg::Process, stream: tg::process::log::Stream, message: String| {
				let server = server.clone();
				let process = process.clone();
				async move { crate::run::util::log(&server, &process, stream, message).await }
					.boxed()
			},
		);
		let output = tangram_builtin::run(self, process, logger).await?;
		let output = super::Output {
			checksum: output.checksum,
			error: output.error,
			exit: output.exit,
			output: output.output,
		};
		Ok(output)
	}
}
