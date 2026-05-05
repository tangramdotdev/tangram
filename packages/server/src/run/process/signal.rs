use {crate::Server, futures::TryStreamExt as _, tangram_client::prelude::*};

impl Server {
	pub(crate) async fn run_signal_task(
		&self,
		sandbox: &tangram_sandbox::Sandbox,
		sandbox_process: &tangram_sandbox::Process,
		id: &tg::process::Id,
		location: Option<&tg::Location>,
	) -> tg::Result<()> {
		// Get the signal stream for the process.
		let arg = tg::process::signal::get::Arg {
			location: location.cloned().map(Into::into),
			wait: true,
		};
		let mut stream = self
			.try_get_process_signal_stream(id, arg)
			.await
			.map_err(|source| {
				tg::error!(
					!source,
					process = %id,
					"failed to get the process's signal stream"
				)
			})?
			.ok_or_else(
				|| tg::error!(process = %id, "expected the process's signal stream to exist"),
			)?;

		// Handle the events.
		while let Some(event) = stream.try_next().await.map_err(
			|source| tg::error!(!source, process = %id, "failed to get the next signal event"),
		)? {
			match event {
				tg::process::signal::get::Event::Signal(signal) => {
					sandbox
						.kill(sandbox_process, signal)
						.await
						.map_err(|source| tg::error!(!source, "failed to signal the process"))?;
				},
				tg::process::signal::get::Event::End => {
					break;
				},
			}
		}

		Ok(())
	}
}
