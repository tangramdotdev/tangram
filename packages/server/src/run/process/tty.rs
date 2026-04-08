use {crate::Server, futures::TryStreamExt as _, tangram_client::prelude::*};

impl Server {
	pub(crate) async fn run_tty_task(
		&self,
		sandbox: &tangram_sandbox::Sandbox,
		sandbox_process: &tangram_sandbox::Process,
		id: &tg::process::Id,
		remote: Option<&String>,
	) -> tg::Result<()> {
		// Get the signal stream for the process.
		let arg = tg::process::tty::size::get::Arg {
			local: None,
			remotes: remote.map(|r| vec![r.clone()]),
		};
		let mut stream = self
			.try_get_process_tty_size_stream(id, arg)
			.await
			.map_err(|source| {
				tg::error!(
					!source,
					process = %id,
					"failed to get the process's tty stream"
				)
			})?
			.ok_or_else(
				|| tg::error!(process = %id, "expected the process's tty stream to exist"),
			)?;

		// Handle the events.
		while let Some(event) = stream.try_next().await.map_err(
			|source| tg::error!(!source, process = %id, "failed to get the next tty event"),
		)? {
			match event {
				tg::process::tty::size::get::Event::Size(size) => {
					sandbox
						.set_tty_size(sandbox_process, size)
						.await
						.map_err(|source| tg::error!(!source, "failed to set the tty size"))?;
				},
				tg::process::tty::size::get::Event::End => {
					break;
				},
			}
		}

		Ok(())
	}
}
