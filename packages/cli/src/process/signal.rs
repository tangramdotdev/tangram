use {crate::Cli, tangram_client::prelude::*};

/// Signal a process.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub location: crate::location::Args,

	#[arg(index = 1)]
	pub process: tg::process::Id,

	#[arg(default_value = "INT", long, short)]
	pub signal: tg::process::Signal,
}

impl Cli {
	pub async fn command_process_signal(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let process = tg::Process::<tg::Value>::new(
			args.process.clone(),
			args.location.get(),
			None,
			None,
			None,
			None,
		);

		// Signal the process.
		process
			.signal_with_handle(&handle, args.signal)
			.await
			.map_err(
				|source| tg::error!(!source, id = %process.id(), "failed to signal the process"),
			)?;

		Ok(())
	}
}
