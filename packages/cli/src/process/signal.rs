use {crate::Cli, tangram_client::prelude::*};

/// Signal a process.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub location: crate::location::Args,

	#[arg(index = 1)]
	pub process: tg::Referent<tg::process::Id>,

	#[arg(default_value = "INT", long, short)]
	pub signal: tg::process::Signal,
}

impl Cli {
	pub async fn command_process_signal(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let id = args.process.item;
		let process = tg::Process::<tg::Value>::new(
			id,
			tg::process::Options {
				location: args.location.get(),
				token: args.process.options.token,
				..Default::default()
			},
		);

		// Signal the process.
		process
			.signal_with_handle(&client, args.signal)
			.await
			.map_err(
				|error| tg::error!(!error, id = %process.id(), "failed to signal the process"),
			)?;

		Ok(())
	}
}
