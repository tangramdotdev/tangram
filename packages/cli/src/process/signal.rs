use {crate::Cli, tangram_client::prelude::*};

/// Signal a process.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub location: crate::location::Args,

	#[arg(index = 1)]
	pub reference: tg::Reference,

	#[arg(default_value = "INT", long, short)]
	pub signal: tg::process::Signal,
}

impl Cli {
	pub async fn command_process_signal(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let mut location = args.location;
		location.set_from_reference_if_unset(&args.reference);
		let process = self
			.resolve_process_with_locations(&args.reference, &location)
			.await?;
		let location = location.get_for_options(&process);
		let process = tg::Process::<tg::Value>::with_referent(process);
		let options = tg::process::signal::Options { location };

		// Signal the process.
		process
			.signal_with_handle(&client, args.signal, options)
			.await
			.map_err(
				|error| tg::error!(!error, id = %process.id(), "failed to signal the process"),
			)?;

		Ok(())
	}
}
