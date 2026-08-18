use {crate::Cli, tangram_client::prelude::*};

/// Cancel a process.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 2)]
	pub lease: String,

	#[command(flatten)]
	pub location: crate::location::Args,

	#[arg(index = 1)]
	pub reference: tg::Reference,
}

impl Cli {
	pub async fn command_process_cancel(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let mut location = args.location;
		location.set_from_reference_if_unset(&args.reference);
		let process = self
			.get_process_with_locations(&args.reference, &location)
			.await?;
		let location = location.get_for_options(&process);
		let process = tg::Process::<tg::Value>::with_referent(process);
		let options = tg::process::cancel::Options {
			lease: Some(args.lease),
			location,
		};
		process.cancel_with_handle(&client, options).await.map_err(
			|error| tg::error!(!error, id = %process.id(), "failed to cancel the process"),
		)?;
		Ok(())
	}
}
