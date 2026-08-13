use {crate::Cli, tangram_client::prelude::*};

/// Cancel a process.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub location: crate::location::Args,

	#[arg(index = 1)]
	pub process: tg::Reference,

	#[arg(index = 2)]
	pub lease: String,
}

impl Cli {
	pub async fn command_process_cancel(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let (process, locations) = self
			.resolve_process_with_locations(&args.process, args.location)
			.await?;
		let process = tg::Process::<tg::Value>::new(
			process.node,
			tg::process::Options {
				lease: Some(args.lease),
				location: locations.get(),
				tokens: process.options.tokens,
				..Default::default()
			},
		);
		process.cancel_with_handle(&client).await.map_err(
			|error| tg::error!(!error, id = %process.id(), "failed to cancel the process"),
		)?;
		Ok(())
	}
}
