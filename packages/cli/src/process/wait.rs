use {crate::Cli, tangram_client::prelude::*};

/// Wait for a process to finish.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub locations: crate::location::Args,

	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(index = 1)]
	pub process: tg::Referent<tg::process::Id>,
}

impl Cli {
	pub async fn command_process_wait(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let locations = args.locations.get();
		let id = args.process.item;
		let token = args.process.options.token;
		let process = tg::Process::<tg::Value>::new(
			id.clone(),
			tg::process::Options {
				location: locations.clone(),
				token: token.clone(),
				..Default::default()
			},
		);
		let arg = tg::process::wait::Arg {
			lease: None,
			location: locations,
			token: None,
		};
		let output = process
			.wait_with_handle(&client, arg)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to wait for the process"))?;
		self.print_serde(output.to_data(), args.print).await?;
		Ok(())
	}
}
