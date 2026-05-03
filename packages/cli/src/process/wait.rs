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
	pub process: tg::process::Id,
}

impl Cli {
	pub async fn command_process_wait(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let locations = args.locations.get();
		let process = tg::Process::<tg::Value>::new(
			args.process.clone(),
			locations.clone(),
			None,
			None,
			None,
			None,
		);
		let arg = tg::process::wait::Arg {
			location: locations,
			token: None,
		};
		let output = process.wait_with_handle(&client, arg).await.map_err(
			|source| tg::error!(!source, id = %args.process, "failed to wait for the process"),
		)?;
		self.print_serde(output.to_data(), args.print).await?;
		Ok(())
	}
}
