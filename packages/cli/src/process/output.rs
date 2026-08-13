use {crate::Cli, tangram_client::prelude::*};

/// Get a process's output.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub locations: crate::location::Args,

	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(index = 1)]
	pub process: tg::Reference,
}

impl Cli {
	pub async fn command_process_output(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let process = self
			.resolve_process_with_location(&args.process, &args.locations)
			.await?;
		let id = process.node;
		let location = process.options.location.map(Into::into);
		let process = tg::Process::<tg::Value>::new(
			id.clone(),
			tg::process::Options {
				location,
				tokens: process.options.tokens,
				..Default::default()
			},
		);
		let output = process
			.output_with_handle(&client)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to get the process output"))?;
		self.print_serde(output.to_data(), args.print).await?;
		Ok(())
	}
}
