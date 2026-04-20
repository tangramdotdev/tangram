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
	pub process: tg::process::Id,
}

impl Cli {
	pub async fn command_process_output(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let locations = args.locations.get();
		let process =
			tg::Process::<tg::Value>::new(args.process.clone(), locations, None, None, None, None);
		let output = process.output_with_handle(&handle).await.map_err(
			|source| tg::error!(!source, id = %args.process, "failed to get the process output"),
		)?;
		self.print_serde(output.to_data(), args.print).await?;
		Ok(())
	}
}
