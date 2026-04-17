use {crate::Cli, futures::StreamExt as _, tangram_client::prelude::*};

/// Get a process's status.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub locations: crate::location::Locations,

	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(index = 1)]
	pub process: tg::process::Id,
}

impl Cli {
	pub async fn command_process_status(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let locations = args.locations.get();
		let process = tg::Process::<tg::Value>::new(
			args.process.clone(),
			Some(locations),
			None,
			None,
			None,
			None,
		);
		let stream = process.status_with_handle(&handle).await.map_err(
			|source| tg::error!(!source, id = %args.process, "failed to get the process status"),
		)?;
		self.print_serde_stream(stream.boxed(), args.print).await?;
		Ok(())
	}
}
