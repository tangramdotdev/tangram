use {crate::Cli, tangram_client::prelude::*};

/// Cancel a process.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub location: crate::location::Location,

	#[arg(index = 1)]
	pub process: tg::process::Id,

	#[arg(index = 2)]
	pub token: String,
}

impl Cli {
	pub async fn command_process_cancel(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let process = tg::Process::<tg::Value>::new(
			args.process.clone(),
			Some(args.location.get_locations()?),
			None,
			None,
			Some(args.token),
			None,
		);
		process.cancel_with_handle(&handle).await.map_err(
			|source| tg::error!(!source, id = %process.id(), "failed to cancel the process"),
		)?;
		Ok(())
	}
}
