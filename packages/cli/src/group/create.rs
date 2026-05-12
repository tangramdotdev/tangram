use {crate::Cli, tangram_client::prelude::*};

/// Create a group.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub handle: String,

	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_group_create(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let output = client
			.create_group(tg::group::create::Arg {
				handle: args.handle.clone(),
			})
			.await
			.map_err(
				|error| tg::error!(!error, handle = %args.handle, "failed to create the group"),
			)?;
		self.print_serde(output.group, args.print).await?;
		Ok(())
	}
}
