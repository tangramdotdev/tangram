use {crate::Cli, tangram_client::prelude::*};

/// List namespace grants for a group.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub group: String,

	#[command(flatten)]
	pub location: crate::location::Args,

	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_group_grants(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let output = client
			.list_group_namespace_grants(tg::group::grants::Arg {
				group: args.group.clone(),
				location: args.location.get(),
			})
			.await
			.map_err(
				|error| tg::error!(!error, group = %args.group, "failed to list the namespace grants"),
			)?
			.ok_or_else(|| tg::error!(group = %args.group, "failed to find the group"))?;
		self.print_serde(output, args.print).await?;
		Ok(())
	}
}
