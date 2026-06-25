use {crate::Cli, tangram_client::prelude::*};

/// Delete a remote.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub name: String,

	#[arg(long)]
	pub principal: Option<tg::principal::Selector>,
}

impl Cli {
	pub async fn command_remote_delete(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let arg = tg::remote::delete::Arg {
			principal: args.principal,
		};
		client.delete_remote(&args.name, arg).await.map_err(
			|error| tg::error!(!error, name = %args.name, "failed to delete the remote"),
		)?;
		Ok(())
	}
}
