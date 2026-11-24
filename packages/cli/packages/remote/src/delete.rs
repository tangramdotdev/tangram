use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Delete a remote.
#[derive(Clone, Debug, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	#[usage(long)]
	pub principal: Option<tg::principal::Selector>,

	/// The remote's ID or name.
	pub remote: tg::remote::Selector,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let arg = tg::remote::delete::Arg {
		principal: args.principal,
	};
	client.delete_remote(&args.remote, arg).await.map_err(
		|error| tg::error!(!error, remote = %args.remote, "failed to delete the remote"),
	)?;
	Ok(())
}
