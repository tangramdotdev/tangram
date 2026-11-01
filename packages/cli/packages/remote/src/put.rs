use {tangram_cli_core::Cli, tangram_client::prelude::*, tangram_uri::Uri};

/// Put a remote.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long)]
	pub principal: Option<tg::principal::Selector>,

	/// The remote's ID or name.
	#[arg(index = 1)]
	pub remote: tg::remote::Selector,

	#[arg(long)]
	pub trusted: bool,

	#[arg(index = 2)]
	pub url: Uri,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let remote = args.remote;
	let trusted = args.trusted;
	let url = args.url;
	let arg = tg::remote::put::Arg {
		principal: args.principal,
		trusted,
		url,
	};
	client
		.put_remote(&remote, arg)
		.await
		.map_err(|error| tg::error!(!error, %remote, "failed to put the remote"))?;
	Ok(())
}
