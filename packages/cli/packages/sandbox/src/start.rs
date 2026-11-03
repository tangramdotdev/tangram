use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Start a sandbox.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub sandbox: tg::Referent<tg::sandbox::Id>,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let sandbox = tg::Sandbox::with_referent(args.sandbox);
	let id = sandbox.id().clone();
	sandbox
		.start_with_instance(&client)
		.await
		.map_err(|error| tg::error!(!error, sandbox = %id, "failed to start the sandbox"))?;
	Ok(())
}
