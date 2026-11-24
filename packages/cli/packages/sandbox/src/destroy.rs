use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Destroy a sandbox.
#[derive(Clone, Debug, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	pub sandbox: tg::Referent<tg::sandbox::Id>,

	/// Wait for the sandbox to be destroyed.
	#[usage(long)]
	pub wait: bool,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let sandbox = tg::Sandbox::with_referent(args.sandbox);
	let id = sandbox.id().clone();
	let arg = tg::sandbox::destroy::Arg {
		error: None,
		location: sandbox.location(),
		tokens: sandbox.tokens(),
		wait: args.wait,
	};
	client
		.destroy_sandbox(&id, arg)
		.await
		.map_err(|error| tg::error!(!error, sandbox = %id, "failed to destroy the sandbox"))?;
	sandbox.detach();
	Ok(())
}
