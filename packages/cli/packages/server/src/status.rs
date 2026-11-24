use {tangram_cli_core::Cli, tangram_client::prelude::*, usage_rs as usage};

/// Get the server's health.
#[derive(Clone, Debug, usage::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	#[usage(flatten)]
	pub print: tangram_cli_core::print::Options,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.create_client()?;
	let status = if client.connect().await.is_ok() {
		"started"
	} else {
		"stopped"
	};
	cli.print_serde(status, args.print).await?;
	Ok(())
}
