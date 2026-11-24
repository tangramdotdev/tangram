use {tangram_cli_core::Cli, tangram_client::prelude::*, usage_rs as usage};

/// List shell directories.
#[derive(Clone, Debug, usage::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	#[usage(flatten)]
	pub print: tangram_cli_core::print::Options,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let directories = cli
		.config
		.as_ref()
		.and_then(|config| config.shell.as_ref())
		.and_then(|shell| shell.directories.clone())
		.unwrap_or_default();
	cli.print_serde(directories, args.print).await?;
	Ok(())
}
