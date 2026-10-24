use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// List shell directories.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub print: tangram_cli_core::print::Options,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let config = cli.read_config()?;
	let directories = config
		.shell
		.map_or_else(Default::default, |shell| shell.directories);
	cli.print_serde(directories, args.print).await?;
	Ok(())
}
