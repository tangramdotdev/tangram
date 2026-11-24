use {tangram_cli_core::Cli, tangram_client::prelude::*, usage_rs as usage};

pub mod manage;

/// Manage the user's billing.
#[derive(Clone, Debug, usage::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	#[usage(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, usage::Subcommands)]
pub enum Command {
	Manage(self::manage::Args),
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	match args.command {
		Command::Manage(args) => {
			crate::billing::manage::run(cli, args).await?;
		},
	}
	Ok(())
}
