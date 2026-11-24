use {tangram_cli_core::Cli, tangram_client::prelude::*, usage_rs as usage};

pub mod advance;
pub mod get;
pub mod set;

#[derive(Clone, Debug, usage::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	#[usage(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, usage::Subcommands)]
pub enum Command {
	Advance(self::advance::Args),
	Get(self::get::Args),
	Set(self::set::Args),
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	match args.command {
		Command::Advance(args) => {
			crate::advance::run(cli, args).await?;
		},
		Command::Get(args) => {
			crate::get::run(cli, args).await?;
		},
		Command::Set(args) => {
			crate::set::run(cli, args).await?;
		},
	}
	Ok(())
}
