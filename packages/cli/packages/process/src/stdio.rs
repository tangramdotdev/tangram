use {tangram_cli_core::Cli, tangram_client::prelude::*};

pub mod read;
pub mod write;

/// Read and write process stdio.
#[derive(Clone, Debug, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	#[usage(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, usage_rs::Subcommands)]
pub enum Command {
	Read(self::read::Args),
	Write(self::write::Args),
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	match args.command {
		Command::Read(args) => {
			crate::stdio::read::run(cli, args).await?;
		},
		Command::Write(args) => {
			crate::stdio::write::run(cli, args).await?;
		},
	}
	Ok(())
}
