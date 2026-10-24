use {tangram_cli_core::Cli, tangram_client::prelude::*};

pub mod read;
pub mod write;

/// Read and write process stdio.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, clap::Subcommand)]
pub enum Command {
	Read(self::read::Args),
	Write(self::write::Args),
}

pub async fn command_process_stdio(cli: &mut Cli, args: Args) -> tg::Result<()> {
	match args.command {
		Command::Read(args) => {
			crate::stdio::read::command_process_stdio_read(cli, args).await?;
		},
		Command::Write(args) => {
			crate::stdio::write::command_process_stdio_write(cli, args).await?;
		},
	}
	Ok(())
}
