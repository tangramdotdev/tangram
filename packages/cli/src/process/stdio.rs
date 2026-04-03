use {crate::Cli, tangram_client::prelude::*};

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

impl Cli {
	pub async fn command_process_stdio(&mut self, args: Args) -> tg::Result<()> {
		match args.command {
			Command::Read(args) => {
				self.command_process_stdio_read(args).await?;
			},
			Command::Write(args) => {
				self.command_process_stdio_write(args).await?;
			},
		}
		Ok(())
	}
}
