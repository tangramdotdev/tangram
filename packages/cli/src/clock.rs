use {crate::Cli, tangram_client::prelude::*};

pub mod advance;
pub mod get;
pub mod set;

#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, clap::Subcommand)]
pub enum Command {
	Advance(self::advance::Args),
	Get(self::get::Args),
	Set(self::set::Args),
}

impl Cli {
	pub async fn command_clock(&mut self, args: Args) -> tg::Result<()> {
		match args.command {
			Command::Advance(args) => {
				self.command_clock_advance(args).await?;
			},
			Command::Get(args) => {
				self.command_clock_get(args).await?;
			},
			Command::Set(args) => {
				self.command_clock_set(args).await?;
			},
		}
		Ok(())
	}
}
