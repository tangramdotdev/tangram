use {crate::Cli, tangram_client::prelude::*};

pub mod continue_;
pub mod unwatch;
pub mod wait;
pub mod watch;

#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, clap::Subcommand)]
pub enum Command {
	Continue(self::continue_::Args),
	Unwatch(self::unwatch::Args),
	Wait(self::wait::Args),
	Watch(self::watch::Args),
}

impl Cli {
	pub async fn command_checkpoint(&mut self, args: Args) -> tg::Result<()> {
		match args.command {
			Command::Continue(args) => {
				self.command_checkpoint_continue(args).await?;
			},
			Command::Unwatch(args) => {
				self.command_checkpoint_unwatch(args).await?;
			},
			Command::Wait(args) => {
				self.command_checkpoint_wait(args).await?;
			},
			Command::Watch(args) => {
				self.command_checkpoint_watch(args).await?;
			},
		}
		Ok(())
	}
}
