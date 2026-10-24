use {tangram_cli_core::Cli, tangram_client::prelude::*};

pub mod abort;
pub mod continue_;
pub mod panic;
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
	Abort(self::abort::Args),
	Continue(self::continue_::Args),
	Panic(self::panic::Args),
	Unwatch(self::unwatch::Args),
	Wait(self::wait::Args),
	Watch(self::watch::Args),
}

pub async fn command_checkpoint(cli: &mut Cli, args: Args) -> tg::Result<()> {
	match args.command {
		Command::Abort(args) => {
			crate::abort::command_checkpoint_abort(cli, args).await?;
		},
		Command::Continue(args) => {
			crate::continue_::command_checkpoint_continue(cli, args).await?;
		},
		Command::Panic(args) => {
			crate::panic::command_checkpoint_panic(cli, args).await?;
		},
		Command::Unwatch(args) => {
			crate::unwatch::command_checkpoint_unwatch(cli, args).await?;
		},
		Command::Wait(args) => {
			crate::wait::command_checkpoint_wait(cli, args).await?;
		},
		Command::Watch(args) => {
			crate::watch::command_checkpoint_watch(cli, args).await?;
		},
	}
	Ok(())
}
