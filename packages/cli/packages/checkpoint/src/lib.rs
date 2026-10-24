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

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	match args.command {
		Command::Abort(args) => {
			crate::abort::run(cli, args).await?;
		},
		Command::Continue(args) => {
			crate::continue_::run(cli, args).await?;
		},
		Command::Panic(args) => {
			crate::panic::run(cli, args).await?;
		},
		Command::Unwatch(args) => {
			crate::unwatch::run(cli, args).await?;
		},
		Command::Wait(args) => {
			crate::wait::run(cli, args).await?;
		},
		Command::Watch(args) => {
			crate::watch::run(cli, args).await?;
		},
	}
	Ok(())
}
