use {tangram_cli_core::Cli, tangram_client::prelude::*};

pub mod abort;
pub mod continue_;
pub mod panic;
pub mod unwatch;
pub mod wait;
pub mod watch;

/// The component.
pub struct Component<'a>(&'a mut Cli);

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

impl<'a> Component<'a> {
	pub fn new(cli: &'a mut Cli) -> Self {
		Self(cli)
	}
}

impl Component<'_> {
	pub async fn command_checkpoint(&mut self, args: Args) -> tg::Result<()> {
		match args.command {
			Command::Abort(args) => {
				self.command_checkpoint_abort(args).await?;
			},
			Command::Continue(args) => {
				self.command_checkpoint_continue(args).await?;
			},
			Command::Panic(args) => {
				self.command_checkpoint_panic(args).await?;
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

impl std::ops::Deref for Component<'_> {
	type Target = Cli;

	fn deref(&self) -> &Self::Target {
		self.0
	}
}

impl std::ops::DerefMut for Component<'_> {
	fn deref_mut(&mut self) -> &mut Self::Target {
		self.0
	}
}
