use {tangram_cli_core::Cli, tangram_client::prelude::*};

pub mod availability;
pub mod build;
pub mod cancel;
pub mod children;
pub mod exec;
pub mod metadata;
pub mod output;
pub mod put;
pub mod run;
pub mod signal;
pub mod spawn;
pub mod status;
pub mod stdio;
pub mod touch;
pub mod wait;

/// The component.
pub struct Component<'a>(&'a mut Cli);

/// Manage processes.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, clap::Subcommand)]
pub enum Command {
	Availability(self::availability::Args),
	Cancel(self::cancel::Args),
	Children(self::children::Args),
	Exec(self::exec::Args),
	Get(tangram_cli_get::process::Args),
	Log(self::stdio::read::Args),
	Metadata(self::metadata::Args),
	Output(self::output::Args),
	#[command(alias = "add")]
	Put(self::put::Args),
	#[command(alias = "kill")]
	Signal(self::signal::Args),
	Spawn(self::spawn::Args),
	Status(self::status::Args),
	Stdio(self::stdio::Args),
	Touch(self::touch::Args),
	Wait(self::wait::Args),
}

impl<'a> Component<'a> {
	pub fn new(cli: &'a mut Cli) -> Self {
		Self(cli)
	}
}

impl Component<'_> {
	pub async fn command_process(&mut self, args: Args) -> tg::Result<()> {
		match args.command {
			Command::Availability(args) => {
				self.command_process_availability(args).await?;
			},
			Command::Cancel(args) => {
				self.command_process_cancel(args).await?;
			},
			Command::Children(args) => {
				self.command_process_children(args).await?;
			},
			Command::Exec(args) => {
				self.command_process_exec(args).await?;
			},
			Command::Get(args) => {
				tangram_cli_get::Component::new(self)
					.command_process_get(args)
					.await?;
			},
			Command::Log(args) => {
				self.command_process_stdio_read(args).await?;
			},
			Command::Metadata(args) => {
				self.command_process_metadata(args).await?;
			},
			Command::Output(args) => {
				self.command_process_output(args).await?;
			},
			Command::Put(args) => {
				self.command_process_put(args).await?;
			},
			Command::Signal(args) => {
				self.command_process_signal(args).await?;
			},
			Command::Spawn(args) => {
				self.command_process_spawn(args).await?;
			},
			Command::Status(args) => {
				self.command_process_status(args).await?;
			},
			Command::Stdio(args) => {
				self.command_process_stdio(args).await?;
			},
			Command::Touch(args) => {
				self.command_process_touch(args).await?;
			},
			Command::Wait(args) => {
				self.command_process_wait(args).await?;
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
