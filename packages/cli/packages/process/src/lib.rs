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

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	match args.command {
		Command::Availability(args) => {
			crate::availability::run(cli, args).await?;
		},
		Command::Cancel(args) => {
			crate::cancel::run(cli, args).await?;
		},
		Command::Children(args) => {
			crate::children::run(cli, args).await?;
		},
		Command::Exec(args) => {
			crate::exec::run(cli, args).await?;
		},
		Command::Get(args) => {
			tangram_cli_get::process::run(cli, args).await?;
		},
		Command::Log(args) => {
			crate::stdio::read::run(cli, args).await?;
		},
		Command::Metadata(args) => {
			crate::metadata::run(cli, args).await?;
		},
		Command::Output(args) => {
			crate::output::run(cli, args).await?;
		},
		Command::Put(args) => {
			crate::put::run(cli, args).await?;
		},
		Command::Signal(args) => {
			crate::signal::run(cli, args).await?;
		},
		Command::Spawn(args) => {
			crate::spawn::run(cli, args).await?;
		},
		Command::Status(args) => {
			crate::status::run(cli, args).await?;
		},
		Command::Stdio(args) => {
			crate::stdio::run(cli, args).await?;
		},
		Command::Touch(args) => {
			crate::touch::run(cli, args).await?;
		},
		Command::Wait(args) => {
			crate::wait::run(cli, args).await?;
		},
	}
	Ok(())
}
