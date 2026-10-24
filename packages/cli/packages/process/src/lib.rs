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

pub async fn command_process(cli: &mut Cli, args: Args) -> tg::Result<()> {
	match args.command {
		Command::Availability(args) => {
			crate::availability::command_process_availability(cli, args).await?;
		},
		Command::Cancel(args) => {
			crate::cancel::command_process_cancel(cli, args).await?;
		},
		Command::Children(args) => {
			crate::children::command_process_children(cli, args).await?;
		},
		Command::Exec(args) => {
			crate::exec::command_process_exec(cli, args).await?;
		},
		Command::Get(args) => {
			tangram_cli_get::process::command_process_get(cli, args).await?;
		},
		Command::Log(args) => {
			crate::stdio::read::command_process_stdio_read(cli, args).await?;
		},
		Command::Metadata(args) => {
			crate::metadata::command_process_metadata(cli, args).await?;
		},
		Command::Output(args) => {
			crate::output::command_process_output(cli, args).await?;
		},
		Command::Put(args) => {
			crate::put::command_process_put(cli, args).await?;
		},
		Command::Signal(args) => {
			crate::signal::command_process_signal(cli, args).await?;
		},
		Command::Spawn(args) => {
			crate::spawn::command_process_spawn(cli, args).await?;
		},
		Command::Status(args) => {
			crate::status::command_process_status(cli, args).await?;
		},
		Command::Stdio(args) => {
			crate::stdio::command_process_stdio(cli, args).await?;
		},
		Command::Touch(args) => {
			crate::touch::command_process_touch(cli, args).await?;
		},
		Command::Wait(args) => {
			crate::wait::command_process_wait(cli, args).await?;
		},
	}
	Ok(())
}
