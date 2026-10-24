use {tangram_cli_core::Cli, tangram_client::prelude::*};

pub mod availability;
pub mod children;
pub mod metadata;
pub mod put;
pub mod touch;

/// Manage objects.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, clap::Subcommand)]
pub enum Command {
	Availability(self::availability::Args),
	Children(self::children::Args),
	Get(tangram_cli_get::object::Args),
	Metadata(self::metadata::Args),
	#[command(alias = "add")]
	Put(self::put::Args),
	Touch(self::touch::Args),
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	match args.command {
		Command::Availability(args) => {
			crate::availability::run(cli, args).await?;
		},
		Command::Children(args) => {
			crate::children::run(cli, args).await?;
		},
		Command::Get(args) => {
			tangram_cli_get::object::run(cli, args).await?;
		},
		Command::Metadata(args) => {
			crate::metadata::run(cli, args).await?;
		},
		Command::Put(args) => {
			crate::put::run(cli, args).await?;
		},
		Command::Touch(args) => {
			crate::touch::run(cli, args).await?;
		},
	}
	Ok(())
}
