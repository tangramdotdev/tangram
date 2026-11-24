use {tangram_cli_core::Cli, tangram_client::prelude::*, usage_rs as usage};

pub mod availability;
pub mod children;
pub mod get;
pub mod metadata;
pub mod put;
pub mod touch;

/// Manage objects.
#[derive(Clone, Debug, usage::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	#[usage(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, usage::Subcommands)]
pub enum Command {
	Availability(self::availability::Args),
	Children(self::children::Args),
	Get(self::get::Args),
	Metadata(self::metadata::Args),
	#[usage(alias = "add")]
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
			crate::get::run(cli, args).await?;
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
