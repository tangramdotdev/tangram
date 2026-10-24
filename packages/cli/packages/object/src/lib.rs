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

pub async fn command_object(cli: &mut Cli, args: Args) -> tg::Result<()> {
	match args.command {
		Command::Availability(args) => {
			crate::availability::command_object_availability(cli, args).await?;
		},
		Command::Children(args) => {
			crate::children::command_object_children(cli, args).await?;
		},
		Command::Get(args) => {
			tangram_cli_get::object::command_object_get(cli, args).await?;
		},
		Command::Metadata(args) => {
			crate::metadata::command_object_metadata(cli, args).await?;
		},
		Command::Put(args) => {
			crate::put::command_object_put(cli, args).await?;
		},
		Command::Touch(args) => {
			crate::touch::command_object_touch(cli, args).await?;
		},
	}
	Ok(())
}
