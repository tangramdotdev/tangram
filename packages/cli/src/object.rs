use {crate::Cli, tangram_client::prelude::*};

pub mod availability;
pub mod children;
pub mod get;
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
	Get(self::get::Args),
	Metadata(self::metadata::Args),
	#[command(alias = "add")]
	Put(self::put::Args),
	Touch(self::touch::Args),
}

impl Cli {
	pub async fn command_object(&mut self, args: Args) -> tg::Result<()> {
		match args.command {
			Command::Availability(args) => {
				self.command_object_availability(args).await?;
			},
			Command::Children(args) => {
				self.command_object_children(args).await?;
			},
			Command::Get(args) => {
				self.command_object_get(args).await?;
			},
			Command::Metadata(args) => {
				self.command_object_metadata(args).await?;
			},
			Command::Put(args) => {
				self.command_object_put(args).await?;
			},
			Command::Touch(args) => {
				self.command_object_touch(args).await?;
			},
		}
		Ok(())
	}
}
