use tangram_client::prelude::*;

pub mod update;

/// Manage Tangram.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[clap(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, clap::Subcommand)]
pub enum Command {
	Update(self::update::Args),
}

pub async fn command_tangram(args: Args) -> tg::Result<()> {
	match args.command {
		Command::Update(args) => {
			crate::update::command_tangram_update(args).await?;
		},
	}
	Ok(())
}
