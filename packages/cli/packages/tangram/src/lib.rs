use tangram_client::prelude::*;

pub mod update;

/// Manage Tangram.
#[derive(Clone, Debug, usage_rs::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	#[usage(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, usage_rs::Subcommands)]
pub enum Command {
	Update(self::update::Args),
}

pub async fn run(args: Args) -> tg::Result<()> {
	match args.command {
		Command::Update(args) => {
			crate::update::run(args).await?;
		},
	}
	Ok(())
}
