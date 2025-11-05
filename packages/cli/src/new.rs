use {crate::Cli, std::path::PathBuf, tangram_client::prelude::*};

/// Create a new package.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub path: Option<PathBuf>,
}

impl Cli {
	pub async fn command_new(&mut self, args: Args) -> tg::Result<()> {
		let args = crate::init::Args { path: args.path };
		self.command_init(args).await?;
		Ok(())
	}
}
