use crate::Cli;
use tangram_client as tg;

/// Get a build's output.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	pub build: tg::build::Id,
}

impl Cli {
	pub async fn command_build_output(&self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let build = tg::Build::with_id(args.build);
		let output = build.output(&client).await?;
		println!("{output}");
		Ok(())
	}
}
