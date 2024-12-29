use crate::Cli;
use tangram_client as tg;

/// Get a build's outcome.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub build: tg::build::Id,

	#[arg(long)]
	pub pretty: Option<bool>,
}

impl Cli {
	pub async fn command_build_outcome(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let build = tg::Build::with_id(args.build);
		let outcome = build.outcome(&handle).await?;
		let outcome = outcome.data(&handle).await?;
		Self::output_json(&outcome, args.pretty).await?;
		Ok(())
	}
}
