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
		let output = build.try_get_output(&handle).await?;
		let output = if let Some(output) = output {
			Some(output.data(&handle).await?)
		} else {
			None
		};
		Self::output_json(&output, args.pretty).await?;
		Ok(())
	}
}
