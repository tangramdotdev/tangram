use crate::Cli;
use tangram_client as tg;

/// Get a build's outcome.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	pub build: tg::build::Id,
}

impl Cli {
	pub async fn command_build_outcome(&self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let build = tg::Build::with_id(args.build);
		let outcome = build.outcome(&client).await?;
		let outcome = outcome.data(&client).await?;
		let json = serde_json::to_string(&outcome)
			.map_err(|source| tg::error!(!source, "failed to serialize the output"))?;
		println!("{json}");
		Ok(())
	}
}
