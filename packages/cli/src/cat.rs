use crate::Cli;
use tangram_client as tg;

/// Cat blobs and artifacts.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	pub ids: Vec<Arg>,
}

#[derive(Debug, Clone)]
pub enum Arg {
	Blob(tg::blob::Id),
	Artifact(tg::artifact::Id),
}

impl Cli {
	pub async fn command_cat(&self, args: Args) -> tg::Result<()> {
		for id in args.ids {
			match id {
				Arg::Blob(blob) => {
					self.command_blob_cat(crate::blob::cat::Args { blobs: vec![blob] })
						.await?;
				},
				Arg::Artifact(artifact) => {
					self.command_artifact_cat(crate::artifact::cat::Args {
						artifacts: vec![artifact],
					})
					.await?;
				},
			};
		}
		Ok(())
	}
}

impl std::str::FromStr for Arg {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		if let Ok(blob) = s.parse() {
			return Ok(Arg::Blob(blob));
		}
		if let Ok(artifat) = s.parse() {
			return Ok(Arg::Artifact(artifat));
		}
		Err(tg::error!(%s, "expected a blob or artifact"))
	}
}
