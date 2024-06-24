use crate::Cli;
use tangram_client as tg;

/// Concatenate blobs and artifacts.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub items: Vec<Item>,
}

#[derive(Clone, Debug)]
pub enum Item {
	Blob(tg::blob::Id),
	Artifact(tg::artifact::Id),
}

impl Cli {
	pub async fn command_cat(&self, args: Args) -> tg::Result<()> {
		for item in args.items {
			match item {
				Item::Blob(blob) => {
					self.command_blob_cat(crate::blob::cat::Args { blobs: vec![blob] })
						.await?;
				},
				Item::Artifact(artifact) => {
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

impl std::str::FromStr for Item {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		if let Ok(blob) = s.parse() {
			return Ok(Item::Blob(blob));
		}
		if let Ok(artifat) = s.parse() {
			return Ok(Item::Artifact(artifat));
		}
		Err(tg::error!(%s, "expected a blob or artifact"))
	}
}
