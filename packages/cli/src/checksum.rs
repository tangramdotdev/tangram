use crate::Cli;
use indoc::formatdoc;
use tangram_client as tg;
use url::Url;

/// Compute a checksum.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// The checksum algorithm to use.
	#[arg(long, default_value_t = tg::checksum::Algorithm::Sha256)]
	pub algorithm: tg::checksum::Algorithm,

	/// The artifact, blob, or URL to checksum.
	#[arg(index = 1, conflicts_with_all = ["artifact", "blob", "url"])]
	pub item: Option<Item>,

	/// The artifact to checksum.
	#[arg(long, conflicts_with_all = ["blob", "url", "arg"])]
	pub artifact: Option<tg::artifact::Id>,

	/// The blob to checksum.
	#[arg(long, conflicts_with_all = ["artifact", "url", "arg"])]
	pub blob: Option<tg::blob::Id>,

	/// The url to checksum.
	#[arg(long, conflicts_with_all = ["artifact", "blob", "arg"])]
	pub url: Option<Url>,
}

#[derive(Clone, Debug)]
pub enum Item {
	Artifact(tg::artifact::Id),
	Blob(tg::blob::Id),
	Url(Url),
}

impl Cli {
	pub async fn command_checksum(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let item = if let Some(artifact) = args.artifact {
			Item::Artifact(artifact)
		} else if let Some(blob) = args.blob {
			Item::Blob(blob)
		} else if let Some(url) = args.url {
			Item::Url(url)
		} else if let Some(item) = args.item {
			item
		} else {
			return Err(tg::error!("no arg provided"));
		};
		match item {
			Item::Artifact(artifact) => {
				let args = crate::artifact::checksum::Args {
					algorithm: args.algorithm,
					artifact,
				};
				self.command_artifact_checksum(args).await?;
			},

			Item::Blob(blob) => {
				let args = crate::blob::checksum::Args {
					algorithm: args.algorithm,
					blob,
				};
				self.command_blob_checksum(args).await?;
			},

			Item::Url(url) => {
				let host = "js";
				let executable = tg::Package::from(tg::Artifact::from(formatdoc!(
					r#"
						export default tg.target(async (url, algorithm) => {{
							let blob = await tg.download(url, "unsafe");
							let checksum = await tg.checksum(blob, algorithm);
							return checksum;
						}});
					"#
				)));
				let args = vec![
					"default".into(),
					url.to_string().into(),
					args.algorithm.to_string().into(),
				];
				let target = tg::Target::builder(host)
					.executable(executable)
					.args(args)
					.build();
				let target = target.id(&handle).await?;
				let args = crate::target::build::Args {
					reference: Some(tg::Reference::with_object(&target.into())),
					..Default::default()
				};
				self.command_target_build(args).await?;
			},
		}
		Ok(())
	}
}

impl std::str::FromStr for Item {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		if let Ok(artifact) = s.parse() {
			return Ok(Item::Artifact(artifact));
		}
		if let Ok(blob) = s.parse() {
			return Ok(Item::Blob(blob));
		}
		if let Ok(url) = s.parse() {
			return Ok(Item::Url(url));
		}
		Err(tg::error!(%s, "expected an artifact, blob, or url"))
	}
}
