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
	#[arg(conflicts_with_all = ["artifact", "blob", "url"])]
	pub arg: Option<Arg>,

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
pub enum Arg {
	Artifact(tg::artifact::Id),
	Blob(tg::blob::Id),
	Url(Url),
}

impl Cli {
	pub async fn command_checksum(&self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let arg = if let Some(artifact) = args.artifact {
			Arg::Artifact(artifact)
		} else if let Some(blob) = args.blob {
			Arg::Blob(blob)
		} else if let Some(url) = args.url {
			Arg::Url(url)
		} else if let Some(arg) = args.arg {
			arg
		} else {
			return Err(tg::error!("no arg provided"));
		};
		match arg {
			Arg::Artifact(artifact) => {
				let args = crate::artifact::checksum::Args {
					algorithm: args.algorithm,
					artifact,
				};
				self.command_artifact_checksum(args).await?;
			},

			Arg::Blob(blob) => {
				let args = crate::blob::checksum::Args {
					algorithm: args.algorithm,
					blob,
				};
				self.command_blob_checksum(args).await?;
			},

			Arg::Url(url) => {
				let host = "js";
				let executable = formatdoc!(
					r#"
						export default tg.target(async (url, algorithm) => {{
							let blob = await tg.download(url, "unsafe");
							let checksum = await tg.checksum(blob, algorithm);
							return checksum;
						}});
					"#
				);
				let args = vec![
					"default".into(),
					url.to_string().into(),
					args.algorithm.to_string().into(),
				];
				let target = tg::Target::builder(host)
					.executable(tg::Artifact::from(executable))
					.args(args)
					.build();
				let target = target.id(&client).await?;
				let args = crate::target::build::Args {
					inner: crate::target::build::InnerArgs {
						target: Some(target),
						..Default::default()
					},
					detach: false,
				};
				self.command_target_build(args).await?;
			},
		}
		Ok(())
	}
}

impl std::str::FromStr for Arg {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		if let Ok(artifact) = s.parse() {
			return Ok(Arg::Artifact(artifact));
		}
		if let Ok(blob) = s.parse() {
			return Ok(Arg::Blob(blob));
		}
		if let Ok(url) = s.parse() {
			return Ok(Arg::Url(url));
		}
		Err(tg::error!(%s, "expected an artifact, blob, or url"))
	}
}
