use crate::Cli;
use indoc::formatdoc;
use tangram_client as tg;
use url::Url;

/// Download a blob.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long)]
	pub checksum: Option<tg::Checksum>,

	pub url: Url,
}

impl Cli {
	pub async fn command_blob_download(&self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let host = "js";
		let executable = formatdoc!(
			r#"
				export default tg.target((url, checksum) => tg.download(url, checksum));
			"#
		);
		let args = vec![
			"default".into(),
			args.url.to_string().into(),
			args.checksum.map(|checksum| checksum.to_string()).into(),
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
		Ok(())
	}
}
