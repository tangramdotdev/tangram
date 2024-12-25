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

	#[arg(index = 1)]
	pub url: Url,
}

impl Cli {
	pub async fn command_blob_download(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let host = "js";
		let executable = tg::File::with_contents(formatdoc!(
			r"
				export default tg.target((url, checksum) => tg.download(url, checksum));
			"
		));
		let args = vec![
			"default".into(),
			args.url.to_string().into(),
			args.checksum.map(|checksum| checksum.to_string()).into(),
		];
		let target = tg::Target::builder(host)
			.executable(Some(executable.into()))
			.args(args)
			.build();
		let target = target.id(&handle).await?;
		let args = crate::target::build::Args {
			reference: Some(tg::Reference::with_object(&target.into())),
			..Default::default()
		};
		self.command_target_build(args).await?;
		Ok(())
	}
}
