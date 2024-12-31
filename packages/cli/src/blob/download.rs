use crate::Cli;
use tangram_client as tg;
use url::Url;

/// Download a blob.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long)]
	pub checksum: Option<tg::Checksum>,

	#[command(flatten)]
	pub inner: crate::target::build::InnerArgs,

	#[arg(index = 1)]
	pub url: Url,
}

impl Cli {
	pub async fn command_blob_download(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let host = "builtin";
		let target_args = vec!["download".into(), args.url.to_string().into()];
		let target = tg::Target::builder(host)
			.args(target_args)
			.checksum(args.checksum)
			.build();
		let target = target.id(&handle).await?;
		let args = crate::target::build::Args {
			reference: Some(tg::Reference::with_object(&target.into())),
			inner: args.inner,
		};
		self.command_target_build(args).await?;
		Ok(())
	}
}
