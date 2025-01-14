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
	pub inner: crate::command::build::InnerArgs,

	#[arg(index = 1)]
	pub url: Url,
}

impl Cli {
	pub async fn command_blob_download(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let host = "builtin";
		let command_args = vec!["download".into(), args.url.to_string().into()];
		let command = tg::Command::builder(host)
			.args(command_args)
			.checksum(args.checksum)
			.build();
		let command = command.id(&handle).await?;
		let args = crate::command::build::Args {
			reference: Some(tg::Reference::with_object(&command.into())),
			inner: args.inner,
		};
		self.command_command_build(args).await?;
		Ok(())
	}
}
