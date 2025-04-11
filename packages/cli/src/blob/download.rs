use crate::Cli;
use tangram_client as tg;
use url::Url;

/// Download a blob.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub build: crate::process::build::Options,

	#[arg(index = 1)]
	pub url: Url,
}

impl Cli {
	pub async fn command_blob_download(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let host = "builtin";
		let executable = tg::command::Executable::Path("download".into());
		let command_args = vec![args.url.to_string().into()];
		let command = tg::Command::builder(host, executable)
			.args(command_args)
			.build();
		let command = command.id(&handle).await?;
		let reference = tg::Reference::with_object(&command.into());
		self.build_process(args.build, reference, vec![]).await?;
		Ok(())
	}
}
