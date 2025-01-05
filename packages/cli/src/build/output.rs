use crate::Cli;
use tangram_client as tg;

/// Get a build's output.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub build: tg::build::Id,
}

impl Cli {
	pub async fn command_build_output(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let build = tg::Build::with_id(args.build);
		let output = build.output(&handle).await?;
		if let Some(error) = output.error {
			println!("{error}");
		} else if let Some(output) = output.output {
			let value = tg::Value::try_from(output)?;
			println!("{value}");
		}
		Ok(())
	}
}
