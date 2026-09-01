use {crate::Cli, tangram_client::prelude::*};

/// List runners.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long, conflicts_with = "owner")]
	pub all: bool,

	#[arg(long, conflicts_with = "all")]
	pub owner: Option<tg::principal::Selector>,

	#[command(flatten)]
	pub output: crate::print::OutputOptions,

	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_runner_list(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let arg = tg::runner::list::Arg {
			all: args.all,
			owner: args.owner,
		};
		let output = client
			.list_runners(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to list the runners"))?;
		if args.output.verbose {
			self.print_serde(output, args.print).await?;
		} else {
			self.print_serde(output.data, args.print).await?;
		}

		Ok(())
	}
}
