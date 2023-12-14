use super::PackageArgs;
use crate::Cli;
use tangram_client as tg;
use tangram_error::Result;

/// Build the target named "test" from the specified package.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct Args {
	/// If this flag is set, then the command will exit immediately instead of waiting for the build's output.
	#[arg(short, long)]
	pub detach: bool,

	/// The package to build.
	#[arg(short, long, default_value = ".")]
	pub package: tg::Dependency,

	#[command(flatten)]
	pub package_args: PackageArgs,

	/// The retry strategy to use.
	#[arg(long, default_value_t)]
	pub retry: tg::build::Retry,
}

impl Cli {
	pub async fn command_test(&self, args: Args) -> Result<()> {
		// Create the build args.
		let args = super::build::Args {
			no_tui: false,
			detach: args.detach,
			output: None,
			package: args.package,
			package_args: args.package_args,
			retry: args.retry,
			target: "test".to_owned(),
		};

		// Build!
		self.command_build(args).await?;

		Ok(())
	}
}
