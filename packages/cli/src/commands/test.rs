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

	/// The path to the executable in the artifact to run.
	#[arg(long)]
	pub executable_path: Option<tg::Path>,

	/// If this flag is set, the package's lockfile will not be updated.
	#[arg(long)]
	pub locked: bool,

	/// The package to build.
	#[arg(short, long, default_value = ".")]
	pub package: tg::Dependency,

	/// The retry strategy to use.
	#[arg(long, default_value_t)]
	pub retry: tg::build::Retry,
}

impl Cli {
	pub async fn command_test(&self, args: Args) -> Result<()> {
		// Create the build args.
		let args = super::build::NewArgs {
			detach: args.detach,
			locked: args.locked,
			no_tui: false,
			output: None,
			package: args.package,
			retry: args.retry,
			target: "test".to_owned(),
		};
		let args = super::build::Args {
			args,
			command: None,
		};

		// Build!
		self.command_build(args).await?;

		Ok(())
	}
}
