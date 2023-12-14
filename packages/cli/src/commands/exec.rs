use crate::Cli;
use tangram_client as tg;
use tangram_error::Result;

/// Build a target from the specified package and execute a command from its output.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
#[command(trailing_var_arg = true)]
pub struct Args {
	/// The name of the target to build.
	#[arg(short, long, default_value = "default")]
	pub target: String,

	#[command(flatten)]
	pub package_args: super::PackageArgs,

	/// The retry strategy to use.
	#[arg(long, default_value_t)]
	pub retry: tg::build::Retry,

	#[command(flatten)]
	pub run_args: super::RunArgs,

	#[arg(default_value = ".")]
	pub package: tg::Dependency,

	pub trailing_args: Vec<String>,
}

impl Cli {
	pub async fn command_exec(&self, args: Args) -> Result<()> {
		// Create the run args.
		let args = super::run::Args {
			no_tui: false,
			package: args.package,
			package_args: args.package_args,
			retry: args.retry,
			run_args: args.run_args,
			target: args.target,
			trailing_args: args.trailing_args,
		};

		// Run!
		self.command_run(args).await?;

		Ok(())
	}
}
