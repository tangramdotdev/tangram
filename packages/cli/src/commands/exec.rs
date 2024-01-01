use crate::Cli;
use tangram_client as tg;
use tangram_error::Result;

/// Build a target from the specified package and execute a command from its output.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
#[command(trailing_var_arg = true)]
pub struct Args {
	/// The path to the executable in the artifact to run.
	#[arg(long)]
	pub executable_path: Option<tg::Path>,

	/// If this flag is set, the package's lockfile will not be updated.
	#[arg(long)]
	pub locked: bool,

	#[arg(default_value = ".")]
	pub package: tg::Dependency,

	/// The retry strategy to use.
	#[arg(long, default_value_t)]
	pub retry: tg::build::Retry,

	/// The name of the target to build.
	#[arg(short, long, default_value = "default")]
	pub target: String,

	pub trailing: Vec<String>,
}

impl Cli {
	pub async fn command_exec(&self, args: Args) -> Result<()> {
		// Create the run args.
		let args = super::run::Args {
			executable_path: args.executable_path,
			locked: args.locked,
			no_tui: false,
			package: args.package,
			retry: args.retry,
			target: args.target,
			trailing: args.trailing,
		};

		// Run!
		self.command_run(args).await?;

		Ok(())
	}
}
