use crate::Cli;
use tangram_client as tg;

/// Spawn and await an unsandboxed process.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub options: Options,

	/// The reference to the command.
	#[arg(index = 1)]
	pub reference: Option<tg::Reference>,

	/// Set arguments.
	#[arg(index = 2, trailing_var_arg = true)]
	pub trailing: Vec<String>,
}

#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Options {
	/// If this flag is set, then exit immediately instead of waiting for the process to finish.
	#[arg(short, long)]
	pub detach: bool,

	#[command(flatten)]
	pub spawn: crate::process::spawn::Options,
}

impl Cli {
	pub async fn command_process_run(&self, _args: Args) -> tg::Result<()> {
		todo!()
	}

	#[allow(dead_code)]
	pub(crate) async fn run_process(
		&self,
		_options: Options,
		_reference: tg::Reference,
		_trailing: Vec<String>,
	) -> tg::Result<()> {
		todo!()
	}
}
