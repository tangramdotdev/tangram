use crate::Cli;
use std::{os::unix::process::CommandExt as _, path::PathBuf};
use tangram_client as tg;

/// Build a target and run a command.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// The path to the executable in the artifact to run.
	#[arg(short = 'x', long)]
	pub executable: Option<std::path::PathBuf>,

	#[command(flatten)]
	pub inner: crate::command::build::InnerArgs,

	/// The reference to the target to build.
	#[arg(index = 1)]
	pub reference: Option<tg::Reference>,

	/// Arguments to pass to the executable.
	#[arg(index = 2, trailing_var_arg = true)]
	pub trailing: Vec<String>,
}

impl Cli {
	pub async fn command_command_run(&self, mut args: Args) -> tg::Result<()> {
		todo!()
	}
}
