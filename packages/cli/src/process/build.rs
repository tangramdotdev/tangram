use crate::Cli;
use tangram_client as tg;

pub use crate::process::run::InnerArgs;

/// Spawn and await a sandboxed process.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub inner: InnerArgs,

	/// The reference to the command.
	#[arg(index = 1)]
	pub reference: Option<tg::Reference>,
}

impl Cli {
	pub async fn command_process_build(&self, args: Args) -> tg::Result<()> {
		let reference = args.reference.unwrap_or_else(|| ".".parse().unwrap());
		self.command_process_build_inner(args.inner, reference)
			.await?;
		Ok(())
	}

	pub async fn command_process_build_inner(
		&self,
		mut args: InnerArgs,
		reference: tg::Reference,
	) -> tg::Result<()> {
		args.inner.sandbox = true;
		self.command_process_run_inner(args, reference).await?;
		Ok(())
	}
}
