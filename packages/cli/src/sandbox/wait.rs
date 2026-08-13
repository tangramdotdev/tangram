use {crate::Cli, tangram_client::prelude::*};

/// Wait for a sandbox to be destroyed.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub locations: crate::location::Args,

	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(index = 1)]
	pub sandbox: tg::sandbox::Id,
}

impl Cli {
	pub async fn command_sandbox_wait(&mut self, args: Args) -> tg::Result<()> {
		let sandbox = tg::Referent::with_node(args.sandbox);
		self.command_sandbox_wait_with_referent(sandbox, args.locations.get(), args.print)
			.await
	}

	pub(crate) async fn command_sandbox_wait_with_referent(
		&mut self,
		sandbox: tg::Referent<tg::sandbox::Id>,
		location: Option<tg::location::Arg>,
		print: crate::print::Options,
	) -> tg::Result<()> {
		let client = self.client().await?;
		let id = sandbox.node.clone();
		let sandbox = tg::Sandbox::with_referent(sandbox);
		let arg = tg::sandbox::status::Arg {
			location,
			..tg::sandbox::status::Arg::default()
		};
		let output = sandbox
			.wait_with_handle(&client, arg)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to wait for the sandbox"))?;
		self.print_serde(output, print).await?;
		Ok(())
	}
}
