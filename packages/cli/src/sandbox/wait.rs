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
		let sandbox = tg::Sandbox::new(
			args.sandbox,
			tg::sandbox::Options {
				location: args.locations.get(),
				..tg::sandbox::Options::default()
			},
		);
		self.command_sandbox_wait_with_sandbox(sandbox, args.print)
			.await
	}

	pub(crate) async fn command_sandbox_wait_with_referent(
		&mut self,
		sandbox: tg::Referent<tg::sandbox::Id>,
		print: crate::print::Options,
	) -> tg::Result<()> {
		let sandbox = tg::Sandbox::with_referent(sandbox);
		self.command_sandbox_wait_with_sandbox(sandbox, print).await
	}

	async fn command_sandbox_wait_with_sandbox(
		&mut self,
		sandbox: tg::Sandbox,
		print: crate::print::Options,
	) -> tg::Result<()> {
		let client = self.client().await?;
		let id = sandbox.id().clone();
		let arg = tg::sandbox::status::Arg {
			location: None,
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
