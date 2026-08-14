use {crate::Cli, tangram_client::prelude::*};

/// Wait for a sandbox to be destroyed.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub options: Options,

	#[arg(index = 1)]
	pub sandbox: tg::sandbox::Id,
}

#[derive(Clone, Debug, Default, clap::Args)]
#[group(skip)]
pub struct Options {
	#[command(flatten)]
	pub locations: crate::location::Args,

	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_sandbox_wait(&mut self, args: Args) -> tg::Result<()> {
		let sandbox = tg::Sandbox::new(
			args.sandbox,
			tg::sandbox::Options {
				location: args.options.locations.get(),
				..tg::sandbox::Options::default()
			},
		);
		self.command_sandbox_wait_inner(sandbox, args.options).await
	}

	pub(crate) async fn command_sandbox_wait_inner(
		&mut self,
		sandbox: tg::Sandbox,
		options: Options,
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
		self.print_serde(output, options.print).await?;
		Ok(())
	}
}
