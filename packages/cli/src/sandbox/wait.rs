use {crate::Cli, tangram_client::prelude::*};

/// Wait for a sandbox to be destroyed.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub options: Options,

	#[arg(index = 1)]
	pub sandbox: tg::Referent<tg::sandbox::Id>,
}

#[derive(Clone, Debug, Default, clap::Args)]
#[group(skip)]
pub struct Options {
	#[command(flatten)]
	pub locations: crate::location::Args,

	#[command(flatten)]
	pub print: crate::print::Options,

	/// Select the source of sandbox state.
	#[arg(long, default_value = "auto", value_parser = crate::process::source_parser())]
	pub source: tg::sandbox::Source,
}

impl Cli {
	pub async fn command_sandbox_wait(&mut self, args: Args) -> tg::Result<()> {
		let entry = tg::sandbox::Options {
			location: args.options.locations.get_for_options(&args.sandbox),
			tokens: args.sandbox.options.tokens.clone(),
			..tg::sandbox::Options::default()
		};
		let sandbox = tg::Sandbox::new(args.sandbox.node, entry);
		self.command_sandbox_wait_inner(sandbox, args.options).await
	}

	pub(crate) async fn command_sandbox_wait_inner(
		&mut self,
		sandbox: tg::Sandbox,
		options: Options,
	) -> tg::Result<()> {
		let client = self.client().await?;
		let id = sandbox.id().clone();
		let options_ = tg::sandbox::status::Options {
			source: options.source,
			..Default::default()
		};
		let output = sandbox
			.wait_with_handle(&client, options_)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to wait for the sandbox"))?;
		self.print_serde(output, options.print).await?;
		Ok(())
	}
}
