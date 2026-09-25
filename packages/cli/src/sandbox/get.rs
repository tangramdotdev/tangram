use {crate::Cli, tangram_client::prelude::*};

/// Get a sandbox.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// Only use cached remote results. Do not fetch from remotes.
	#[arg(long)]
	pub cached: bool,

	#[command(flatten)]
	pub locations: crate::location::Args,

	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(index = 1)]
	pub sandbox: tg::Referent<tg::sandbox::Id>,

	/// Select the source of sandbox state.
	#[arg(long, default_value = "auto", value_parser = crate::process::source_parser())]
	pub source: tg::sandbox::Source,

	#[command(flatten)]
	pub ttl: crate::get::Ttl,
}

impl Cli {
	pub async fn command_sandbox_get(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let location = args.locations.get_for_options(&args.sandbox);
		let id = args.sandbox.node;
		let entry = tg::sandbox::Options {
			location,
			tokens: args.sandbox.options.tokens,
			..Default::default()
		};
		let sandbox = tg::Sandbox::new(id.clone(), entry);
		let options = tg::sandbox::get::Options {
			cached: args.cached,
			source: args.source,
			ttl: args.ttl.get(),
		};
		let output = sandbox
			.try_get_with_handle(&client, options)
			.await
			.map_err(|error| tg::error!(!error, sandbox = %id, "failed to get the sandbox"))?
			.ok_or_else(|| tg::error!(sandbox = %id, "failed to find the sandbox"))?;
		self.print_serde(output.as_ref(), args.print).await?;
		Ok(())
	}
}
