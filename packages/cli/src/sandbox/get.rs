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
	pub sandbox: tg::sandbox::Id,

	#[command(flatten)]
	pub ttl: crate::get::Ttl,
}

impl Cli {
	pub async fn command_sandbox_get(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let id = args.sandbox;
		let entry = tg::sandbox::Options {
			location: args.locations.get(),
			..Default::default()
		};
		let sandbox = tg::Sandbox::new(id.clone(), entry);
		let options = tg::sandbox::get::Options {
			cached: args.cached,
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
