use {crate::Cli, tangram_client::prelude::*};

/// Get object metadata.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub object: tg::object::Id,

	#[command(flatten)]
	pub local: crate::util::args::Local,

	#[command(flatten)]
	pub print: crate::print::Options,

	#[command(flatten)]
	pub remotes: crate::util::args::Remotes,
}

impl Cli {
	pub async fn command_object_metadata(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let arg = tg::object::metadata::Arg {
			local: args.local.local,
			remotes: args.remotes.remotes,
		};
		let output = handle
			.try_get_object_metadata(&args.object, arg)
			.await
			.map_err(
				|source| tg::error!(!source, id = %args.object, "failed to get the object metadata"),
			)?
			.ok_or_else(|| tg::error!(id = %args.object, "failed to get the object metadata"))?;
		self.print_serde(output, args.print).await?;
		Ok(())
	}
}
