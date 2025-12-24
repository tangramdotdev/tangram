use {crate::Cli, std::collections::BTreeSet, tangram_client::prelude::*};

/// Get an object's children.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub local: crate::util::args::Local,

	#[arg(index = 1)]
	pub object: tg::object::Id,

	#[command(flatten)]
	pub print: crate::print::Options,

	#[command(flatten)]
	pub remotes: crate::util::args::Remotes,
}

impl Cli {
	pub async fn command_object_children(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let arg = tg::object::get::Arg {
			local: args.local.local,
			remotes: args.remotes.remotes,
		};
		let output = handle
			.try_get_object(&args.object, arg)
			.await
			.map_err(|source| tg::error!(!source, id = %args.object, "failed to get the object"))?
			.ok_or_else(|| tg::error!(id = %args.object, "failed to find the object"))?;
		let data = tg::object::Data::deserialize(args.object.kind(), output.bytes)?;
		let mut children = BTreeSet::new();
		data.children(&mut children);
		let output = children.into_iter().collect::<Vec<_>>();
		self.print_serde(output, args.print).await?;
		Ok(())
	}
}
