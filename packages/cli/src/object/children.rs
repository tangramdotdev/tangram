use {crate::Cli, std::collections::BTreeSet, tangram_client::prelude::*};

/// Get an object's children.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub locations: crate::location::Args,

	#[arg(index = 1)]
	pub object: tg::Referent<tg::object::Id>,

	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_object_children(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let id = args.object.item;
		let arg = tg::object::get::Arg {
			location: args.locations.get(),
			metadata: false,
			token: args.object.options.token,
		};
		let output = client
			.try_get_object(&id, arg)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to get the object"))?
			.ok_or_else(|| tg::error!(%id, "failed to find the object"))?;
		let data = tg::object::Data::deserialize(id.kind(), output.bytes)?;
		let mut children = BTreeSet::new();
		data.children(&mut children);
		let output = children.into_iter().collect::<Vec<_>>();
		self.print_serde(output, args.print).await?;
		Ok(())
	}
}
