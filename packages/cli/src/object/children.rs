use {crate::Cli, std::collections::BTreeSet, tangram_client::prelude::*};

/// Get an object's children.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub locations: crate::location::Args,

	#[arg(index = 1)]
	pub object: tg::Reference,

	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_object_children(&mut self, args: Args) -> tg::Result<()> {
		let object = self
			.resolve_object_with_location(&args.object, &args.locations)
			.await?;
		self.command_object_children_with_referent(args, object)
			.await
	}

	pub(crate) async fn command_object_children_with_referent(
		&mut self,
		args: Args,
		object: tg::Referent<tg::object::Id>,
	) -> tg::Result<()> {
		let client = self.client().await?;
		let location = object.options.location.clone().map(Into::into);
		let id = object.node;
		let arg = tg::object::get::Arg {
			location,
			metadata: false,
			stored: false,
			tokens: object.options.tokens,
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
