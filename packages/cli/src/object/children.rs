use {crate::Cli, std::collections::BTreeSet, tangram_client::prelude::*};

/// Get an object's children.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub options: Options,

	#[arg(index = 1)]
	pub reference: tg::Reference,
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
	pub async fn command_object_children(&mut self, args: Args) -> tg::Result<()> {
		let mut options = args.options;
		options
			.locations
			.set_from_reference_if_unset(&args.reference);
		let object = self
			.resolve_object_with_locations(&args.reference, &options.locations)
			.await?;
		self.command_object_children_inner(object, options).await
	}

	pub(crate) async fn command_object_children_inner(
		&mut self,
		object: tg::Referent<tg::object::Id>,
		options: Options,
	) -> tg::Result<()> {
		let client = self.client().await?;
		let id = object.node.clone();
		let location = options.locations.get_for_options(&object);
		let object = tg::Object::with_referent(object);
		let options_ = tg::object::get::Options { location };
		let children = object
			.children_with_handle(&client, options_)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to get the object's children"))?;
		let output = children
			.into_iter()
			.map(|object| object.id())
			.collect::<BTreeSet<_>>();
		self.print_serde(output, options.print).await?;
		Ok(())
	}
}
