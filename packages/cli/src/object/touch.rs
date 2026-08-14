use {crate::Cli, tangram_client::prelude::*};

/// Touch an object.
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
}

impl Cli {
	pub async fn command_object_touch(&mut self, args: Args) -> tg::Result<()> {
		let object = self
			.resolve_object_with_locations(&args.reference, &args.options.locations)
			.await?;
		self.command_object_touch_inner(object, args.options).await
	}

	pub(crate) async fn command_object_touch_inner(
		&mut self,
		object: tg::Referent<tg::object::Id>,
		_options: Options,
	) -> tg::Result<()> {
		let client = self.client().await?;
		let id = object.node.clone();
		let object = tg::Object::with_referent(object);
		object
			.touch_with_handle(&client)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to touch the object"))?;
		Ok(())
	}
}
