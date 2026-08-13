use {crate::Cli, tangram_client::prelude::*};

/// Touch an object.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub locations: crate::location::Args,

	#[arg(index = 1)]
	pub object: tg::Reference,
}

impl Cli {
	pub async fn command_object_touch(&mut self, args: Args) -> tg::Result<()> {
		let object = self
			.resolve_object_with_location(&args.object, &args.locations)
			.await?;
		self.command_object_touch_with_referent(object).await
	}

	pub(crate) async fn command_object_touch_with_referent(
		&mut self,
		object: tg::Referent<tg::object::Id>,
	) -> tg::Result<()> {
		let client = self.client().await?;
		let location = object.options.location.clone().map(Into::into);
		let id = object.node;
		let arg = tg::object::touch::Arg {
			location,
			tokens: object.options.tokens,
		};
		client
			.touch_object(&id, arg)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to touch the object"))?;
		Ok(())
	}
}
