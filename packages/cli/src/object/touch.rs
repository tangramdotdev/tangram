use {crate::Cli, tangram_client::prelude::*};

/// Touch an object.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub object: tg::Reference,

	#[command(flatten)]
	pub options: Options,
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
			.resolve_object_with_location(&args.object, &args.options.locations)
			.await?;
		self.command_object_touch_inner(object, args.options).await
	}

	pub(crate) async fn command_object_touch_inner(
		&mut self,
		object: tg::Referent<tg::object::Id>,
		_options: Options,
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
