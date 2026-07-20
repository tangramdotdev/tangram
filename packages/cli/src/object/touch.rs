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
		let client = self.client().await?;
		let object = self.get_resolved_object(&args.object).await?;
		let id = object.item;
		let arg = tg::object::touch::Arg {
			location: args.locations.get(),
			token: object.options.token,
		};
		client
			.touch_object(&id, arg)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to touch the object"))?;
		Ok(())
	}
}
