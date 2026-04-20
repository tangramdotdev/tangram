use {crate::Cli, tangram_client::prelude::*};

/// Touch an object.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub locations: crate::location::Args,

	#[arg(index = 1)]
	pub object: tg::object::Id,
}

impl Cli {
	pub async fn command_object_touch(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let arg = tg::object::touch::Arg {
			location: args.locations.get(),
		};
		handle.touch_object(&args.object, arg).await.map_err(
			|source| tg::error!(!source, id = %args.object, "failed to touch the object"),
		)?;
		Ok(())
	}
}
