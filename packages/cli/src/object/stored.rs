use {crate::Cli, tangram_client::prelude::*};

/// Get an object's storage status.
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
	pub async fn command_object_stored(&mut self, args: Args) -> tg::Result<()> {
		let object = self
			.resolve_object_with_location(&args.object, &args.locations)
			.await?;
		self.command_object_stored_with_referent(args, object).await
	}

	pub(crate) async fn command_object_stored_with_referent(
		&mut self,
		args: Args,
		object: tg::Referent<tg::object::Id>,
	) -> tg::Result<()> {
		let client = self.client().await?;
		let location = object.options.location.clone().map(Into::into);
		let id = object.node;
		let arg = tg::object::stored::Arg {
			location,
			tokens: object.options.tokens,
		};
		let output = client
			.try_get_object_stored(&id, arg)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to get the object's storage status"))?
			.ok_or_else(|| tg::error!(%id, "failed to find the object's storage status"))?;
		self.print_serde(output, args.print).await?;
		Ok(())
	}
}
