use {crate::Cli, tangram_client::prelude::*};

/// Get object metadata.
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

	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_object_metadata(&mut self, args: Args) -> tg::Result<()> {
		let object = self
			.resolve_object_with_locations(&args.object, &args.options.locations)
			.await?;
		self.command_object_metadata_inner(object, args.options)
			.await
	}

	pub(crate) async fn command_object_metadata_inner(
		&mut self,
		object: tg::Referent<tg::object::Id>,
		options: Options,
	) -> tg::Result<()> {
		let client = self.client().await?;
		let location = object.options.location.clone().map(Into::into);
		let id = object.node;
		let arg = tg::object::metadata::Arg {
			location,
			tokens: object.options.tokens,
		};
		let output = client
			.try_get_object_metadata(&id, arg)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to get the object metadata"))?
			.ok_or_else(|| tg::error!(%id, "failed to find the object metadata"))?;
		self.print_serde(output, options.print).await?;
		Ok(())
	}
}
