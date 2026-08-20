use {crate::Cli, tangram_client::prelude::*, tokio::io::AsyncWriteExt as _};

/// Get an object.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub options: Options,

	/// The object to print.
	#[arg(index = 1)]
	pub reference: tg::Reference,
}

#[derive(Clone, Debug, Default, clap::Args)]
#[group(skip)]
pub struct Options {
	/// Get the object's availability.
	#[arg(long)]
	pub availability: bool,

	/// Get the object's raw bytes.
	#[arg(long)]
	pub bytes: bool,

	#[command(flatten)]
	pub locations: crate::location::Args,

	/// Get the object's metadata.
	#[arg(long)]
	pub metadata: bool,

	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_object_get(&mut self, args: Args) -> tg::Result<()> {
		let mut options = args.options;
		options
			.locations
			.set_from_reference_if_unset(&args.reference);
		let object = self
			.get_object_with_locations(&args.reference, &options.locations)
			.await?;
		self.command_object_get_inner(object, options).await
	}

	pub(crate) async fn command_object_get_inner(
		&mut self,
		object: tg::Referent<tg::object::Id>,
		mut options: Options,
	) -> tg::Result<()> {
		let client = self.client().await?;
		let id = object.node.clone();
		let location = options.locations.get_for_options(&object);
		let tokens = object.options.tokens.clone();
		if options.bytes {
			let arg = tg::object::get::Arg {
				availability: options.availability,
				location: location.clone(),
				metadata: options.metadata,
				tokens: tokens.clone(),
			};
			let tg::object::get::Output {
				availability,
				bytes,
				metadata,
				tokens: _,
			} = client
				.try_get_object(&id, arg)
				.await
				.map_err(|error| tg::error!(!error, %id, "failed to get the object"))?
				.ok_or_else(|| tg::error!(%id, "failed to find the object"))?;
			if let Some(metadata) = metadata {
				let metadata = serde_json::to_string(&metadata)
					.map_err(|error| tg::error!(!error, "failed to serialize the metadata"))?;
				self.print_info_message(&metadata);
			}
			if let Some(availability) = availability {
				let availability = serde_json::to_string(&availability)
					.map_err(|error| tg::error!(!error, "failed to serialize the availability"))?;
				self.print_info_message(&availability);
			}
			tokio::io::stdout()
				.write_all(&bytes)
				.await
				.map_err(|error| tg::error!(!error, "failed to write to stdout"))?;
			return Ok(());
		}
		let object = tg::Object::with_referent(object);
		options
			.print
			.depth
			.get_or_insert(crate::print::Depth::Finite(1));
		if options.metadata {
			let options_ = tg::object::metadata::Options {
				location: location.clone(),
			};
			let metadata = object
				.metadata_with_handle(&client, options_)
				.await
				.map_err(|error| tg::error!(!error, %id, "failed to get the object metadata"))?;
			let metadata = serde_json::to_string(&metadata)
				.map_err(|error| tg::error!(!error, "failed to serialize the metadata"))?;
			self.print_info_message(&metadata);
		}
		if options.availability {
			let options_ = tg::object::availability::Options {
				location: location.clone(),
			};
			let availability = object
				.availability_with_handle(&client, options_)
				.await
				.map_err(
					|error| tg::error!(!error, %id, "failed to get the object's availability"),
				)?;
			let availability = serde_json::to_string(&availability)
				.map_err(|error| tg::error!(!error, "failed to serialize the availability"))?;
			self.print_info_message(&availability);
		}
		let value = tg::Value::Object(object);
		self.print_value(&value, options.print, location).await?;
		Ok(())
	}
}
