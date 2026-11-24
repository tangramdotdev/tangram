use {
	bytes::Bytes, tangram_cli_core::Cli, tangram_client::prelude::*, tokio::io::AsyncReadExt as _,
	tokio_util::io::StreamReader, usage_rs as usage,
};

/// Put an object.
#[derive(Clone, Debug, usage::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	/// Put the object's raw bytes.
	#[usage(long)]
	pub bytes: bool,

	pub id: Option<tg::object::Id>,

	pub input: Option<String>,

	#[usage(long, short)]
	pub kind: Option<tg::object::Kind>,

	#[usage(flatten)]
	pub location: tangram_cli_core::location::Args,

	#[usage(flatten)]
	pub print: tangram_cli_core::print::Options,

	/// Grant public read access.
	#[usage(long)]
	pub public: bool,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	let client = cli.client().await?;
	let location = args.location.get();
	let location_ = args.location.to_location()?;

	// Read input from argument or stdin.
	let input = if let Some(input) = args.input {
		input.into_bytes()
	} else {
		let mut input = Vec::new();
		StreamReader::new(
			tangram_util::io::stdin()
				.map_err(|error| tg::error!(!error, "failed to open stdin"))?,
		)
		.read_to_end(&mut input)
		.await
		.map_err(|error| tg::error!(!error, "failed to read stdin"))?;
		input
	};

	let referent = if args.bytes {
		let bytes = Bytes::from(input);

		// Compute the ID if necessary.
		let id = if let Some(id) = args.id {
			id
		} else {
			let kind = args
				.kind
				.ok_or_else(|| tg::error!("kind must be set when using --bytes"))?;
			tg::object::Id::new(kind, &bytes)
		};

		// Put the object.
		let arg = tg::object::put::Arg {
			bytes,
			children: Vec::new(),
			location,
			metadata: None,
			public: args.public,
		};
		let output = client
			.put_object(&id, arg)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to put the object"))?;

		output.object
	} else {
		// Parse the value.
		let input = std::str::from_utf8(&input)
			.map_err(|error| tg::error!(!error, "the input was not valid utf-8"))?;
		let value = tg::value::parse(input)
			.map_err(|error| tg::error!(!error, "failed to parse the value"))?;

		// Store the value.
		let arg = tg::value::store::Arg {
			location: location_,
			public: args.public,
		};
		value
			.store_with_arg_with_instance(&client, arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to store the value"))?;

		// Extract the object from the value.
		let tg::Value::Object(object) = value else {
			return Err(tg::error!("expected an object value"));
		};

		object.to_referent()
	};

	// Print the referent.
	Cli::print_referent(&referent, &args.print);

	Ok(())
}
