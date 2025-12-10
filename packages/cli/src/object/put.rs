use {crate::Cli, bytes::Bytes, tangram_client::prelude::*, tokio::io::AsyncReadExt as _};

/// Put an object.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// Put the object's raw bytes.
	#[arg(long)]
	pub bytes: bool,

	#[arg(index = 1)]
	pub id: Option<tg::object::Id>,

	#[arg(index = 2)]
	pub input: Option<String>,

	#[arg(long, short)]
	pub kind: Option<tg::object::Kind>,

	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_object_put(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Read input from argument or stdin.
		let input = if let Some(input) = args.input {
			input.into_bytes()
		} else {
			let mut input = Vec::new();
			crate::util::stdio::stdin()
				.read_to_end(&mut input)
				.await
				.map_err(|source| tg::error!(!source, "failed to read stdin"))?;
			input
		};

		let id = if args.bytes {
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
			let arg = tg::object::put::Arg { bytes };
			handle.put_object(&id, arg).await?;

			id
		} else {
			// Parse the value.
			let input = std::str::from_utf8(&input)
				.map_err(|source| tg::error!(!source, "the input was not valid utf-8"))?;
			let value = tg::value::parse(input)
				.map_err(|source| tg::error!(!source, "failed to parse the value"))?;

			// Store the value.
			value
				.store(&handle)
				.await
				.map_err(|source| tg::error!(!source, "failed to store the value"))?;

			// Extract the object from the value.
			let tg::Value::Object(object) = value else {
				return Err(tg::error!("expected an object value"));
			};

			object.id()
		};

		// Print the id.
		Self::print_id(&id);

		Ok(())
	}
}
