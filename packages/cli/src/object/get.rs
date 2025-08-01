use crate::Cli;
use tangram_client::{self as tg, prelude::*};
use tokio::io::AsyncWriteExt as _;

/// Get an object.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// The format to use.
	#[arg(long)]
	pub format: Option<Format>,

	/// The object to print.
	#[arg(index = 1)]
	pub object: tg::object::Id,

	/// Whether to print blobs.
	#[arg(long)]
	pub print_blobs: bool,

	/// The depth to print.
	#[arg(short = 'd', long, default_value = "1")]
	pub print_depth: Depth,

	/// Whether to print pretty.
	#[arg(long)]
	pub print_pretty: Option<bool>,
}

#[derive(Clone, Copy, Debug)]
pub enum Depth {
	Finite(u64),
	Infinite,
}

#[derive(Clone, Copy, Debug, Default, clap::ValueEnum)]
pub enum Format {
	Bytes,
	Json,
	#[default]
	Tgon,
}

impl Cli {
	pub async fn command_object_get(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let mut stdout = tokio::io::BufWriter::new(tokio::io::stdout());
		let pretty = args.print_pretty;
		match args.format.unwrap_or_default() {
			Format::Bytes => {
				let tg::object::get::Output { bytes } = handle.get_object(&args.object).await?;
				stdout
					.write_all(&bytes)
					.await
					.map_err(|source| tg::error!(!source, "failed to write to stdout"))?;
			},
			Format::Json => {
				let tg::object::get::Output { bytes } = handle.get_object(&args.object).await?;
				let data = tg::object::Data::deserialize(args.object.kind(), bytes.clone())?;
				Self::print_json(&data, args.print_pretty).await?;
			},
			Format::Tgon => {
				let object = tg::Object::with_id(args.object);
				let value = tg::Value::from(object);
				Cli::print_output(&handle, &value, args.print_depth, pretty, args.print_blobs)
					.await?;
			},
		}
		stdout
			.flush()
			.await
			.map_err(|source| tg::error!(!source, "failed to flush stdout"))?;
		Ok(())
	}
}

impl std::str::FromStr for Depth {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let depth = if s.starts_with("inf") {
			Depth::Infinite
		} else {
			s.parse()
				.map(Depth::Finite)
				.map_err(|_| tg::error!("invalid depth"))?
		};
		Ok(depth)
	}
}
