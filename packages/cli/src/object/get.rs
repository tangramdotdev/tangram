use crate::Cli;
use crossterm::tty::IsTty as _;
use tangram_client::{self as tg, prelude::*};
use tokio::io::AsyncWriteExt as _;

/// Get an object.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(short, long, default_value = "1")]
	pub depth: Depth,

	#[arg(long)]
	pub format: Option<Format>,

	#[arg(index = 1)]
	pub object: tg::object::Id,

	#[arg(long)]
	pub pretty: Option<bool>,
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
		let pretty = args.pretty.unwrap_or(stdout.get_ref().is_tty());
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
				Self::print_json(&data, args.pretty).await?;
			},
			Format::Tgon => {
				let depth = match args.depth {
					Depth::Finite(depth) => Some(depth),
					Depth::Infinite => None,
				};
				let style = if pretty {
					tg::value::print::Style::Pretty { indentation: "  " }
				} else {
					tg::value::print::Style::Compact
				};
				let options = tg::value::print::Options { depth, style };
				let object = tg::Object::with_id(args.object);
				match depth {
					Some(0) => (),
					Some(1) => {
						object.load(&handle).await?;
					},
					None | Some(_) => {
						object.load_recursive(&handle).await?;
					},
				}
				let value = tg::Value::from(object);
				let output = value.print(options);
				stdout
					.write_all(output.as_bytes())
					.await
					.map_err(|source| tg::error!(!source, "failed to write the output"))?;
				if pretty {
					stdout
						.write(b"\n")
						.await
						.map_err(|source| tg::error!(!source, "failed to write the output"))?;
				}
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
