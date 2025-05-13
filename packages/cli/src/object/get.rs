use crate::Cli;
use crossterm::tty::IsTty as _;
use tangram_client::{self as tg, prelude::*};
use tokio::io::AsyncWriteExt as _;

/// Get an object.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long)]
	pub format: Option<Format>,

	#[arg(index = 1)]
	pub object: tg::object::Id,

	#[arg(long)]
	pub pretty: Option<bool>,

	#[arg(long)]
	pub recursive: bool,
}

#[derive(Clone, Copy, Debug, Default, clap::ValueEnum)]
pub enum Format {
	Bytes,
	Json,
	#[default]
	Tgvn,
}

impl Cli {
	pub async fn command_object_get(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let tg::object::get::Output { bytes } = handle.get_object(&args.object).await?;
		let data = tg::object::Data::deserialize(args.object.kind(), bytes.clone())?;
		let mut stdout = tokio::io::stdout();
		let pretty = args.pretty.unwrap_or(stdout.is_tty());
		match args.format.unwrap_or_default() {
			Format::Bytes => {
				stdout
					.write_all(&bytes)
					.await
					.map_err(|source| tg::error!(!source, "failed to write to stdout"))?;
			},
			Format::Json => {
				Self::output_json(&data, args.pretty).await?;
			},
			Format::Tgvn => {
				let recursive = args.recursive;
				let style = if pretty {
					tg::value::print::Style::Pretty { indentation: "  " }
				} else {
					tg::value::print::Style::Compact
				};
				let options = tg::value::print::Options { recursive, style };
				let object = tg::Object::with_object(data.try_into()?);
				if recursive {
					object.load_recursive(&handle).await?;
				} else {
					object.load(&handle).await?;
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
			.map_err(|source| tg::error!(!source, "failed to flush the output"))?;
		Ok(())
	}
}
