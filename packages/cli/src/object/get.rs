use crate::Cli;
use crossterm::{style::Stylize as _, tty::IsTty};
use tangram_client::{self as tg, handle::Ext as _};
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
	#[default]
	Bytes,
	Tgvn,
}

impl Cli {
	pub async fn command_object_get(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let tg::object::get::Output { bytes, metadata } = handle.get_object(&args.object).await?;
		let metadata = metadata.ok_or_else(|| tg::error!("expected the metadata to be set"))?;
		eprintln!("{} complete {}", "info".blue().bold(), metadata.complete);
		eprint!("{} count ", "info".blue().bold());
		if let Some(count) = metadata.count {
			eprint!("{count}");
		} else {
			eprint!("∅");
		}
		eprintln!();
		eprint!("{} depth ", "info".blue().bold());
		if let Some(depth) = metadata.depth {
			eprint!("{depth}");
		} else {
			eprint!("∅");
		}
		eprintln!();
		eprint!("{} weight ", "info".blue().bold());
		if let Some(weight) = metadata.weight {
			let weight = byte_unit::Byte::from_u64(weight)
				.get_appropriate_unit(byte_unit::UnitType::Decimal);
			eprint!("{weight}");
		} else {
			eprint!("∅");
		}
		eprintln!();
		let mut stdout = tokio::io::stdout();
		let pretty = args.pretty.unwrap_or(stdout.is_tty());
		match args.format.unwrap_or_default() {
			Format::Bytes => {
				if matches!(args.object, tg::object::Id::Leaf(_)) {
					stdout
						.write_all(&bytes)
						.await
						.map_err(|source| tg::error!(!source, "failed to write the output"))?;
					stdout
						.flush()
						.await
						.map_err(|source| tg::error!(!source, "failed to flush the output"))?;
				} else {
					let output =
						serde_json::from_slice::<serde_json::Value>(&bytes).map_err(|source| {
							tg::error!(!source, "failed to deserialize the object")
						})?;
					Self::output_json(&output, args.pretty).await?;
				}
			},
			Format::Tgvn => {
				let recursive = args.recursive;
				let style = if pretty {
					tg::value::print::Style::Pretty { indentation: "  " }
				} else {
					tg::value::print::Style::Compact
				};
				let options = tg::value::print::Options { recursive, style };
				let object = tg::Object::with_id(args.object);
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
				stdout
					.flush()
					.await
					.map_err(|source| tg::error!(!source, "failed to flush the output"))?;
			},
		}
		Ok(())
	}
}
