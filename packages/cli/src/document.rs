use crate::Cli;
use tangram_client::{self as tg, prelude::*};

/// Document a package.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// If this flag is set, the package's lockfile will not be updated.
	#[arg(long)]
	pub locked: bool,

	#[arg(long)]
	pub pretty: Option<bool>,

	#[arg(index = 1, default_value = ".")]
	pub reference: tg::Reference,

	#[allow(clippy::option_option)]
	#[arg(short, long)]
	pub remote: Option<Option<String>>,

	/// Generate the documentation for the runtime.
	#[arg(long, default_value = "false")]
	pub runtime: bool,
}

impl Cli {
	pub async fn command_document(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the remote.
		let remote = args
			.remote
			.map(|option| option.unwrap_or_else(|| "default".to_owned()));

		// Get the module.
		let module = if args.runtime {
			tg::module::Data {
				kind: tg::module::Kind::Dts,
				referent: tg::Referent {
					item: tg::module::data::Item::Path("tangram.d.ts".into()),
					path: None,
					tag: None,
				},
			}
		} else {
			self.get_module(&args.reference).await?.to_data()
		};

		// Document the module.
		let arg = tg::document::Arg { module, remote };
		let output = handle.document(arg).await?;

		// Print the output.
		Self::output_json(&output, args.pretty).await?;

		Ok(())
	}
}
