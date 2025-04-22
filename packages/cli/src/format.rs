use crate::{Cli, util::infer_module_kind};
use std::path::PathBuf;
use tangram_client::{self as tg, prelude::*};

/// Format a package.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1, default_value = ".")]
	pub path: PathBuf,
}

impl Cli {
	pub async fn command_format(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the absolute path.
		let path = std::path::absolute(&args.path)
			.map_err(|source| tg::error!(!source, "failed to get the absolute path"))?;

		// Get the module kind.
		let kind = infer_module_kind(&path).unwrap_or(tg::module::Kind::Artifact);

		// Create a module.
		let module = tg::Module {
			referent: tg::Referent::with_item(tg::module::Item::Path(path)),
			kind,
		};

		// Format the package.
		let arg = tg::format::Arg { module };
		handle.format(arg).await?;

		Ok(())
	}
}
