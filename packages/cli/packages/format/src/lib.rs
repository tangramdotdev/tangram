use {std::path::PathBuf, tangram_cli_core::Cli, tangram_client::prelude::*};

/// The component.
pub struct Component<'a>(&'a mut Cli);

/// Format.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(default_value = ".", index = 1)]
	pub path: PathBuf,
}

impl<'a> Component<'a> {
	pub fn new(cli: &'a mut Cli) -> Self {
		Self(cli)
	}
}

impl Component<'_> {
	pub async fn command_format(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;

		// Canonicalize the path's parent.
		let path = tangram_util::fs::canonicalize_parent(&args.path)
			.await
			.map_err(|error| tg::error!(!error, "failed to canonicalize the path"))?;

		// Format.
		let arg = tg::format::Arg { path };
		client
			.format(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to format"))?;

		Ok(())
	}
}

impl std::ops::Deref for Component<'_> {
	type Target = Cli;

	fn deref(&self) -> &Self::Target {
		self.0
	}
}

impl std::ops::DerefMut for Component<'_> {
	fn deref_mut(&mut self) -> &mut Self::Target {
		self.0
	}
}
