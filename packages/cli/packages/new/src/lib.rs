use {std::path::PathBuf, tangram_cli_core::Cli, tangram_client::prelude::*};

/// The component.
pub struct Component<'a>(&'a mut Cli);

/// Create a new package.
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
	pub async fn command_new(&mut self, args: Args) -> tg::Result<()> {
		let args = tangram_cli_init::Args { path: args.path };
		tangram_cli_init::Component::new(self)
			.command_init(args)
			.await?;
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
