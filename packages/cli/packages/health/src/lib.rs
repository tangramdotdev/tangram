use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// The component.
pub struct Component<'a>(&'a mut Cli);

/// Get the server's health.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(long, value_delimiter = ',')]
	pub fields: Option<Vec<String>>,

	#[command(flatten)]
	pub print: tangram_cli_core::print::Options,
}

impl<'a> Component<'a> {
	pub fn new(cli: &'a mut Cli) -> Self {
		Self(cli)
	}
}

impl Component<'_> {
	pub async fn command_health(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let arg = tg::health::Arg {
			fields: args.fields,
		};
		let output = client
			.health(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the health"))?;
		self.print_serde(output, args.print).await?;
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
