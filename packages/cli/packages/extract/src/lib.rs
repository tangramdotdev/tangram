use {
	futures::FutureExt as _, tangram_cli_core::Cli, tangram_cli_get::Ext as _,
	tangram_client::prelude::*,
};

/// The component.
pub struct Component<'a>(&'a mut Cli);

/// Extract an artifact from a blob.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub build: tangram_cli_process::build::Options,

	#[arg(index = 1)]
	pub reference: tg::Reference,
}

impl<'a> Component<'a> {
	pub fn new(cli: &'a mut Cli) -> Self {
		Self(cli)
	}
}

impl Component<'_> {
	pub async fn command_extract(&mut self, args: Args) -> tg::Result<()> {
		let client = self.client().await?;
		let blob = self.get_object(&args.reference).await?;
		let blob = tg::Object::with_referent(blob)
			.try_unwrap_blob()
			.map_err(|_| tg::error!("expected a blob"))?;
		let command = tg::builtin::extract_command(&blob);
		let command = command
			.store_with_instance(&client)
			.await
			.map_err(|error| tg::error!(!error, "failed to store the command"))?;
		let reference = tg::Reference::with_object(command.into());
		let args = tangram_cli_process::build::Args {
			options: args.build,
			reference: Some(reference),
			trailing: Vec::new(),
		};
		tangram_cli_process::Component::new(self)
			.command_build(args)
			.boxed()
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
