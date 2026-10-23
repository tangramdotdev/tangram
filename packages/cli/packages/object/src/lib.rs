use {tangram_cli_core::Cli, tangram_client::prelude::*};

pub mod availability;
pub mod children;
pub mod metadata;
pub mod put;
pub mod touch;

/// The component.
pub struct Component<'a>(&'a mut Cli);

/// Manage objects.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(subcommand)]
	pub command: Command,
}

#[derive(Clone, Debug, clap::Subcommand)]
pub enum Command {
	Availability(self::availability::Args),
	Children(self::children::Args),
	Get(tangram_cli_get::object::Args),
	Metadata(self::metadata::Args),
	#[command(alias = "add")]
	Put(self::put::Args),
	Touch(self::touch::Args),
}

impl<'a> Component<'a> {
	pub fn new(cli: &'a mut Cli) -> Self {
		Self(cli)
	}
}

impl Component<'_> {
	pub async fn command_object(&mut self, args: Args) -> tg::Result<()> {
		match args.command {
			Command::Availability(args) => {
				self.command_object_availability(args).await?;
			},
			Command::Children(args) => {
				self.command_object_children(args).await?;
			},
			Command::Get(args) => {
				tangram_cli_get::Component::new(self)
					.command_object_get(args)
					.await?;
			},
			Command::Metadata(args) => {
				self.command_object_metadata(args).await?;
			},
			Command::Put(args) => {
				self.command_object_put(args).await?;
			},
			Command::Touch(args) => {
				self.command_object_touch(args).await?;
			},
		}
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
