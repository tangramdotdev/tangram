use {tangram_cli_core::Cli, tangram_cli_get::Ext as _, tangram_client::prelude::*};

/// The component.
pub struct Component<'a>(&'a mut Cli);

/// Touch an object or a process.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub locations: tangram_cli_core::location::Args,

	#[arg(index = 1)]
	pub reference: tg::Reference,
}

impl<'a> Component<'a> {
	pub fn new(cli: &'a mut Cli) -> Self {
		Self(cli)
	}
}

impl Component<'_> {
	pub async fn command_touch(&mut self, mut args: Args) -> tg::Result<()> {
		args.locations.set_from_reference_if_unset(&args.reference);
		let reference = args.locations.apply_to_reference(&args.reference);
		let locations = args.locations;

		let referent = self.get_with_follow(&reference).await?;
		match referent.node {
			tg::get::Node::Id(id) if id.kind() == tg::id::Kind::Process => {
				let process = tg::Referent::new(id.try_into()?, referent.options);
				let entry = tangram_cli_process::touch::Options { locations };
				tangram_cli_process::Component::new(self)
					.command_process_touch_inner(process, entry)
					.await?;
			},
			tg::get::Node::Id(id) => {
				let object = tg::Referent::new(id.try_into()?, referent.options);
				let options = tangram_cli_object::touch::Options { locations };
				tangram_cli_object::Component::new(self)
					.command_object_touch_inner(object, options)
					.await?;
			},
			tg::get::Node::Pointer(_) => {
				return Err(tg::error!("expected an object or process id"));
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
