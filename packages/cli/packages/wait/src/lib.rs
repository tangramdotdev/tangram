use {tangram_cli_core::Cli, tangram_cli_get::Ext as _, tangram_client::prelude::*};

/// The component.
pub struct Component<'a>(&'a mut Cli);

/// Wait for a process to finish or a sandbox to be destroyed.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub locations: tangram_cli_core::location::Args,

	#[command(flatten)]
	pub print: tangram_cli_core::print::Options,

	#[arg(index = 1)]
	pub reference: tg::Reference,

	/// Select the source of process or sandbox state.
	#[arg(long, default_value = "auto", value_parser = tangram_cli_core::source_parser())]
	pub source: tg::process::Source,
}

impl<'a> Component<'a> {
	pub fn new(cli: &'a mut Cli) -> Self {
		Self(cli)
	}
}

impl Component<'_> {
	pub async fn command_wait(&mut self, mut args: Args) -> tg::Result<()> {
		args.locations.set_from_reference_if_unset(&args.reference);
		let reference = args.locations.apply_to_reference(&args.reference);
		let locations = args.locations;
		let referent = self.get_with_follow(&reference).await?;
		let id = match referent.node {
			tg::get::Node::Id(id) => id,
			tg::get::Node::Pointer(_) => {
				return Err(tg::error!("expected a process or sandbox"));
			},
		};
		match id.kind() {
			tg::id::Kind::Process => {
				let process = tg::Referent::new(id.try_into()?, referent.options);
				let options = tangram_cli_process::wait::Options {
					locations,
					print: args.print,
					source: args.source,
				};
				tangram_cli_process::Component::new(self)
					.command_process_wait_inner(process, options)
					.await?;
			},
			tg::id::Kind::Sandbox => {
				let sandbox = tg::Referent::new(id.try_into()?, referent.options);
				let sandbox = tg::Sandbox::with_referent(sandbox);
				let options = tangram_cli_sandbox::wait::Options {
					locations,
					print: args.print,
					source: args.source,
				};
				tangram_cli_sandbox::Component::new(self)
					.command_sandbox_wait_inner(sandbox, options)
					.await?;
			},
			_ => return Err(tg::error!(%id, "expected a process or sandbox")),
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
