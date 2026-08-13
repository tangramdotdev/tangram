use {crate::Cli, tangram_client::prelude::*};

/// Wait for a process to finish or a sandbox to be destroyed.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub locations: crate::location::Args,

	#[command(flatten)]
	pub print: crate::print::Options,

	#[arg(index = 1)]
	pub reference: tg::Reference,
}

impl Cli {
	pub async fn command_wait(&mut self, args: Args) -> tg::Result<()> {
		let explicit_location = args.locations.get();
		let reference_location = args.reference.options().location.clone();
		let mut options = args.reference.options().clone();
		if let Some(location) = explicit_location.clone() {
			options.location = Some(location);
		}
		let reference =
			tg::Reference::with_node_and_options(args.reference.node().clone(), options);
		let referent = self.resolve(&reference).await?;
		let location = explicit_location
			.or_else(|| referent.options.location.clone().map(Into::into))
			.or(reference_location);
		let id = match referent.node {
			tg::get::Node::Id(id) => id,
			tg::get::Node::Pointer(_) => {
				return Err(tg::error!("expected a process or sandbox"));
			},
		};
		match id.kind() {
			tg::id::Kind::Process => {
				let process = tg::Referent::new(id.try_into()?, referent.options);
				self.command_process_wait_with_referent(process, location, args.print)
					.await?;
			},
			tg::id::Kind::Sandbox => {
				let sandbox = tg::Referent::new(id.try_into()?, referent.options);
				self.command_sandbox_wait_with_referent(sandbox, location, args.print)
					.await?;
			},
			_ => return Err(tg::error!(%id, "expected a process or sandbox")),
		}
		Ok(())
	}
}
