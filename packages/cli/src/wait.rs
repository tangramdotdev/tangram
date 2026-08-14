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
		let reference = args.locations.apply_to_reference(&args.reference);
		let referent = self.resolve(&reference).await?;
		let id = match referent.node {
			tg::get::Node::Id(id) => id,
			tg::get::Node::Pointer(_) => {
				return Err(tg::error!("expected a process or sandbox"));
			},
		};
		match id.kind() {
			tg::id::Kind::Process => {
				let process = tg::Referent::new(id.try_into()?, referent.options);
				let options = crate::process::wait::Options {
					locations: crate::location::Args::default(),
					print: args.print,
				};
				self.command_process_wait_inner(process, options).await?;
			},
			tg::id::Kind::Sandbox => {
				let sandbox = tg::Referent::new(id.try_into()?, referent.options);
				let sandbox = tg::Sandbox::with_referent(sandbox);
				let options = crate::sandbox::wait::Options {
					locations: crate::location::Args::default(),
					print: args.print,
				};
				self.command_sandbox_wait_inner(sandbox, options).await?;
			},
			_ => return Err(tg::error!(%id, "expected a process or sandbox")),
		}
		Ok(())
	}
}
