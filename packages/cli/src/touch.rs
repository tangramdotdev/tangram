use {crate::Cli, tangram_client::prelude::*};

/// Touch an object or a process.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub locations: crate::location::Args,

	#[arg(index = 1)]
	pub reference: tg::Reference,
}

impl Cli {
	pub async fn command_touch(&mut self, args: Args) -> tg::Result<()> {
		let reference = args.locations.apply_to_reference(&args.reference);

		let referent = self.resolve(&reference).await?;
		match referent.node {
			tg::get::Node::Id(id) if id.kind() == tg::id::Kind::Process => {
				let process = tg::Referent::new(id.try_into()?, referent.options);
				self.command_process_touch_inner(
					process,
					crate::process::touch::Options::default(),
				)
				.await?;
			},
			tg::get::Node::Id(id) => {
				let object = tg::Referent::new(id.try_into()?, referent.options);
				self.command_object_touch_inner(object, crate::object::touch::Options::default())
					.await?;
			},
			tg::get::Node::Pointer(_) => {
				return Err(tg::error!("expected an object or process id"));
			},
		}

		Ok(())
	}
}
