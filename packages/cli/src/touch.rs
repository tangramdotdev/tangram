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
		let locations = args.locations;

		let referent = self.resolve(&args.reference).await?;
		let is_process = matches!(
			referent.node(),
			tg::get::Node::Id(id) if id.kind() == tg::id::Kind::Process
		);
		if is_process {
			let process = referent.try_map::<tg::process::Id, _>(|node| match node {
				tg::get::Node::Id(id) => id.try_into(),
				tg::get::Node::Pointer(_) => unreachable!(),
			})?;
			let process = tg::Reference::with_node_and_token(
				tg::reference::Node::Id(process.node.into()),
				process.options.token,
			);
			let args = crate::process::touch::Args { locations, process };
			self.command_process_touch(args).await?;
		} else {
			let object = referent.try_map::<tg::object::Id, _>(|node| match node {
				tg::get::Node::Id(id) => id.try_into(),
				tg::get::Node::Pointer(_) => Err(tg::error!("expected an object or process id")),
			})?;
			let object = tg::Reference::with_node_and_token(
				tg::reference::Node::Id(object.node.into()),
				object.options.token,
			);
			let args = crate::object::touch::Args { locations, object };
			self.command_object_touch(args).await?;
		}

		Ok(())
	}
}
