use {crate::Cli, std::collections::BTreeSet, tangram_client::prelude::*};

/// Get an object's children.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub object: tg::object::Id,

	#[command(flatten)]
	pub print: crate::print::Options,
}

impl Cli {
	pub async fn command_object_children(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let object = tg::Object::with_id(args.object);
		let data = object.data(&handle).await?;
		let mut children = BTreeSet::new();
		data.children(&mut children);
		let output = children.into_iter().collect::<Vec<_>>();
		self.print_serde(output, args.print).await?;
		Ok(())
	}
}
