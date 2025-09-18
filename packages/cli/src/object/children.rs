use crate::Cli;
use std::collections::BTreeSet;
use tangram_client as tg;

/// Get an object's children.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub object: tg::object::Id,
}

impl Cli {
	pub async fn command_object_children(&mut self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Print the children.
		let object = tg::Object::with_id(args.object);
		let data = object.data(&handle).await?;
		let mut children = BTreeSet::new();
		data.children(&mut children);
		for child in children {
			println!("{child}");
		}

		Ok(())
	}
}
