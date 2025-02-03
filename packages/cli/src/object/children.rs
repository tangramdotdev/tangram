use crate::Cli;
use tangram_client as tg;

/// Get an object's children.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub object: tg::object::Id,
}

impl Cli {
	pub async fn command_object_children(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the object.
		let object = tg::Object::with_id(args.object);

		// Get the children.
		let children = object.children(&handle).await?;
		for child in children {
			println!("{}", child.id(&handle).await?);
		}

		Ok(())
	}
}
