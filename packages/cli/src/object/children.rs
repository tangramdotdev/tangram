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

		// Print the children.
		let object = tg::Object::with_id(args.object);
		let data = object.data(&handle).await?;
		for child in data.children() {
			println!("{child}");
		}

		Ok(())
	}
}
