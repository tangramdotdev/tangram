use crate::{tree::Tree, Cli};
use crossterm::style::Stylize as _;
use futures::{
	stream::{self, FuturesUnordered},
	StreamExt as _, TryStreamExt as _,
};
use tangram_client as tg;

/// Display the object tree.
#[derive(Debug, clap::Args)]
pub struct Args {
	pub id: tg::object::Id,
	#[clap(short, long)]
	pub depth: Option<u32>,
}

impl Cli {
	pub async fn command_object_tree(&self, args: Args) -> tg::Result<()> {
		let tree = get_object_tree(&self.handle, args.id, 1, args.depth).await?;
		tree.print();
		Ok(())
	}
}

async fn get_object_tree<H>(
	handle: &H,
	object: tg::object::Id,
	current_depth: u32,
	max_depth: Option<u32>,
) -> tg::Result<Tree>
where
	H: tg::Handle,
{
	let title = match &object {
		tg::object::Id::Leaf(id) => id.to_string().green(),
		tg::object::Id::Branch(id) => id.to_string().blue(),
		tg::object::Id::Directory(id) => id.to_string().red(),
		tg::object::Id::File(id) => id.to_string().green(),
		tg::object::Id::Symlink(id) => id.to_string().blue(),
		tg::object::Id::Lock(id) => id.to_string().magenta(),
		tg::object::Id::Target(id) => id.to_string().red(),
	};
	let title = title.to_string();
	let children = tg::Object::with_id(object.clone())
		.data(handle, None)
		.await
		.map_err(|source| tg::error!(!source, %object, "failed to get the object data"))?
		.children();
	let children = if max_depth.map_or(true, |max_depth| current_depth < max_depth) {
		stream::iter(children)
			.map(|child| get_object_tree(handle, child, current_depth + 1, max_depth))
			.collect::<FuturesUnordered<_>>()
			.await
			.try_collect::<Vec<_>>()
			.await?
	} else {
		Vec::new()
	};
	let tree = Tree { title, children };
	Ok(tree)
}
