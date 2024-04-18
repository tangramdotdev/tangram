use crate::{tree::Tree, Cli};
use crossterm::style::Stylize;
use futures::{
	stream::{self, FuturesUnordered},
	StreamExt as _, TryStreamExt as _,
};
use tangram_client as tg;
use tg::Handle as _;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

/// Manage objects.
#[derive(Debug, clap::Args)]
pub struct Args {
	#[clap(subcommand)]
	pub command: Command,
}

#[derive(Debug, clap::Subcommand)]
pub enum Command {
	Get(GetArgs),
	Put(PutArgs),
	Push(PushArgs),
	Pull(PullArgs),
	Tree(TreeArgs),
}

/// Get an object.
#[derive(Debug, clap::Args)]
pub struct GetArgs {
	pub id: tg::object::Id,
}

/// Put an object.
#[derive(Debug, clap::Args)]
pub struct PutArgs {
	#[clap(short, long)]
	bytes: Option<String>,
	#[clap(short, long)]
	kind: tg::object::Kind,
}

/// Push an object.
#[derive(Debug, clap::Args)]
pub struct PushArgs {
	pub id: tg::object::Id,
}

/// Pull an object.
#[derive(Debug, clap::Args)]
pub struct PullArgs {
	pub id: tg::object::Id,
}

/// Display the object tree.
#[derive(Debug, clap::Args)]
pub struct TreeArgs {
	pub id: tg::object::Id,
	#[clap(short, long)]
	pub depth: Option<u32>,
}

impl Cli {
	pub async fn command_object(&self, args: Args) -> tg::Result<()> {
		match args.command {
			Command::Get(args) => {
				self.command_object_get(args).await?;
			},
			Command::Put(args) => {
				self.command_object_put(args).await?;
			},
			Command::Push(args) => {
				self.command_object_push(args).await?;
			},
			Command::Pull(args) => {
				self.command_object_pull(args).await?;
			},
			Command::Tree(args) => {
				self.command_object_tree(args).await?;
			},
		}
		Ok(())
	}

	pub async fn command_object_get(&self, args: GetArgs) -> tg::Result<()> {
		let tg::object::GetOutput { bytes, .. } = self.handle.get_object(&args.id).await?;
		tokio::io::stdout()
			.write_all(&bytes)
			.await
			.map_err(|source| tg::error!(!source, "failed to write the data"))?;
		Ok(())
	}

	pub async fn command_object_put(&self, args: PutArgs) -> tg::Result<()> {
		let kind = args.kind.into();
		let bytes = if let Some(bytes) = args.bytes {
			bytes.into_bytes()
		} else {
			let mut bytes = Vec::new();
			tokio::io::stdin()
				.read_to_end(&mut bytes)
				.await
				.map_err(|source| tg::error!(!source, "failed to read stdin"))?;
			bytes
		};
		let id = tg::Id::new_blake3(kind, &bytes).try_into().unwrap();
		let arg = tg::object::PutArg {
			bytes: bytes.into(),
			count: None,
			weight: None,
		};
		self.handle.put_object(&id, arg, None).await?;
		println!("{id}");
		Ok(())
	}

	pub async fn command_object_push(&self, args: PushArgs) -> tg::Result<()> {
		self.handle.push_object(&args.id).await?;
		Ok(())
	}

	pub async fn command_object_pull(&self, args: PullArgs) -> tg::Result<()> {
		self.handle.pull_object(&args.id).await?;
		Ok(())
	}

	pub async fn command_object_tree(&self, args: TreeArgs) -> tg::Result<()> {
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
