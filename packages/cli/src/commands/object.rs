use crate::{util::TreeDisplay, Cli};
use async_recursion::async_recursion;
use console::style;
use futures::{
	stream::{self, FuturesUnordered},
	StreamExt, TryStreamExt,
};
use tangram_client as tg;
use tangram_error::{error, Result};
use tg::Handle;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Manage objects.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct Args {
	#[command(subcommand)]
	pub command: Command,
}

#[derive(Debug, clap::Subcommand)]
#[command(verbatim_doc_comment)]
pub enum Command {
	Get(GetArgs),
	Put(PutArgs),
	Push(PushArgs),
	Pull(PullArgs),
	Tree(TreeArgs),
}

/// Get an object.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct GetArgs {
	pub id: tg::object::Id,
}

/// Put an object.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct PutArgs {
	#[clap(short, long)]
	bytes: Option<String>,
	#[clap(short, long)]
	kind: tg::object::Kind,
}

/// Push an object.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct PushArgs {
	pub id: tg::object::Id,
}

/// Pull an object.
#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct PullArgs {
	pub id: tg::object::Id,
}

#[derive(Debug, clap::Args)]
#[command(verbatim_doc_comment)]
pub struct TreeArgs {
	pub id: tg::object::Id,
	#[arg(short, long)]
	pub depth: Option<u32>,
}

impl Cli {
	pub async fn command_object(&self, args: Args) -> Result<()> {
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

	pub async fn command_object_get(&self, args: GetArgs) -> Result<()> {
		let client = &self.client().await?;
		let tg::object::GetOutput { bytes, .. } = client.get_object(&args.id).await?;
		tokio::io::stdout()
			.write_all(&bytes)
			.await
			.map_err(|error| error!(source = error, "failed to write the data"))?;
		Ok(())
	}

	pub async fn command_object_put(&self, args: PutArgs) -> Result<()> {
		let client = &self.client().await?;
		let kind = args.kind.into();
		let bytes = if let Some(bytes) = args.bytes {
			bytes.into_bytes()
		} else {
			let mut bytes = Vec::new();
			tokio::io::stdin()
				.read_to_end(&mut bytes)
				.await
				.map_err(|error| error!(source = error, "failed to read stdin"))?;
			bytes
		};
		let id = tg::Id::new_blake3(kind, &bytes).try_into().unwrap();
		let arg = tg::object::PutArg {
			bytes: bytes.into(),
			count: None,
			weight: None,
		};
		client.put_object(&id, &arg).await?;
		Ok(())
	}

	pub async fn command_object_push(&self, args: PushArgs) -> Result<()> {
		let client = &self.client().await?;
		client.push_object(&args.id).await?;
		Ok(())
	}

	pub async fn command_object_pull(&self, args: PullArgs) -> Result<()> {
		let client = &self.client().await?;
		client.pull_object(&args.id).await?;
		Ok(())
	}

	pub async fn command_object_tree(&self, args: TreeArgs) -> Result<()> {
		let client = &self.client().await?;
		let tree = get_object_tree(client, args.id, 1, args.depth).await?;
		println!("{tree}");
		Ok(())
	}
}

#[async_recursion]
async fn get_object_tree(
	client: &dyn tg::Handle,
	object: tg::object::Id,
	current_depth: u32,
	max_depth: Option<u32>,
) -> Result<TreeDisplay> {
	let data = match &object {
		tg::object::Id::Leaf(object) => style(object).white().to_string(),
		tg::object::Id::Branch(object) => style(object).blue().bright().to_string(),
		tg::object::Id::Directory(object) => style(object).blue().dim().to_string(),
		tg::object::Id::File(object) => style(object).yellow().to_string(),
		tg::object::Id::Symlink(object) => style(object).green().to_string(),
		tg::object::Id::Lock(object) => style(object).red().to_string(),
		tg::object::Id::Target(object) => style(object).magenta().to_string(),
	};
	let children = tg::Object::with_id(object.clone())
		.data(client)
		.await
		.map_err(|error| error!(source = error, %object, "Failed to get object data."))?
		.children();

	let children = if max_depth.map_or(true, |max_depth| current_depth < max_depth) {
		stream::iter(children)
			.map(|child| get_object_tree(client, child, current_depth + 1, max_depth))
			.collect::<FuturesUnordered<_>>()
			.await
			.try_collect::<Vec<_>>()
			.await?
	} else {
		Vec::new()
	};

	let tree = TreeDisplay { data, children };

	Ok(tree)
}
