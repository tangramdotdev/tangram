use crate::Cli;
use either::Either;
use futures::{stream::FuturesOrdered, TryStreamExt as _};
use tangram_client::{self as tg, handle::Ext as _};

/// Display a tree for a build or value.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// The tree depth.
	#[arg(long)]
	pub depth: Option<u32>,

	/// If this flag is set, the package's lockfile will not be updated.
	#[arg(long, default_value = "false")]
	pub locked: bool,

	/// The remote to use.
	#[arg(short, long)]
	pub remote: Option<String>,

	/// The reference to display a tree for.
	#[arg(index = 1)]
	pub reference: tg::Reference,
}

#[derive(Debug)]
pub struct Tree {
	pub title: String,
	pub children: Vec<Self>,
}

impl Cli {
	pub async fn command_tree(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;
		let item = handle
			.get_reference(&args.reference)
			.await
			.map_err(|source| tg::error!(!source, "failed to get reference"))?
			.item;
		match item {
			Either::Left(_build) => {
				todo!()
			},
			Either::Right(object) => {
				let value = tg::Value::Object(tg::Object::with_id(object));
				let Some(tree) = self
					.value_tree(None, value, 0, args.depth.unwrap_or(u32::MAX))
					.await
					.map_err(|source| tg::error!(!source, "failed to create value tree"))?
				else {
					return Ok(());
				};
				tree.print();
			},
		}
		Ok(())
	}

	async fn value_tree(
		&self,
		label: Option<&str>,
		value: tg::Value,
		current_depth: u32,
		max_depth: u32,
	) -> tg::Result<Option<Tree>> {
		if current_depth == max_depth {
			return Ok(None);
		}
		let handle = self.handle().await?;

		let (title, children) = match &value {
			tg::Value::Null => ("null".to_owned(), vec![]),
			tg::Value::Bool(value) => (value.to_string(), vec![]),
			tg::Value::Number(value) => (value.to_string(), vec![]),
			tg::Value::String(value) => (value.to_string(), vec![]),
			tg::Value::Array(value) => {
				let children = value
					.iter()
					.map(|value| {
						Box::pin(self.value_tree(None, value.clone(), current_depth + 1, max_depth))
					})
					.collect::<FuturesOrdered<_>>()
					.try_collect()
					.await?;
				(String::new(), children)
			},
			tg::Value::Map(value) => {
				let children = value
					.iter()
					.map(|(label, value)| {
						Box::pin(self.value_tree(
							Some(label),
							value.clone(),
							current_depth + 1,
							max_depth,
						))
					})
					.collect::<FuturesOrdered<_>>()
					.try_collect()
					.await?;
				(String::new(), children)
			},
			tg::Value::Object(tg::Object::Branch(object)) => {
				let title = object.id(&handle).await?.to_string();
				let children = object
					.children(&handle)
					.await?
					.iter()
					.map(|child| {
						let object = tg::Value::Object(child.blob.clone().into());
						Box::pin(self.value_tree(None, object, current_depth + 1, max_depth))
					})
					.collect::<FuturesOrdered<_>>()
					.try_collect()
					.await?;
				(title, children)
			},
			tg::Value::Object(tg::Object::Directory(object)) => {
				let title = object.id(&handle).await?.to_string();
				let children = object
					.entries(&handle)
					.await?
					.iter()
					.map(|(label, artifact)| {
						let value = artifact.clone().into();
						Box::pin(self.value_tree(Some(label), value, current_depth + 1, max_depth))
					})
					.collect::<FuturesOrdered<_>>()
					.try_collect()
					.await?;
				(title, children)
			},
			tg::Value::Object(tg::Object::Lock(lock)) => {
				// let title = lock.id(&handle).await?.to_string();
				// let metadata = lock.metadata(&handle).await?;
				// let value = tg::Value::Map(metadata.clone());
				// let mut children = vec![
				// 	Box::pin(self.value_tree(Some("metadata"), value, current_depth, max_depth))
				// 		.await?,
				// ];
				// for dependency in lock.dependencies(&handle).await? {
				// 	let package = lock.get_dependency(&handle, &dependency).await?;
				// 	let child = Box::pin(self.value_tree(
				// 		Some(&dependency.to_string()),
				// 		tg::Value::Object(package),
				// 		current_depth,
				// 		max_depth,
				// 	))
				// 	.await?;
				// 	children.push(child);
				// }
				// (title, children)
				todo!()
			},
			tg::Value::Object(object) => {
				let title = object.id(&handle).await?.to_string();
				(title, vec![])
			},
			tg::Value::Bytes(_) => ("bytes".into(), vec![]),
			tg::Value::Path(path) => (path.to_string(), vec![]),
			tg::Value::Mutation(_) => todo!(),
			tg::Value::Template(_) => todo!(),
		};
		let title = label
			.map(|label| format!("{label}: {title}"))
			.unwrap_or(title);
		let children = children.into_iter().flatten().collect();
		Ok(Some(Tree { title, children }))
	}
}

impl Tree {
	pub fn print(&self) {
		self.print_inner("");
		println!();
	}

	fn print_inner(&self, prefix: &str) {
		print!("{}", self.title);
		for (n, child) in self.children.iter().enumerate() {
			print!("\n{prefix}");
			if n < self.children.len() - 1 {
				print!("├── ");
				child.print_inner(&format!("{prefix}│   "));
			} else {
				print!("└── ");
				child.print_inner(&format!("{prefix}    "));
			}
		}
	}
}
