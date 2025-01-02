use super::{Item, Options};
use crossterm as ct;
use futures::{Future, StreamExt, TryFutureExt as _, TryStreamExt as _};
use num::ToPrimitive as _;
use ratatui::{self as tui, prelude::*};
use std::{
	cell::RefCell,
	collections::BTreeMap,
	fmt::Write,
	rc::{Rc, Weak},
};
use tangram_client as tg;
use tangram_either::Either;
use tangram_futures::task::Task;

const SPINNER: [char; 10] = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];

pub struct Tree<H> {
	handle: H,
	rect: Option<Rect>,
	roots: Vec<Rc<RefCell<Node>>>,
	selected: Rc<RefCell<Node>>,
	scroll: usize,
}

struct Node {
	children: Vec<Rc<RefCell<Self>>>,
	expand_task: Option<Task<()>>,
	expanded: bool,
	indicator: Option<Indicator>,
	item: Option<Item>,
	label: Option<String>,
	options: Rc<Options>,
	parent: Option<Weak<RefCell<Self>>>,
	ready_sender: tokio::sync::watch::Sender<bool>,
	title: String,
	update_receiver: NodeUpdateReceiver,
	update_sender: NodeUpdateSender,
	update_task: Option<Task<()>>,
}

#[derive(Clone, Copy, Debug)]
pub enum Indicator {
	Created,
	Dequeued,
	Started,
	Canceled,
	Failed,
	Succeeded,
}

type NodeUpdateSender = std::sync::mpsc::Sender<Box<dyn FnOnce(Rc<RefCell<Node>>)>>;

type NodeUpdateReceiver = std::sync::mpsc::Receiver<Box<dyn FnOnce(Rc<RefCell<Node>>)>>;

pub struct Display {
	title: String,
	children: Vec<Self>,
}

impl<H> Tree<H>
where
	H: tg::Handle,
{
	fn ancestors(node: &Rc<RefCell<Node>>) -> Vec<Rc<RefCell<Node>>> {
		let mut ancestors = vec![node.clone()];
		let mut node = node.clone();
		loop {
			let Some(parent) = node
				.borrow()
				.parent
				.clone()
				.map(|parent| parent.upgrade().unwrap())
			else {
				break;
			};
			ancestors.push(parent.clone());
			node = parent;
		}
		ancestors
	}

	fn bottom(&mut self) {
		let nodes = self.nodes();
		self.selected = nodes.last().unwrap().clone();
		let height = self.rect.as_ref().unwrap().height.to_usize().unwrap();
		self.scroll = nodes.len().saturating_sub(height);
	}

	fn collapse(&mut self) {
		if self.selected.borrow().expanded {
			let mut node = self.selected.borrow_mut();
			node.children.clear();
			if let Some(task) = node.expand_task.take() {
				task.abort();
			}
			node.expanded = false;
		} else {
			let parent = self
				.selected
				.borrow()
				.parent
				.as_ref()
				.map(|parent| parent.upgrade().unwrap());
			if let Some(parent) = parent {
				self.selected = parent;
			}
		}
	}

	fn create_node(
		parent: &Rc<RefCell<Node>>,
		label: Option<String>,
		item: Option<&Item>,
	) -> Rc<RefCell<Node>> {
		let (update_sender, update_receiver) = std::sync::mpsc::channel();
		let (ready_sender, _) = tokio::sync::watch::channel(false);
		let options = parent.borrow().options.clone();
		let parent = Rc::downgrade(parent);
		let title = item.map_or(String::new(), |item| Self::item_title(item));

		Rc::new(RefCell::new(Node {
			children: Vec::new(),
			expand_task: None,
			expanded: false,
			indicator: None,
			item: item.cloned(),
			label,
			options,
			parent: Some(parent),
			ready_sender,
			title,
			update_receiver,
			update_sender,
			update_task: None,
		}))
	}

	pub fn display(&self) -> Display {
		let now = std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.unwrap()
			.as_millis();
		Self::display_node(&self.roots.last().unwrap(), now)
	}

	fn display_node(node: &Rc<RefCell<Node>>, now: u128) -> Display {
		let mut title = String::new();
		let indicator = match node.borrow().indicator {
			None => None,
			Some(Indicator::Created) => Some(crossterm::style::Stylize::yellow('⟳')),
			Some(Indicator::Dequeued) => Some(crossterm::style::Stylize::yellow('•')),
			Some(Indicator::Started) => {
				let position = (now / (1000 / 10)) % 10;
				let position = position.to_usize().unwrap();
				Some(crossterm::style::Stylize::blue(SPINNER[position]))
			},
			Some(Indicator::Canceled) => Some(crossterm::style::Stylize::yellow('⦻')),
			Some(Indicator::Failed) => Some(crossterm::style::Stylize::red('✗')),
			Some(Indicator::Succeeded) => Some(crossterm::style::Stylize::green('✓')),
		};
		if let Some(indicator) = indicator {
			title.push_str(&indicator.to_string());
			title.push_str(" ");
		}
		if let Some(label) = node.borrow().label.clone() {
			title.push_str(&label);
			title.push_str(": ");
		}
		title.push_str(&node.borrow().title);
		let children = node
			.borrow()
			.children
			.iter()
			.map(|node| Self::display_node(node, now))
			.collect();
		Display { title, children }
	}

	fn down(&mut self) {
		let nodes = self.nodes();
		let index = nodes
			.iter()
			.position(|node| Rc::ptr_eq(node, &self.selected))
			.unwrap();
		let index = (index + 1).min(nodes.len() - 1);
		self.selected = nodes[index].clone();
		let height = self.rect.as_ref().unwrap().height.to_usize().unwrap();
		if index >= self.scroll + height {
			self.scroll += 1;
		}
	}

	fn expand(&mut self) {
		let mut node = self.selected.borrow_mut();
		let Some(item) = node.item.clone() else {
			return;
		};
		if node.expanded {
			return;
		}
		let children_task = Task::spawn_local({
			let handle = self.handle.clone();
			let update_sender = node.update_sender.clone();
			let ready_sender = node.ready_sender.clone();
			move |_| async move { Self::expand_task(&handle, item, update_sender, ready_sender).await }
		});
		node.expand_task.replace(children_task);
		node.expanded = true;
	}

	pub fn handle(&mut self, event: &ct::event::Event) {
		if let ct::event::Event::Key(event) = event {
			match (event.code, event.modifiers) {
				(ct::event::KeyCode::Char('G'), ct::event::KeyModifiers::SHIFT) => {
					self.bottom();
				},
				(ct::event::KeyCode::Char('g'), ct::event::KeyModifiers::NONE) => {
					self.top();
				},
				(
					ct::event::KeyCode::Char('h') | ct::event::KeyCode::Left,
					ct::event::KeyModifiers::NONE,
				) => {
					self.collapse();
				},
				(
					ct::event::KeyCode::Char('j') | ct::event::KeyCode::Down,
					ct::event::KeyModifiers::NONE,
				) => {
					self.down();
				},
				(
					ct::event::KeyCode::Char('k') | ct::event::KeyCode::Up,
					ct::event::KeyModifiers::NONE,
				) => {
					self.up();
				},
				(
					ct::event::KeyCode::Char('l') | ct::event::KeyCode::Right,
					ct::event::KeyModifiers::NONE,
				) => {
					self.expand();
				},
				(ct::event::KeyCode::Enter, ct::event::KeyModifiers::NONE) => {
					self.push();
				},
				(ct::event::KeyCode::Backspace, ct::event::KeyModifiers::NONE) => {
					self.pop();
				},
				_ => (),
			}
		}
	}

	async fn expand_array(
		_handle: &H,
		array: tg::value::Array,
		update_sender: NodeUpdateSender,
	) -> tg::Result<()> {
		let update = |node: Rc<RefCell<Node>>| {
			for value in array {
				let item = Item::Value(value);
				let child = Self::create_node(&node, None, Some(&item));
				node.borrow_mut().children.push(child);
			}
		};
		update_sender.send(Box::new(update)).unwrap();
		Ok(())
	}

	async fn expand_branch(
		handle: &H,
		branch: tg::Branch,
		update_sender: NodeUpdateSender,
	) -> tg::Result<()> {
		// Get the branch children and unload the object immediately.
		let children = branch
			.children_(handle)
			.await?
			.into_iter()
			.map(tg::Value::Object)
			.collect();
		branch.unload();

		// Convert to a value and send the update.
		let value = tg::Value::Array(children);
		let update = |node: Rc<RefCell<Node>>| {
			let item = Item::Value(value);
			let child = Self::create_node(&node, Some("children".to_owned()), Some(&item));
			node.borrow_mut().children.push(child);
		};
		update_sender.send(Box::new(update)).unwrap();
		Ok(())
	}

	async fn build_update_task(
		handle: &H,
		build: tg::Build,
		options: &Options,
		update_sender: NodeUpdateSender,
	) -> tg::Result<()>
	where
		H: tg::Handle,
	{
		if let Some(title) = Self::build_title(handle, &build).await {
			update_sender
				.send(Box::new(|node| {
					node.borrow_mut().label.replace(title);
				}))
				.unwrap();
		}

		// Create the status stream.
		let mut status = build.status(handle).await?;
		while let Some(status) = status.try_next().await? {
			let indicator = match status {
				tg::build::Status::Created => Indicator::Created,
				tg::build::Status::Dequeued => Indicator::Dequeued,
				tg::build::Status::Started => Indicator::Started,
				tg::build::Status::Finished => {
					// Remove the child if necessary.
					if options.collapse_finished_builds {
						let id = build.id().clone();
						let update = move |node: Rc<RefCell<Node>>| {
							// Get the parent if it exists.
							let Some(parent) =
								node.borrow().parent.as_ref().and_then(Weak::upgrade)
							else {
								return;
							};

							// Find this build as a child of the parent.
							let Some(index) = parent.borrow().children.iter().position(|child| {
								match &child.borrow().item {
									Some(Item::Build(build)) if build.id() == &id => true,
									_ => false,
								}
							}) else {
								return;
							};

							// Remove this node from its parent.
							parent.borrow_mut().children.remove(index);
						};
						update_sender.send(Box::new(update)).unwrap();
						return Ok(());
					}

					let outcome = build.outcome(handle).await?;
					match outcome {
						tg::build::Outcome::Cancelation(_) => Indicator::Canceled,
						tg::build::Outcome::Failure(_) => Indicator::Failed,
						tg::build::Outcome::Success(_) => Indicator::Succeeded,
					}
				},
			};
			let update = move |node: Rc<RefCell<Node>>| {
				node.borrow_mut().indicator.replace(indicator);
			};
			update_sender.send(Box::new(update)).unwrap();
		}

		Ok(())
	}

	async fn build_log_task(handle: &H, build: tg::Build, update_sender: NodeUpdateSender) -> tg::Result<()> {
		let mut log = build.log(handle, tg::build::log::get::Arg::default()).await?;

		while let Some(chunk) = log.try_next().await? {
			let chunk = String::from_utf8_lossy(&chunk.bytes);
			for line in chunk.lines() {
				let line = line.to_owned();
				let update = move |node: Rc<RefCell<Node>>| {
					// Get or create the log node.
					let log_node = node
						.borrow()
						.children
						.iter()
						.position(|node| node.borrow().label.as_deref() == Some("log"));
					let log_node = log_node.unwrap_or_else(|| {
						let child = Self::create_node(&node, Some("log".to_owned()), None);
						let index = node.borrow().children.len();
						node.borrow_mut().children.push(child);
						index
					});
					let log_node = &node.borrow().children[log_node];
					log_node.borrow_mut().title = line;	
				};
				update_sender.send(Box::new(update)).unwrap();
			}
		}

		Ok(())
	}

	async fn expand_build(
		handle: &H,
		build: tg::Build,
		update_sender: NodeUpdateSender,
	) -> tg::Result<()> {
		// Get the target.
		let target = build.target(handle).await?;
		let value = tg::Value::Object(target.into());
		update_sender
			.send(Box::new(move |node| {
				let child =
					Self::create_node(&node, Some("target".to_owned()), Some(&Item::Value(value)));
				node.borrow_mut().children.push(child);
			}))
			.unwrap();

		// Create the children stream.
		let mut children = build
			.children(handle, tg::build::children::get::Arg::default())
			.await?;
		while let Some(build) = children.try_next().await? {
			let handle = handle.clone();
			let update = move |node: Rc<RefCell<Node>>| {
				// Get or create the children.
				let children_node = node
					.borrow()
					.children
					.iter()
					.position(|node| node.borrow().label.as_deref() == Some("children"));
				let children_node = children_node.unwrap_or_else(|| {
					let child = Self::create_node(&node, Some("children".to_owned()), None);
					let index = node.borrow().children.len();
					node.borrow_mut().children.push(child);
					index
				});
				let children_node = &node.borrow().children[children_node];

				// Create the child.
				let item = Item::Build(build.clone());
				let child = Self::create_node(&children_node, None, Some(&item));

				// Create the update task.
				let update_task = Task::spawn_local({
					let options = child.borrow().options.clone();
					let update_sender = child.borrow().update_sender.clone();
					|_| async move {
						Self::build_update_task(&handle, build, options.as_ref(), update_sender)
							.await
							.ok();
					}
				});
				child.borrow_mut().update_task.replace(update_task);

				// Add the child to the children node.
				children_node.borrow_mut().children.push(child);
			};
			update_sender.send(Box::new(update)).unwrap();
		}
		Ok(())
	}

	async fn build_title(handle: &H, build: &tg::Build) -> Option<String> {
		let target = build.target(handle).await.ok()?;
		let args = target.args(handle).await.ok()?;
		let executable = target.executable(handle).await.ok()?;

		let mut title = String::new();
		match &*executable {
			Some(tg::target::Executable::Module(module)) => {
				if let Some(path) = &module.referent.path {
					let path = module
						.referent
						.subpath
						.as_ref()
						.map(|subpath| path.join(subpath))
						.unwrap_or_else(|| path.to_owned());
					write!(title, "{} ", path.display()).unwrap();
				} else if let Some(tag) = &module.referent.tag {
					write!(title, "{tag} ").unwrap();
				}
			},
			None => write!(title, "builtin ").unwrap(),
			_ => (),
		};

		if let Some(tg::Value::String(arg0)) = args.get(0) {
			write!(title, "{arg0}").unwrap();
		}

		Some(title)
	}

	async fn handle_directory(
		handle: &H,
		directory: tg::Directory,
		update_sender: NodeUpdateSender,
	) -> tg::Result<()> {
		let object = directory.object(handle).await?;
		let children: Vec<_> = match object.as_ref() {
			tg::directory::Object::Graph { graph, node } => [
				("graph".to_owned(), tg::Value::Object(graph.clone().into())),
				("node".to_owned(), tg::Value::Number(node.to_f64().unwrap())),
			]
			.into_iter()
			.collect(),
			tg::directory::Object::Normal { entries } => {
				let entries = entries
					.clone()
					.into_iter()
					.map(|(name, artifact)| (name, artifact.into()))
					.collect();
				[("entries".to_owned(), tg::Value::Map(entries))]
					.into_iter()
					.collect()
			},
		};
		directory.unload();

		// Send the update.
		let update = |node: Rc<RefCell<Node>>| {
			for (name, child) in children {
				let item = Item::Value(child);
				let child = Self::create_node(&node, Some(name), Some(&item));
				node.borrow_mut().children.push(child);
			}
		};
		update_sender.send(Box::new(update)).unwrap();
		Ok(())
	}

	async fn handle_file(
		handle: &H,
		file: tg::File,
		update_sender: NodeUpdateSender,
	) -> tg::Result<()> {
		let object = file.object(handle).await?;
		let children = match object.as_ref() {
			tg::file::Object::Graph { graph, node } => [
				("graph".to_owned(), tg::Value::Object(graph.clone().into())),
				("node".to_owned(), tg::Value::Number(node.to_f64().unwrap())),
			]
			.into_iter()
			.collect(),
			tg::file::Object::Normal {
				contents,
				dependencies,
				executable,
			} => {
				let mut children = Vec::with_capacity(3);
				children.push((
					"contents".to_owned(),
					tg::Value::Object(contents.clone().into()),
				));
				if *executable {
					children.push(("executable".to_owned(), tg::Value::Bool(*executable)));
				}
				let dependencies = dependencies
					.clone()
					.into_iter()
					.map(|(reference, referent)| {
						let mut map = BTreeMap::new();
						let item = tg::Value::Object(referent.item);
						map.insert("item".into(), item);
						if let Some(path) = referent.path {
							let path = path.to_string_lossy().to_string();
							map.insert("path".to_owned(), tg::Value::String(path));
						}
						if let Some(subpath) = referent.subpath {
							let subpath = subpath.to_string_lossy().to_string();
							map.insert("subpath".to_owned(), tg::Value::String(subpath));
						}
						if let Some(tag) = referent.tag {
							map.insert("tag".to_owned(), tg::Value::String(tag.to_string()));
						}
						(reference.to_string(), tg::Value::Map(map))
					})
					.collect::<BTreeMap<_, _>>();
				if !dependencies.is_empty() {
					children.push(("dependencies".to_owned(), tg::Value::Map(dependencies)))
				}
				children
			},
		};
		file.unload();

		// Send the update.
		let update = |node: Rc<RefCell<Node>>| {
			for (name, child) in children {
				let item = Item::Value(child);
				let child = Self::create_node(&node, Some(name), Some(&item));
				node.borrow_mut().children.push(child);
			}
		};
		update_sender.send(Box::new(update)).unwrap();
		Ok(())
	}

	async fn handle_graph(
		handle: &H,
		graph: tg::Graph,
		update_sender: NodeUpdateSender,
	) -> tg::Result<()> {
		// Get the graph nodes and unload the object immediately.
		let nodes = graph.nodes(handle).await?;
		graph.unload();

		// Convert nodes to tg::Value::Maps
		let nodes = nodes
			.into_iter()
			.map(|node| {
				let mut map = BTreeMap::new();
				map.insert(
					"kind".to_owned(),
					tg::Value::String(node.kind().to_string()),
				);
				match node {
					tg::graph::Node::Directory(directory) => {
						let entries = directory
							.entries
							.into_iter()
							.map(|(name, entry)| {
								let value = match entry {
									Either::Left(index) => {
										tg::Value::Number(index.to_f64().unwrap())
									},
									Either::Right(artifact) => tg::Value::Object(artifact.into()),
								};
								(name, value)
							})
							.collect::<BTreeMap<_, _>>();
						map.insert("entries".into(), tg::Value::Map(entries));
					},
					tg::graph::Node::File(file) => {
						map.insert("contents".into(), tg::Value::Object(file.contents.into()));
						if file.executable {
							map.insert("executable".into(), file.executable.into());
						}
						let dependencies = file
							.dependencies
							.into_iter()
							.map(|(reference, referent)| {
								let mut map = BTreeMap::new();
								let item = match referent.item {
									Either::Left(index) => {
										tg::Value::Number(index.to_f64().unwrap())
									},
									Either::Right(object) => tg::Value::Object(object),
								};
								map.insert("item".into(), item);
								if let Some(path) = referent.path {
									let path = path.to_string_lossy().to_string();
									map.insert("path".to_owned(), tg::Value::String(path));
								}
								if let Some(subpath) = referent.subpath {
									let subpath = subpath.to_string_lossy().to_string();
									map.insert("subpath".to_owned(), tg::Value::String(subpath));
								}
								if let Some(tag) = referent.tag {
									map.insert(
										"tag".to_owned(),
										tg::Value::String(tag.to_string()),
									);
								}
								(reference.to_string(), tg::Value::Map(map))
							})
							.collect::<BTreeMap<_, _>>();
						if !dependencies.is_empty() {
							map.insert("dependencies".into(), tg::Value::Map(dependencies));
						}
					},
					tg::graph::Node::Symlink(symlink) => match symlink {
						tg::graph::object::Symlink::Artifact { artifact, subpath } => {
							let artifact = match artifact {
								Either::Left(index) => tg::Value::Number(index.to_f64().unwrap()),
								Either::Right(artifact) => tg::Value::Object(artifact.into()),
							};
							map.insert("artifact".to_owned(), artifact);
							if let Some(subpath) = subpath {
								let subpath = subpath.to_string_lossy().to_string();
								map.insert("subpath".into(), tg::Value::String(subpath));
							}
						},
						tg::graph::object::Symlink::Target { target } => {
							let target = target.to_string_lossy().to_string();
							map.insert("target".into(), tg::Value::String(target));
						},
					},
				}
				tg::Value::Map(map)
			})
			.collect();

		// Convert to a value and send the update.
		let value = tg::Value::Array(nodes);
		let update = |node: Rc<RefCell<Node>>| {
			let item = Item::Value(value);
			let child = Self::create_node(&node, Some("nodes".to_owned()), Some(&item));
			node.borrow_mut().children.push(child);
		};
		update_sender.send(Box::new(update)).unwrap();
		Ok(())
	}

	async fn handle_leaf(
		_handle: &H,
		_leaf: tg::Leaf,
		_update_sender: NodeUpdateSender,
	) -> tg::Result<()> {
		Ok(())
	}

	async fn expand_map(
		_handle: &H,
		map: tg::value::Map,
		update_sender: NodeUpdateSender,
	) -> tg::Result<()> {
		let update = |node: Rc<RefCell<Node>>| {
			for (name, value) in map {
				let item = Item::Value(value);
				let child = Self::create_node(&node, Some(name), Some(&item));
				node.borrow_mut().children.push(child);
			}
		};
		update_sender.send(Box::new(update)).unwrap();
		Ok(())
	}

	async fn expand_mutation(
		_handle: &H,
		value: tg::Mutation,
		update_sender: NodeUpdateSender,
	) -> tg::Result<()> {
		let children: Vec<_> = match value {
			tg::Mutation::Append { values } => [
				("kind".to_owned(), tg::Value::String("append".to_owned())),
				("values".to_owned(), tg::Value::Array(values)),
			]
			.into_iter()
			.collect(),
			tg::Mutation::Prefix {
				separator,
				template,
			} => {
				let mut children = Vec::new();
				children.push(("kind".to_owned(), tg::Value::String("prefix".to_owned())));
				if let Some(separator) = separator {
					children.push(("separator".to_owned(), tg::Value::String(separator)));
				}
				children.push(("template".to_owned(), tg::Value::Template(template)));
				children
			},
			tg::Mutation::Prepend { values } => [
				("kind".to_owned(), tg::Value::String("append".to_owned())),
				("values".to_owned(), tg::Value::Array(values)),
			]
			.into_iter()
			.collect(),
			tg::Mutation::Set { value } => [
				("kind".to_owned(), tg::Value::String("set".to_owned())),
				("value".to_owned(), *value),
			]
			.into_iter()
			.collect(),
			tg::Mutation::SetIfUnset { value } => [
				("kind".to_owned(), tg::Value::String("set".to_owned())),
				("value".to_owned(), *value),
			]
			.into_iter()
			.collect(),
			tg::Mutation::Suffix {
				separator,
				template,
			} => {
				let mut children = Vec::new();
				children.push(("kind".to_owned(), tg::Value::String("prefix".to_owned())));
				if let Some(separator) = separator {
					children.push(("separator".to_owned(), tg::Value::String(separator)));
				}
				children.push(("template".to_owned(), tg::Value::Template(template)));
				children
			},
			tg::Mutation::Unset => [("kind".to_owned(), tg::Value::String("unset".to_owned()))]
				.into_iter()
				.collect(),
		};

		let update = |node: Rc<RefCell<Node>>| {
			for (name, child) in children {
				let item = Item::Value(child);
				let child = Self::create_node(&node, Some(name), Some(&item));
				node.borrow_mut().children.push(child);
			}
		};
		update_sender.send(Box::new(update)).unwrap();
		Ok(())
	}

	async fn expand_object(
		handle: &H,
		object: tg::Object,
		update_sender: NodeUpdateSender,
	) -> tg::Result<()> {
		match object {
			tg::Object::Leaf(leaf) => {
				Self::handle_leaf(handle, leaf, update_sender).await?;
			},
			tg::Object::Branch(branch) => {
				Self::expand_branch(handle, branch, update_sender).await?;
			},
			tg::Object::Directory(directory) => {
				Self::handle_directory(handle, directory, update_sender).await?;
			},
			tg::Object::File(file) => {
				Self::handle_file(handle, file, update_sender).await?;
			},
			tg::Object::Symlink(symlink) => {
				Self::handle_symlink(handle, symlink, update_sender).await?;
			},
			tg::Object::Graph(graph) => {
				Self::handle_graph(handle, graph, update_sender).await?;
			},
			tg::Object::Target(target) => {
				Self::handle_target(handle, target, update_sender).await?;
			},
		}
		Ok(())
	}

	async fn handle_symlink(
		handle: &H,
		symlink: tg::Symlink,
		update_sender: NodeUpdateSender,
	) -> tg::Result<()> {
		let object = symlink.object(handle).await?;
		let children = match object.as_ref() {
			tg::symlink::Object::Graph { graph, node } => [
				("graph".to_owned(), tg::Value::Object(graph.clone().into())),
				("node".to_owned(), tg::Value::Number(node.to_f64().unwrap())),
			]
			.into_iter()
			.collect(),
			tg::symlink::Object::Artifact { artifact, subpath } => {
				let mut children = Vec::new();
				children.push((
					"artifact".to_owned(),
					tg::Value::Object(artifact.clone().into()),
				));
				if let Some(subpath) = subpath {
					let subpath = subpath.to_string_lossy().to_string();
					children.push(("subpath".to_owned(), tg::Value::String(subpath)));
				}
				children
			},
			tg::symlink::Object::Target { target } => {
				let target = target.to_string_lossy().to_string();
				vec![("target".to_owned(), tg::Value::String(target))]
			},
		};
		symlink.unload();

		// Send the update.
		let update = |node: Rc<RefCell<Node>>| {
			for (name, child) in children {
				let item = Item::Value(child);
				let child = Self::create_node(&node, Some(name), Some(&item));
				node.borrow_mut().children.push(child);
			}
		};
		update_sender.send(Box::new(update)).unwrap();

		Ok(())
	}

	async fn handle_target(
		handle: &H,
		target: tg::Target,
		update_sender: NodeUpdateSender,
	) -> tg::Result<()> {
		let object = target.object(handle).await?;
		let mut children = Vec::new();
		children.push(("args".to_owned(), tg::Value::Array(object.args.clone())));
		if let Some(checksum) = &object.checksum {
			children.push((
				"checksum".to_owned(),
				tg::Value::String(checksum.to_string()),
			));
		}
		children.push(("env".to_owned(), tg::Value::Map(object.env.clone())));
		if let Some(executable) = &object.executable {
			let value = match executable {
				tg::target::Executable::Artifact(artifact) => {
					tg::Value::Object(artifact.clone().into())
				},
				tg::target::Executable::Module(module) => {
					let mut map = BTreeMap::new();
					map.insert(
						"kind".to_owned(),
						tg::Value::String(module.kind.to_string()),
					);

					let mut referent = BTreeMap::new();
					referent.insert(
						"item".to_owned(),
						tg::Value::Object(module.referent.item.clone()),
					);
					if let Some(path) = &module.referent.path {
						let path = path.to_string_lossy().to_string();
						referent.insert("path".to_owned(), tg::Value::String(path));
					}
					if let Some(subpath) = &module.referent.subpath {
						let subpath = subpath.to_string_lossy().to_string();
						referent.insert("subpath".to_owned(), tg::Value::String(subpath));
					}
					if let Some(tag) = &module.referent.tag {
						referent.insert("tag".to_owned(), tg::Value::String(tag.to_string()));
					}
					map.insert("referent".to_owned(), tg::Value::Map(referent));
					tg::Value::Map(map)
				},
			};
			children.push(("executable".to_owned(), value));
		}
		children.push(("host".to_owned(), tg::Value::String(object.host.clone())));
		target.unload();

		// Send the update.;
		let update = |node: Rc<RefCell<Node>>| {
			for (name, child) in children {
				let item = Item::Value(child);
				let child = Self::create_node(&node, Some(name), Some(&item));
				node.borrow_mut().children.push(child);
			}
		};
		update_sender.send(Box::new(update)).unwrap();
		Ok(())
	}

	async fn expand_template(
		_handle: &H,
		template: tg::Template,
		update_sender: NodeUpdateSender,
	) -> tg::Result<()> {
		let array = template
			.components
			.into_iter()
			.map(|component| {
				let mut map = BTreeMap::new();
				match component {
					tg::template::Component::Artifact(artifact) => {
						map.insert("kind".to_owned(), tg::Value::String("artifact".to_owned()));
						map.insert("value".to_owned(), tg::Value::Object(artifact.into()));
					},
					tg::template::Component::String(string) => {
						map.insert("kind".to_owned(), tg::Value::String("string".to_owned()));
						map.insert("value".to_owned(), tg::Value::String(string));
					},
				}
				tg::Value::Map(map)
			})
			.collect();
		let value = tg::Value::Array(array);
		let update = |node: Rc<RefCell<Node>>| {
			let item = Item::Value(value);
			let child = Self::create_node(&node, Some("components".to_owned()), Some(&item));
			node.borrow_mut().children.push(child);
		};
		update_sender.send(Box::new(update)).unwrap();
		Ok(())
	}

	async fn expand_value(
		handle: &H,
		value: tg::Value,
		update_sender: NodeUpdateSender,
	) -> tg::Result<()> {
		match value {
			tg::Value::Array(array) => {
				Self::expand_array(handle, array, update_sender).await?;
			},
			tg::Value::Map(map) => {
				Self::expand_map(handle, map, update_sender).await?;
			},
			tg::Value::Object(object) => {
				Self::expand_object(handle, object, update_sender).await?;
			},
			tg::Value::Mutation(mutation) => {
				Self::expand_mutation(handle, mutation, update_sender).await?;
			},
			tg::Value::Template(template) => {
				Self::expand_template(handle, template, update_sender).await?;
			},
			_ => (),
		}
		Ok(())
	}

	fn is_last_child(node: &Rc<RefCell<Node>>) -> bool {
		let Some(parent) = node
			.borrow()
			.parent
			.clone()
			.and_then(|parent| parent.upgrade())
		else {
			return true;
		};
		if Rc::ptr_eq(parent.borrow().children.last().unwrap(), node) {
			return true;
		}
		false
	}

	fn item_title(item: &Item) -> String {
		match item {
			Item::Build(build) => build.id().to_string(),
			Item::Value(value) => match value {
				tg::Value::Null => "null".to_owned(),
				tg::Value::Bool(bool) => {
					if *bool {
						"true".to_owned()
					} else {
						"false".to_owned()
					}
				},
				tg::Value::Number(number) => number.to_string(),
				tg::Value::String(string) => format!("\"{string}\""),
				tg::Value::Array(_) => "array".to_owned(),
				tg::Value::Map(_) => "map".to_owned(),
				tg::Value::Object(object) => match object {
					tg::Object::Leaf(leaf) => leaf
						.state()
						.read()
						.unwrap()
						.id
						.as_ref()
						.map_or_else(|| "object".to_owned(), ToString::to_string),
					tg::Object::Branch(branch) => branch
						.state()
						.read()
						.unwrap()
						.id
						.as_ref()
						.map_or_else(|| "object".to_owned(), ToString::to_string),
					tg::Object::Directory(directory) => directory
						.state()
						.read()
						.unwrap()
						.id
						.as_ref()
						.map_or_else(|| "object".to_owned(), ToString::to_string),
					tg::Object::File(file) => file
						.state()
						.read()
						.unwrap()
						.id
						.as_ref()
						.map_or_else(|| "object".to_owned(), ToString::to_string),
					tg::Object::Symlink(symlink) => symlink
						.state()
						.read()
						.unwrap()
						.id
						.as_ref()
						.map_or_else(|| "object".to_owned(), ToString::to_string),
					tg::Object::Graph(graph) => graph
						.state()
						.read()
						.unwrap()
						.id
						.as_ref()
						.map_or_else(|| "object".to_owned(), ToString::to_string),
					tg::Object::Target(target) => target
						.state()
						.read()
						.unwrap()
						.id
						.as_ref()
						.map_or_else(|| "object".to_owned(), ToString::to_string),
				},
				tg::Value::Bytes(_) => "bytes".to_owned(),
				tg::Value::Mutation(_) => "mutation".to_owned(),
				tg::Value::Template(_) => "template".to_owned(),
			},
		}
	}

	pub fn new(handle: &H, item: Item, options: Options) -> Self {
		let options = Rc::new(options);
		let (update_sender, update_receiver) = std::sync::mpsc::channel();
		let (ready_sender, _) = tokio::sync::watch::channel(false);
		let title = Self::item_title(&item);

		let update_task = if let Item::Build(build) = &item {
			// Create the update task.
			let update_task = Task::spawn_local({
				let build = build.clone();
				let handle = handle.clone();
				let options = options.clone();
				let update_sender = update_sender.clone();
				|_| async move {
					Self::build_update_task(&handle, build, options.as_ref(), update_sender)
						.await
						.ok();
				}
			});
			Some(update_task)
		} else {
			None
		};
		let root = Rc::new(RefCell::new(Node {
			children: Vec::new(),
			expand_task: None,
			expanded: false,
			indicator: None,
			item: Some(item),
			label: None,
			options: options.clone(),
			parent: None,
			ready_sender,
			title,
			update_receiver,
			update_sender,
			update_task,
		}));
		let roots = vec![root.clone()];
		let selected = root.clone();
		Self {
			handle: handle.clone(),
			rect: None,
			roots,
			scroll: 0,
			selected,
		}
	}

	fn nodes(&mut self) -> Vec<Rc<RefCell<Node>>> {
		let mut nodes = Vec::new();
		let mut stack = vec![self.roots.last().unwrap().clone()];
		while let Some(node) = stack.pop() {
			nodes.push(node.clone());
			stack.extend(node.borrow().children.iter().rev().cloned());
		}
		nodes
	}

	fn pop(&mut self) {
		if self.roots.len() > 1 {
			self.roots.pop();
		}
	}

	fn push(&mut self) {
		if !Rc::ptr_eq(self.roots.last().unwrap(), &self.selected) {
			self.roots.push(self.selected.clone());
		}
	}

	pub fn ready(&self) -> impl Future<Output = ()> + Send + 'static {
		let mut ready = self.roots.last().unwrap().borrow().ready_sender.subscribe();
		async move { ready.wait_for(|ready| *ready).map_ok(|_| ()).await.unwrap() }
	}

	pub fn render(&mut self, rect: Rect, buffer: &mut Buffer) {
		// Get the current time.
		let now = std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.unwrap()
			.as_millis();

		// Get the expanded nodes.
		let nodes = self.nodes();

		// Filter by the scroll and height.
		let nodes = nodes
			.into_iter()
			.skip(self.scroll)
			.take(rect.height.to_usize().unwrap());

		// Render the nodes.
		for (node, rect) in nodes.zip(rect.rows()) {
			let line = tui::text::Line::default();
			let style = if Rc::ptr_eq(&node, &self.selected) {
				Style::default().bg(Color::White).fg(Color::Black)
			} else {
				Style::default()
			};
			let mut line = line.style(style);

			let mut prefix = String::new();
			for ancestor in Self::ancestors(&node)
				.iter()
				.skip(1)
				.rev()
				.skip_while(|ancestor| !Rc::ptr_eq(ancestor, self.roots.last().unwrap()))
				.skip(1)
			{
				let is_last_child = Self::is_last_child(ancestor);
				let string = if is_last_child { "  " } else { "│ " };
				prefix.push_str(string);
			}
			if !Rc::ptr_eq(&node, self.roots.last().unwrap()) {
				let is_last_child = Self::is_last_child(&node);
				let string = if is_last_child { "└─" } else { "├─" };
				prefix.push_str(string);
			}
			line.push_span(prefix);

			let disclosure = if node.borrow().expanded { "▼" } else { "▶" };
			line.push_span(disclosure);
			line.push_span(" ");

			let indicator = match node.borrow().indicator {
				None => None,
				Some(Indicator::Created) => Some("⟳".yellow()),
				Some(Indicator::Dequeued) => Some("•".yellow()),
				Some(Indicator::Started) => {
					let position = (now / (1000 / 10)) % 10;
					let position = position.to_usize().unwrap();
					Some(SPINNER[position].to_string().blue())
				},
				Some(Indicator::Canceled) => Some("⦻".yellow()),
				Some(Indicator::Failed) => Some("✗".red()),
				Some(Indicator::Succeeded) => Some("✓".green()),
			};
			if let Some(indicator) = indicator {
				line.push_span(indicator);
				line.push_span(" ");
			}

			if let Some(label) = node.borrow().label.clone() {
				line.push_span(label);
				line.push_span(": ");
			}

			line.push_span(node.borrow().title.clone());

			tui::widgets::Paragraph::new(line).render(rect, buffer);
		}

		self.rect = Some(rect);
	}

	async fn expand_task(
		handle: &H,
		item: Item,
		update_sender: NodeUpdateSender,
		ready_sender: tokio::sync::watch::Sender<bool>,
	) {
		let _result = match item {
			Item::Build(build) => Self::expand_build(handle, build, update_sender).await,
			Item::Value(value) => Self::expand_value(handle, value, update_sender).await,
		};
		ready_sender.send_replace(true);
	}

	fn top(&mut self) {
		let nodes = self.nodes();
		self.selected = nodes.first().unwrap().clone();
		self.scroll = 0;
	}

	fn up(&mut self) {
		let nodes = self.nodes();
		let index = nodes
			.iter()
			.position(|node| Rc::ptr_eq(node, &self.selected))
			.unwrap();
		let index = index.saturating_sub(1);
		self.selected = nodes[index].clone();
		if index < self.scroll {
			self.scroll = self.scroll.saturating_sub(1);
		}
	}

	pub fn update(&mut self) {
		for node in self.nodes() {
			loop {
				let Ok(update) = node.borrow_mut().update_receiver.try_recv() else {
					break;
				};
				update(node.clone());
			}
		}
	}
}

impl Drop for Node {
	fn drop(&mut self) {
		if let Some(task) = self.expand_task.take() {
			task.abort();
		}
		if let Some(task) = self.update_task.take() {
			task.abort();
		}
	}
}

impl std::fmt::Display for Display {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		fn inner(
			this: &Display,
			f: &mut std::fmt::Formatter<'_>,
			prefix: &str,
		) -> std::fmt::Result {
			write!(f, "{}", this.title)?;
			for (n, child) in this.children.iter().enumerate() {
				write!(f, "\n{prefix}")?;
				if n < this.children.len() - 1 {
					write!(f, "├╴")?;
					inner(child, f, &format!("{prefix}│ "))?;
				} else {
					write!(f, "└╴")?;
					inner(child, f, &format!("{prefix}  "))?;
				}
			}
			Ok(())
		}
		inner(self, f, "")?;
		Ok(())
	}
}
