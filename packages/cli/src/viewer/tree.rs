use {
	super::{Item, Options, Package, data, log::Log},
	crossterm as ct,
	futures::{TryStreamExt as _, future, stream::FuturesUnordered},
	num::ToPrimitive as _,
	ratatui::{self as tui, prelude::*},
	std::{
		cell::RefCell,
		collections::{BTreeMap, HashSet},
		io::Write as _,
		pin::pin,
		rc::{Rc, Weak},
		sync::{
			Arc,
			atomic::{AtomicU32, Ordering},
		},
	},
	tangram_client::prelude::*,
	tangram_futures::task::Task,
};

const SPINNER: [char; 10] = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];

pub struct Tree<H> {
	handle: H,
	counter: UpdateCounter,
	data: data::UpdateSender,
	rect: Option<Rect>,
	roots: Vec<Rc<RefCell<Node>>>,
	selected: Rc<RefCell<Node>>,
	selected_task: Option<Task<()>>,
	scroll: usize,
	viewer: super::UpdateSender<H>,
}

struct Node {
	children: Vec<Rc<RefCell<Self>>>,
	counter: UpdateCounter,
	depth: u32,
	expand_task: Option<Task<()>>,
	expanded: Option<bool>,
	expanded_nodes: Rc<RefCell<HashSet<NodeID>>>,
	guard: Option<UpdateGuard>,
	indicator: Option<Indicator>,
	label: Option<String>,
	log_task: Option<Task<()>>,
	options: Rc<Options>,
	parent: Option<Weak<RefCell<Self>>>,
	referent: Option<tg::Referent<Item>>,
	title: String,
	update_receiver: NodeUpdateReceiver,
	update_sender: NodeUpdateSender,
	update_task: Option<Task<()>>,
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
enum NodeID {
	Object(tg::object::Id),
	Package(tg::object::Id),
	Process(tg::process::Id),
	Tag(String),
}

#[derive(Clone)]
struct UpdateCounter {
	num_pending: Arc<AtomicU32>,
	watch: tokio::sync::watch::Sender<()>,
}

struct UpdateGuard {
	num_pending: Arc<AtomicU32>,
	watch: tokio::sync::watch::Sender<()>,
}

impl UpdateCounter {
	fn new() -> Self {
		let watch = tokio::sync::watch::Sender::new(());
		Self {
			num_pending: Arc::new(AtomicU32::new(0)),
			watch,
		}
	}

	fn guard(&self) -> UpdateGuard {
		self.num_pending.fetch_add(1, Ordering::AcqRel);
		self.watch.send(()).ok();
		UpdateGuard {
			num_pending: self.num_pending.clone(),
			watch: self.watch.clone(),
		}
	}

	fn is_finished(&self) -> bool {
		self.num_pending.load(Ordering::Acquire) == 0
	}

	fn changed(&self) -> impl Future<Output = ()> + Send + Sync {
		let mut receiver = self.watch.subscribe();
		async move {
			receiver.changed().await.ok();
		}
	}
}

impl Drop for UpdateGuard {
	fn drop(&mut self) {
		self.num_pending.fetch_sub(1, Ordering::AcqRel);
		self.watch.send(()).ok();
	}
}

#[derive(Clone, Copy, Debug)]
pub enum Indicator {
	Created,
	Enqueued,
	Dequeued,
	Started,
	Canceled,
	Failed,
	Succeeded,
	Error,
}

type NodeUpdateSender = std::sync::mpsc::Sender<Box<dyn FnOnce(Rc<RefCell<Node>>)>>;

type NodeUpdateReceiver = std::sync::mpsc::Receiver<Box<dyn FnOnce(Rc<RefCell<Node>>)>>;

#[derive(Debug)]
pub struct Display {
	title: String,
	children: Vec<Self>,
}

impl<H> Tree<H>
where
	H: tg::Handle,
{
	pub fn changed(&self) -> impl Future<Output = ()> + Send + Sync {
		self.counter.changed()
	}

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
		self.select(nodes.last().unwrap().clone());
		let height = self.rect.as_ref().unwrap().height.to_usize().unwrap();
		self.scroll = nodes.len().saturating_sub(height);
	}

	fn collapse(&mut self) {
		if matches!(self.selected.borrow().expanded, Some(true)) {
			let mut node = self.selected.borrow_mut();
			node.children.clear();
			if let Some(task) = node.expand_task.take() {
				task.abort();
			}
			node.expanded.replace(false);
		} else {
			let parent = self
				.selected
				.borrow()
				.parent
				.as_ref()
				.map(|parent| parent.upgrade().unwrap());
			if let Some(parent) = parent {
				let index = self
					.nodes()
					.iter()
					.position(|n| Rc::ptr_eq(n, &parent))
					.unwrap();
				self.scroll = self.scroll.min(index);
				self.select(parent);
			}
		}
	}

	fn create_node(
		handle: &H,
		parent: &Rc<RefCell<Node>>,
		label: Option<String>,
		referent: Option<tg::Referent<Item>>,
	) -> Rc<RefCell<Node>>
	where
		H: tg::Handle,
	{
		let counter = parent.borrow().counter.clone();
		let depth = parent.borrow().depth + 1;
		let expanded_nodes = parent.borrow().expanded_nodes.clone();
		let (update_sender, update_receiver) = std::sync::mpsc::channel();
		let options = parent.borrow().options.clone();
		let parent = Rc::downgrade(parent);
		let title = referent.as_ref().map_or(String::new(), Self::item_title);
		let expand = match referent.as_ref().map(tg::Referent::item) {
			Some(Item::Package(package)) if options.expand_packages => expanded_nodes
				.borrow_mut()
				.insert(NodeID::Package(package.0.id())),
			Some(Item::Process(process)) if options.expand_processes => expanded_nodes
				.borrow_mut()
				.insert(NodeID::Process(process.id().clone())),
			Some(Item::Tag(pattern)) if options.expand_tags => expanded_nodes
				.borrow_mut()
				.insert(NodeID::Tag(pattern.to_string())),
			Some(Item::Value(tg::Value::Object(object))) => {
				if options.expand_objects {
					expanded_nodes
						.borrow_mut()
						.insert(NodeID::Object(object.id()))
				} else {
					false
				}
			},
			Some(Item::Value(_)) => options.expand_values,
			_ => false,
		};

		let expand_task = match (&referent, expand) {
			(Some(referent), true) => {
				let counter = counter.clone();
				let handle = handle.clone();
				let referent = referent.clone();
				let update_sender = update_sender.clone();
				let guard = counter.guard();
				let task = Task::spawn_local(|_| async move {
					Self::expand_task(&handle, counter.clone(), referent, update_sender).await;
					drop(guard);
				});
				Some(task)
			},
			_ => None,
		};

		let expanded = match referent.as_ref().map(|r| &r.item) {
			Some(
				Item::Process(_)
				| Item::Value(
					tg::Value::Array(_)
					| tg::Value::Map(_)
					| tg::Value::Mutation(_)
					| tg::Value::Object(_)
					| tg::Value::Template(_),
				),
			) => Some(expand),
			_ => None,
		};

		let guard = Some(counter.guard());
		Rc::new(RefCell::new(Node {
			children: Vec::new(),
			counter,
			depth,
			expand_task,
			expanded,
			expanded_nodes,
			guard,
			indicator: None,
			referent,
			label,
			log_task: None,
			options,
			parent: Some(parent),
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
		Self::display_node(self.roots.last().unwrap(), now).unwrap()
	}

	pub fn is_finished(&self) -> bool {
		self.counter.is_finished()
	}

	fn display_node(node: &Rc<RefCell<Node>>, now: u128) -> Option<Display> {
		// Clear the guard.
		node.borrow_mut().guard.take();

		// Recurse over the children.
		let children = node
			.borrow()
			.children
			.iter()
			.filter_map(|node| Self::display_node(node, now))
			.collect();

		// Display the node if we haven't reached the maximum depth.
		if node
			.borrow()
			.options
			.depth
			.is_none_or(|max_depth| node.borrow().depth <= max_depth)
		{
			let mut title = String::new();
			let indicator = match node.borrow().indicator {
				None => None,
				Some(Indicator::Created) => Some(crossterm::style::Stylize::blue('⟳')),
				Some(Indicator::Enqueued) => Some(crossterm::style::Stylize::yellow('⟳')),
				Some(Indicator::Dequeued) => Some(crossterm::style::Stylize::yellow('•')),
				Some(Indicator::Started) => {
					let position = (now / (1000 / 10)) % 10;
					let position = position.to_usize().unwrap();
					Some(crossterm::style::Stylize::blue(SPINNER[position]))
				},
				Some(Indicator::Canceled) => Some(crossterm::style::Stylize::yellow('⦻')),
				Some(Indicator::Failed) => Some(crossterm::style::Stylize::red('✗')),
				Some(Indicator::Succeeded) => Some(crossterm::style::Stylize::green('✓')),
				Some(Indicator::Error) => Some(crossterm::style::Stylize::red('?')),
			};
			if let Some(indicator) = indicator {
				title.push_str(&indicator.to_string());
				title.push(' ');
			}
			if let Some(label) = node.borrow().label.clone() {
				title.push_str(&label);
				title.push_str(": ");
			}
			title.push_str(&node.borrow().title);
			return Some(Display { title, children });
		}

		None
	}

	fn down(&mut self) {
		let nodes = self.nodes();
		let index = nodes
			.iter()
			.position(|node| Rc::ptr_eq(node, &self.selected))
			.unwrap();
		let index = (index + 1).min(nodes.len() - 1);
		self.select(nodes[index].clone());
		let height = self.rect.as_ref().unwrap().height.to_usize().unwrap();
		if index >= self.scroll + height {
			self.scroll += 1;
		}
	}

	pub(crate) fn expand(&mut self) {
		let mut node = self.selected.borrow_mut();
		let Some(referent) = node.referent.as_ref() else {
			return;
		};
		if matches!(node.expanded, Some(true) | None) {
			return;
		}
		let children_task = Task::spawn_local({
			let counter = self.counter.clone();
			let guard = counter.guard();
			let handle = self.handle.clone();
			let update_sender = node.update_sender.clone();
			let referent = referent.clone();
			move |_| async move {
				Self::expand_task(&handle, counter, referent, update_sender).await;
				drop(guard);
			}
		});
		node.expand_task.replace(children_task);
		node.expanded.replace(true);
	}

	async fn expand_array(
		handle: &H,
		array: tg::value::Array,
		update_sender: NodeUpdateSender,
		guard: UpdateGuard,
	) -> tg::Result<()> {
		let handle = handle.clone();
		let update = move |node: Rc<RefCell<Node>>| {
			for value in array {
				let item = tg::Referent::with_item(Item::Value(value));
				let child = Self::create_node(&handle, &node, None, Some(item));
				node.borrow_mut().children.push(child);
			}
			node.borrow_mut().guard.replace(guard);
		};
		update_sender.send(Box::new(update)).unwrap();
		Ok(())
	}

	async fn expand_blob(
		handle: &H,
		blob: tg::Blob,
		update_sender: NodeUpdateSender,
		guard: UpdateGuard,
	) -> tg::Result<()> {
		let children = match blob.load(handle).await?.as_ref() {
			tg::blob::Object::Leaf(_) => {
				return Ok(());
			},
			tg::blob::Object::Branch(branch) => branch
				.children
				.iter()
				.map(|child| child.blob.clone().into())
				.collect(),
		};
		blob.unload();
		let handle = handle.clone();
		let value = tg::Value::Array(children);
		let update = move |node: Rc<RefCell<Node>>| {
			let item = tg::Referent::with_item(Item::Value(value));
			let child = Self::create_node(&handle, &node, Some("children".to_owned()), Some(item));
			node.borrow_mut().children.push(child);
			node.borrow_mut().guard.replace(guard);
		};
		update_sender.send(Box::new(update)).unwrap();
		Ok(())
	}

	async fn expand_command(
		handle: &H,
		command: tg::Command,
		update_sender: NodeUpdateSender,
		guard: UpdateGuard,
	) -> tg::Result<()> {
		let object = command.object(handle).await?;
		let mut children = Vec::new();
		children.push(("args".to_owned(), tg::Value::Array(object.args.clone())));
		children.push(("env".to_owned(), tg::Value::Map(object.env.clone())));
		let value = match &object.executable {
			tg::command::Executable::Artifact(executable) => {
				let mut map = BTreeMap::new();
				map.insert(
					"artifact".to_owned(),
					tg::Value::Object(executable.artifact.clone().into()),
				);
				if let Some(path) = &executable.path {
					let path = path.to_string_lossy().to_string();
					map.insert("path".to_owned(), tg::Value::String(path));
				}
				tg::Value::Map(map)
			},
			tg::command::Executable::Module(executable) => {
				let mut map = BTreeMap::new();
				map.insert(
					"kind".to_owned(),
					tg::Value::String(executable.module.kind.to_string()),
				);
				let mut referent = BTreeMap::new();
				referent.insert(
					"item".to_owned(),
					match executable.module.referent.item.clone() {
						tg::module::Item::Edge(edge) => {
							let object = match edge {
								tg::graph::Edge::Pointer(pointer) => {
									pointer.get(handle).await?.into()
								},
								tg::graph::Edge::Object(object) => object,
							};
							tg::Value::Object(object)
						},
						tg::module::Item::Path(path) => {
							tg::Value::String(path.to_string_lossy().into_owned())
						},
					},
				);
				if let Some(path) = executable.module.referent.path() {
					let path = path.to_string_lossy().to_string();
					referent.insert("path".to_owned(), tg::Value::String(path));
				}
				if let Some(tag) = executable.module.referent.tag() {
					referent.insert("tag".to_owned(), tg::Value::String(tag.to_string()));
				}
				map.insert("referent".to_owned(), tg::Value::Map(referent));
				tg::Value::Map(map)
			},
			tg::command::Executable::Path(executable) => {
				tg::Value::String(executable.path.to_string_lossy().to_string())
			},
		};
		children.push(("executable".to_owned(), value));
		children.push(("host".to_owned(), tg::Value::String(object.host.clone())));
		let mut mounts = Vec::new();
		for mount in &object.mounts {
			let mut map = BTreeMap::new();
			map.insert(
				"source".to_owned(),
				tg::Value::Object(mount.source.clone().into()),
			);
			map.insert(
				"target".to_owned(),
				tg::Value::String(mount.target.to_string_lossy().to_string()),
			);
			mounts.push(tg::Value::Map(map));
		}
		children.push(("mounts".to_owned(), tg::Value::Array(mounts)));
		command.unload();

		// Send the update.
		let handle = handle.clone();
		let update = move |node: Rc<RefCell<Node>>| {
			for (name, child) in children {
				let item = tg::Referent::with_item(Item::Value(child));
				let child = Self::create_node(&handle, &node, Some(name), Some(item));
				node.borrow_mut().children.push(child);
			}
			node.borrow_mut().guard.replace(guard);
		};
		update_sender.send(Box::new(update)).unwrap();
		Ok(())
	}

	async fn expand_error(
		handle: &H,
		error: tg::Error,
		update_sender: NodeUpdateSender,
		guard: UpdateGuard,
	) -> tg::Result<()> {
		let object = error.object(handle).await?;
		let mut children = Vec::new();

		// Add message if present.
		if let Some(message) = &object.message {
			children.push(("message".to_owned(), tg::Value::String(message.clone())));
		}

		// Add code if present.
		if let Some(code) = &object.code {
			children.push(("code".to_owned(), tg::Value::String(code.to_string())));
		}

		// Add source error if present.
		if let Some(source) = &object.source {
			let source_error: tg::Error = match &source.item {
				tg::Either::Left(object) => tg::Error::with_object(object.as_ref().clone()),
				tg::Either::Right(handle) => handle.as_ref().clone(),
			};
			children.push(("source".to_owned(), tg::Value::Object(source_error.into())));
		}

		// Add location if present.
		if let Some(location) = &object.location {
			let mut map = BTreeMap::new();
			if let Some(symbol) = &location.symbol {
				map.insert("symbol".to_owned(), tg::Value::String(symbol.clone()));
			}
			match &location.file {
				tg::error::File::Internal(path) => {
					map.insert(
						"file".to_owned(),
						tg::Value::String(path.to_string_lossy().to_string()),
					);
				},
				tg::error::File::Module(module) => {
					let referent = &module.referent;
					let item = match referent.item.clone() {
						tg::module::Item::Edge(edge) => {
							let object = match edge {
								tg::graph::Edge::Pointer(pointer) => {
									pointer.get(handle).await?.into()
								},
								tg::graph::Edge::Object(object) => object,
							};
							tg::Value::Object(object)
						},
						tg::module::Item::Path(path) => {
							tg::Value::String(path.to_string_lossy().into_owned())
						},
					};
					let mut module_map = BTreeMap::new();
					module_map.insert(
						"kind".to_owned(),
						tg::Value::String(module.kind.to_string()),
					);
					module_map.insert("item".to_owned(), item);
					map.insert("file".to_owned(), tg::Value::Map(module_map));
				},
			}
			let range_str = format!(
				"{}:{}-{}:{}",
				location.range.start.line,
				location.range.start.character,
				location.range.end.line,
				location.range.end.character
			);
			map.insert("range".to_owned(), tg::Value::String(range_str));
			children.push(("location".to_owned(), tg::Value::Map(map)));
		}

		// Add stack if present.
		if let Some(stack) = &object.stack {
			let stack_array: Vec<tg::Value> = stack
				.iter()
				.map(|loc| {
					let mut map = BTreeMap::new();
					if let Some(symbol) = &loc.symbol {
						map.insert("symbol".to_owned(), tg::Value::String(symbol.clone()));
					}
					match &loc.file {
						tg::error::File::Internal(path) => {
							map.insert(
								"file".to_owned(),
								tg::Value::String(path.to_string_lossy().to_string()),
							);
						},
						tg::error::File::Module(module) => {
							map.insert(
								"file".to_owned(),
								tg::Value::String(format!("{}", module.kind)),
							);
						},
					}
					let range_str = format!(
						"{}:{}-{}:{}",
						loc.range.start.line,
						loc.range.start.character,
						loc.range.end.line,
						loc.range.end.character
					);
					map.insert("range".to_owned(), tg::Value::String(range_str));
					tg::Value::Map(map)
				})
				.collect();
			children.push(("stack".to_owned(), tg::Value::Array(stack_array)));
		}

		// Add values if non-empty.
		if !object.values.is_empty() {
			let values_map: BTreeMap<String, tg::Value> = object
				.values
				.iter()
				.map(|(k, v)| (k.clone(), tg::Value::String(v.clone())))
				.collect();
			children.push(("values".to_owned(), tg::Value::Map(values_map)));
		}

		// Add diagnostics if present.
		if let Some(diagnostics) = &object.diagnostics {
			let diagnostics_array: Vec<tg::Value> = diagnostics
				.iter()
				.map(|diag| {
					let mut map = BTreeMap::new();
					if let Some(location) = &diag.location {
						let mut loc_map = BTreeMap::new();
						match &location.module.referent.item {
							tg::module::Item::Edge(_) => {
								loc_map.insert(
									"module".to_owned(),
									tg::Value::String("(graph)".to_owned()),
								);
							},
							tg::module::Item::Path(path) => {
								loc_map.insert(
									"module".to_owned(),
									tg::Value::String(path.to_string_lossy().into_owned()),
								);
							},
						}
						let range_str = format!(
							"{}:{}-{}:{}",
							location.range.start.line,
							location.range.start.character,
							location.range.end.line,
							location.range.end.character
						);
						loc_map.insert("range".to_owned(), tg::Value::String(range_str));
						map.insert("location".to_owned(), tg::Value::Map(loc_map));
					}
					map.insert(
						"severity".to_owned(),
						tg::Value::String(diag.severity.to_string()),
					);
					map.insert(
						"message".to_owned(),
						tg::Value::String(diag.message.clone()),
					);
					tg::Value::Map(map)
				})
				.collect();
			children.push((
				"diagnostics".to_owned(),
				tg::Value::Array(diagnostics_array),
			));
		}

		error.unload();

		// Send the update.
		let handle = handle.clone();
		let update = move |node: Rc<RefCell<Node>>| {
			for (name, child) in children {
				let item = tg::Referent::with_item(Item::Value(child));
				let child = Self::create_node(&handle, &node, Some(name), Some(item));
				node.borrow_mut().children.push(child);
			}
			node.borrow_mut().guard.replace(guard);
		};
		update_sender.send(Box::new(update)).unwrap();
		Ok(())
	}

	async fn expand_directory(
		handle: &H,
		directory: tg::Directory,
		update_sender: NodeUpdateSender,
		guard: UpdateGuard,
	) -> tg::Result<()> {
		let object = directory.object(handle).await?;
		let children: Vec<_> = match object.as_ref() {
			tg::directory::Object::Pointer(pointer) => [
				(
					"graph".to_owned(),
					tg::Value::Object(pointer.graph.clone().unwrap().into()),
				),
				(
					"index".to_owned(),
					tg::Value::Number(pointer.index.to_f64().unwrap()),
				),
				(
					"kind".to_owned(),
					tg::Value::String(pointer.kind.to_string()),
				),
			]
			.into_iter()
			.collect(),
			tg::directory::Object::Node(node) => {
				let entries = node
					.entries
					.clone()
					.into_iter()
					.map(async |(name, artifact)| {
						let artifact = match artifact {
							tg::graph::Edge::Pointer(pointer) => pointer.get(handle).await?,
							tg::graph::Edge::Object(artifact) => artifact,
						};
						Ok::<_, tg::Error>((name, artifact.into()))
					})
					.collect::<FuturesUnordered<_>>()
					.try_collect()
					.await?;
				[("entries".to_owned(), tg::Value::Map(entries))]
					.into_iter()
					.collect()
			},
		};
		directory.unload();

		// Send the update.
		let handle = handle.clone();
		let update = move |node: Rc<RefCell<Node>>| {
			for (name, child) in children {
				let item = tg::Referent::with_item(Item::Value(child));
				let child = Self::create_node(&handle, &node, Some(name), Some(item));
				node.borrow_mut().children.push(child);
			}
			node.borrow_mut().guard.replace(guard);
		};
		update_sender.send(Box::new(update)).unwrap();
		Ok(())
	}

	async fn expand_file(
		handle: &H,
		file: tg::File,
		update_sender: NodeUpdateSender,
		guard: UpdateGuard,
	) -> tg::Result<()> {
		let object = file.object(handle).await?;

		let children = match object.as_ref() {
			tg::file::Object::Pointer(pointer) => [
				(
					"graph".to_owned(),
					tg::Value::Object(pointer.graph.clone().unwrap().into()),
				),
				(
					"index".to_owned(),
					tg::Value::Number(pointer.index.to_f64().unwrap()),
				),
				(
					"kind".to_owned(),
					tg::Value::String(pointer.kind.to_string()),
				),
			]
			.into_iter()
			.collect(),
			tg::file::Object::Node(node) => {
				let mut children = Vec::with_capacity(3);
				children.push((
					"contents".to_owned(),
					tg::Value::Object(node.contents.clone().into()),
				));
				if node.executable {
					children.push(("executable".to_owned(), tg::Value::Bool(node.executable)));
				}
				let dependencies: BTreeMap<String, tg::Value> = node
					.dependencies
					.clone()
					.into_iter()
					.map(async |(reference, option)| {
						let mut map = BTreeMap::new();
						let Some(dependency) = option else {
							return Ok::<_, tg::Error>((reference.to_string(), tg::Value::Null));
						};
						if let Some(edge) = dependency.0.item() {
							let item = match edge {
								tg::graph::Edge::Pointer(pointer) => {
									pointer.get(handle).await?.into()
								},
								tg::graph::Edge::Object(object) => object.clone(),
							};
							map.insert("item".into(), tg::Value::Object(item));
						} else {
							map.insert("item".into(), tg::Value::Null);
						}
						if let Some(artifact) = &dependency.0.options.artifact {
							map.insert(
								"artifact".to_owned(),
								tg::Value::String(artifact.to_string()),
							);
						}
						if let Some(id) = &dependency.0.options.id {
							map.insert("id".to_owned(), tg::Value::String(id.to_string()));
						}
						if let Some(name) = &dependency.0.options.name {
							map.insert("name".to_owned(), tg::Value::String(name.clone()));
						}
						if let Some(path) = &dependency.0.options.path {
							map.insert(
								"path".to_owned(),
								tg::Value::String(path.to_string_lossy().to_string()),
							);
						}
						if let Some(tag) = &dependency.0.options.tag {
							map.insert("tag".to_owned(), tg::Value::String(tag.to_string()));
						}
						Ok::<_, tg::Error>((reference.to_string(), tg::Value::Map(map)))
					})
					.collect::<FuturesUnordered<_>>()
					.try_collect()
					.await?;
				if !dependencies.is_empty() {
					children.push(("dependencies".to_owned(), tg::Value::Map(dependencies)));
				}
				children
			},
		};
		file.unload();

		// Send the update.
		let handle = handle.clone();
		let update = move |node: Rc<RefCell<Node>>| {
			node.borrow_mut().guard.replace(guard);
			for (name, child) in children {
				let item = tg::Referent::with_item(Item::Value(child));
				let child = Self::create_node(&handle, &node, Some(name), Some(item));
				node.borrow_mut().children.push(child);
			}
		};
		update_sender.send(Box::new(update)).unwrap();
		Ok(())
	}

	async fn expand_graph(
		handle: &H,
		graph: tg::Graph,
		update_sender: NodeUpdateSender,
		guard: UpdateGuard,
	) -> tg::Result<()> {
		// Get the graph nodes and unload the object immediately.
		let nodes = graph.nodes(handle).await?;
		graph.unload();

		// Convert nodes to tg::Value::Maps
		let nodes = nodes
			.into_iter()
			.map(async |node| {
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
							.map(async |(name, edge)| {
								let value = match edge {
									tg::graph::Edge::Pointer(mut pointer) => {
										if pointer.graph.is_none() {
											pointer.graph.replace(graph.clone());
										}
										pointer.get(handle).await?.into()
									},
									tg::graph::Edge::Object(artifact) => artifact.into(),
								};
								Ok::<_, tg::Error>((name, tg::Value::Object(value)))
							})
							.collect::<FuturesUnordered<_>>()
							.try_collect()
							.await?;
						map.insert("entries".into(), tg::Value::Map(entries));
					},
					tg::graph::Node::File(file) => {
						map.insert("contents".into(), tg::Value::Object(file.contents.into()));
						if file.executable {
							map.insert("executable".into(), file.executable.into());
						}
						let dependencies: BTreeMap<String, tg::Value> = file
							.dependencies
							.into_iter()
							.map(async |(reference, option)| {
								let Some(dependency) = option else {
									return Ok::<_, tg::Error>((
										reference.to_string(),
										tg::Value::Null,
									));
								};
								let mut map = BTreeMap::new();
								if let Some(edge) = dependency.0.item() {
									let item = match edge {
										tg::graph::Edge::Pointer(pointer) => {
											let mut pointer = pointer.clone();
											if pointer.graph.is_none() {
												pointer.graph.replace(graph.clone());
											}
											pointer.get(handle).await?.into()
										},
										tg::graph::Edge::Object(object) => object.clone(),
									};
									map.insert("item".into(), tg::Value::Object(item));
								} else {
									map.insert("item".into(), tg::Value::Null);
								}
								if let Some(artifact) = &dependency.0.options.artifact {
									map.insert(
										"artifact".to_owned(),
										tg::Value::String(artifact.to_string()),
									);
								}
								if let Some(id) = &dependency.0.options.id {
									map.insert("id".to_owned(), tg::Value::String(id.to_string()));
								}
								if let Some(name) = &dependency.0.options.name {
									map.insert("name".to_owned(), tg::Value::String(name.clone()));
								}
								if let Some(path) = &dependency.0.options.path {
									map.insert(
										"path".to_owned(),
										tg::Value::String(path.to_string_lossy().to_string()),
									);
								}
								if let Some(tag) = &dependency.0.options.tag {
									map.insert(
										"tag".to_owned(),
										tg::Value::String(tag.to_string()),
									);
								}
								Ok::<_, tg::Error>((reference.to_string(), tg::Value::Map(map)))
							})
							.collect::<FuturesUnordered<_>>()
							.try_collect()
							.await?;
						if !dependencies.is_empty() {
							map.insert("dependencies".into(), tg::Value::Map(dependencies));
						}
					},
					tg::graph::Node::Symlink(symlink) => {
						if let Some(artifact) = symlink.artifact {
							let artifact = match artifact {
								tg::graph::Edge::Pointer(mut pointer) => {
									if pointer.graph.is_none() {
										pointer.graph.replace(graph.clone());
									}
									pointer.get(handle).await?.into()
								},
								tg::graph::Edge::Object(object) => object.into(),
							};
							map.insert("artifact".to_owned(), tg::Value::Object(artifact));
						}
						if let Some(path) = symlink.path {
							let path = path.to_string_lossy().to_string();
							map.insert("path".into(), tg::Value::String(path));
						}
					},
				}
				Ok::<_, tg::Error>(tg::Value::Map(map))
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect()
			.await?;

		// Convert to a value and send the update.
		let handle = handle.clone();
		let value = tg::Value::Array(nodes);
		let update = move |node: Rc<RefCell<Node>>| {
			node.borrow_mut().guard.replace(guard);
			let item = tg::Referent::with_item(Item::Value(value));
			let child = Self::create_node(&handle, &node, Some("nodes".to_owned()), Some(item));
			node.borrow_mut().children.push(child);
		};
		update_sender.send(Box::new(update)).unwrap();
		Ok(())
	}

	async fn expand_map(
		handle: &H,
		map: tg::value::Map,
		update_sender: NodeUpdateSender,
		guard: UpdateGuard,
	) -> tg::Result<()> {
		let handle = handle.clone();
		let update = move |node: Rc<RefCell<Node>>| {
			node.borrow_mut().guard.replace(guard);
			for (name, value) in map {
				let item = tg::Referent::with_item(Item::Value(value));
				let child = Self::create_node(&handle, &node, Some(name), Some(item));
				node.borrow_mut().children.push(child);
			}
		};
		update_sender.send(Box::new(update)).unwrap();
		Ok(())
	}

	async fn expand_mutation(
		handle: &H,
		value: tg::Mutation,
		update_sender: NodeUpdateSender,
		guard: UpdateGuard,
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
				("kind".to_owned(), tg::Value::String("prepend".to_owned())),
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
				(
					"kind".to_owned(),
					tg::Value::String("set_if_unset".to_owned()),
				),
				("value".to_owned(), *value),
			]
			.into_iter()
			.collect(),
			tg::Mutation::Suffix {
				separator,
				template,
			} => {
				let mut children = Vec::new();
				children.push(("kind".to_owned(), tg::Value::String("suffix".to_owned())));
				if let Some(separator) = separator {
					children.push(("separator".to_owned(), tg::Value::String(separator)));
				}
				children.push(("template".to_owned(), tg::Value::Template(template)));
				children
			},
			tg::Mutation::Merge { value } => [
				("kind".to_owned(), tg::Value::String("merge".to_owned())),
				("value".to_owned(), tg::Value::Map(value)),
			]
			.into_iter()
			.collect(),
			tg::Mutation::Unset => [("kind".to_owned(), tg::Value::String("unset".to_owned()))]
				.into_iter()
				.collect(),
		};

		let handle = handle.clone();
		let update = move |node: Rc<RefCell<Node>>| {
			node.borrow_mut().guard.replace(guard);
			for (name, child) in children {
				let item = tg::Referent::with_item(Item::Value(child));
				let child = Self::create_node(&handle, &node, Some(name), Some(item));
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
		guard: UpdateGuard,
	) -> tg::Result<()> {
		match object {
			tg::Object::Blob(blob) => {
				Self::expand_blob(handle, blob, update_sender, guard).await?;
			},
			tg::Object::Directory(directory) => {
				Self::expand_directory(handle, directory, update_sender, guard).await?;
			},
			tg::Object::File(file) => {
				Self::expand_file(handle, file, update_sender, guard).await?;
			},
			tg::Object::Symlink(symlink) => {
				Self::expand_symlink(handle, symlink, update_sender, guard).await?;
			},
			tg::Object::Graph(graph) => {
				Self::expand_graph(handle, graph, update_sender, guard).await?;
			},
			tg::Object::Command(command) => {
				Self::expand_command(handle, command, update_sender, guard).await?;
			},
			tg::Object::Error(error) => {
				Self::expand_error(handle, error, update_sender, guard).await?;
			},
		}
		Ok(())
	}

	async fn expand_package(
		handle: &H,
		counter: UpdateCounter,
		package: tg::Referent<tg::Object>,
		update_sender: NodeUpdateSender,
	) -> tg::Result<()> {
		let mut visitor = PackageVisitor {
			dependencies: BTreeMap::new(),
		};
		tg::object::visit(handle, &mut visitor, &package).await?;
		let dependencies = visitor
			.dependencies
			.into_iter()
			.map(|(reference, option)| {
				let item = option.and_then(|dependency| {
					dependency.0.item.map(|object| tg::Referent {
						item: Item::Package(Package(object)),
						options: dependency.0.options,
					})
				});
				(reference.to_string(), item)
			})
			.collect::<Vec<_>>();
		let guard = counter.guard();
		let handle = handle.clone();
		let update = move |node: Rc<RefCell<Node>>| {
			node.borrow_mut().guard.replace(guard);

			// Add the dependencies.
			for (label, item) in dependencies {
				let child = if let Some(item) = item {
					let label = Self::item_label(&item);
					Self::create_node(&handle, &node, label, Some(item))
				} else {
					Self::create_node(
						&handle,
						&node,
						Some(label),
						Some(tg::Referent::with_item(Item::Value(tg::Value::Null))),
					)
				};
				node.borrow_mut().children.push(child);
			}
		};
		update_sender.send(Box::new(update)).unwrap();

		Ok(())
	}

	async fn expand_tag(
		handle: &H,
		counter: UpdateCounter,
		pattern: &tg::tag::Pattern,
		update_sender: NodeUpdateSender,
	) -> tg::Result<()> {
		// List the tag.
		let output = handle
			.list_tags(tg::tag::list::Arg {
				length: None,
				local: None,
				pattern: tg::tag::Pattern::new(pattern.to_string()),
				recursive: false,
				remotes: None,
				reverse: false,
			})
			.await
			.map_err(|source| tg::error!(!source, tag = %pattern, "failed to list tags"))?;

		// Get the children of this tag.
		let children = output
			.data
			.into_iter()
			.map(move |output| {
				let options = tg::referent::Options {
					tag: Some(output.tag.clone()),
					..tg::referent::Options::default()
				};
				let item = match output.item {
					Some(tg::Either::Left(object)) => {
						Item::Value(tg::Value::Object(tg::Object::with_id(object)))
					},
					Some(tg::Either::Right(process)) => Item::Process(tg::Process::new(
						process,
						None,
						output.remote.clone(),
						None,
						None,
					)),
					None => Item::Tag(tg::tag::Pattern::new(output.tag.to_string())),
				};
				if let Item::Tag(_) = item {
					(None, tg::Referent::with_item(item))
				} else {
					(Some(output.tag.to_string()), tg::Referent { item, options })
				}
			})
			.collect::<Vec<_>>();
		let guard = counter.guard();
		let handle = handle.clone();
		let update = move |node: Rc<RefCell<Node>>| {
			let children = children
				.into_iter()
				.map(|(label, referent)| Self::create_node(&handle, &node, label, Some(referent)))
				.collect();
			node.borrow_mut().children = children;
			node.borrow_mut().guard.replace(guard);
		};
		update_sender.send(Box::new(update)).unwrap();
		Ok(())
	}

	async fn expand_process(
		handle: &H,
		counter: UpdateCounter,
		referent: tg::Referent<tg::Process>,
		update_sender: NodeUpdateSender,
	) -> tg::Result<()> {
		let process = referent.item.clone();

		// Create the log task.
		let log_task = Task::spawn_local({
			let process = process.clone();
			let handle = handle.clone();
			let guard = counter.guard();
			let update_sender = update_sender.clone();
			|_| async move {
				Self::process_log_task(&handle, process, update_sender)
					.await
					.ok();
				drop(guard);
			}
		});
		let update = move |node: Rc<RefCell<Node>>| {
			node.borrow_mut().log_task.replace(log_task);
		};
		update_sender.send(Box::new(update)).unwrap();

		let command = process.command(handle).await?;
		let value = tg::Value::Object(command.clone().into());
		update_sender
			.send({
				let handle = handle.clone();
				let guard = counter.guard();
				Box::new(move |node| {
					node.borrow_mut().guard.replace(guard);
					if node.borrow().options.show_process_commands {
						let child = Self::create_node(
							&handle,
							&node,
							Some("command".to_owned()),
							Some(tg::Referent::with_item(Item::Value(value))),
						);
						node.borrow_mut().children.push(child);
					}
				})
			})
			.unwrap();

		// Get the output.
		let output = handle
			.get_process(process.id())
			.await?
			.data
			.output
			.map(tg::Value::try_from)
			.transpose()?;
		if let Some(output) = output {
			let handle = handle.clone();
			let update = move |node: Rc<RefCell<Node>>| {
				let output = Self::create_node(
					&handle,
					&node,
					Some("output".into()),
					Some(tg::Referent::with_item(Item::Value(output))),
				);
				node.borrow_mut().children.push(output);
			};
			update_sender.send(Box::new(update)).unwrap();
		}

		// Create the children stream.
		let mut children = process
			.children(handle, tg::process::children::get::Arg::default())
			.await?;
		while let Some(mut child) = children.try_next().await? {
			// Inherit from the referent.
			child.inherit(&referent);

			// Check the status of the process.
			let finished = child
				.item
				.status(handle)
				.await?
				.try_next()
				.await?
				.is_none_or(|status| matches!(status, tg::process::Status::Finished));

			// Post the update.
			let handle = handle.clone();
			let guard = counter.guard();
			let update = move |node: Rc<RefCell<Node>>| {
				if node.borrow().options.collapse_process_children && finished {
					return;
				}
				let item = child.clone().map(Item::Process);
				let child_node = Self::create_node(&handle, &node, None, Some(item));

				// Create the update task.
				let update_task = Task::spawn_local({
					node.borrow_mut().guard.replace(guard);
					let counter = child_node.borrow().counter.clone();
					let guard = counter.guard();
					let options = child_node.borrow().options.clone();
					let update_sender = child_node.borrow().update_sender.clone();
					|_| async move {
						Self::process_update_task(
							&handle,
							counter,
							&child,
							options.as_ref(),
							update_sender,
						)
						.await
						.ok();
						drop(guard);
					}
				});
				child_node.borrow_mut().update_task.replace(update_task);

				// Add the child to the children node.
				node.borrow_mut().children.push(child_node);
			};
			update_sender.send(Box::new(update)).unwrap();
		}

		// Remove the log.
		update_sender
			.send(Box::new(|node| {
				let mut node = node.borrow_mut();
				if let Some(task) = node.log_task.take() {
					task.abort();
				}
				let Some(position) = node
					.children
					.iter()
					.position(|child| child.borrow().label.as_deref() == Some("log"))
				else {
					return;
				};
				node.children.remove(position);
			}))
			.unwrap();

		Ok(())
	}

	async fn expand_symlink(
		handle: &H,
		symlink: tg::Symlink,
		update_sender: NodeUpdateSender,
		guard: UpdateGuard,
	) -> tg::Result<()> {
		let object = symlink.object(handle).await?;
		let children = match object.as_ref() {
			tg::symlink::Object::Pointer(pointer) => [
				(
					"graph".to_owned(),
					tg::Value::Object(pointer.graph.clone().unwrap().into()),
				),
				(
					"index".to_owned(),
					tg::Value::Number(pointer.index.to_f64().unwrap()),
				),
				(
					"kind".to_owned(),
					tg::Value::String(pointer.kind.to_string()),
				),
			]
			.into_iter()
			.collect(),
			tg::symlink::Object::Node(node) => {
				let mut children = Vec::new();
				if let Some(artifact) = &node.artifact {
					let artifact = match artifact {
						tg::graph::Edge::Pointer(pointer) => pointer.get(handle).await?.into(),
						tg::graph::Edge::Object(artifact) => artifact.clone().into(),
					};
					children.push(("artifact".to_owned(), tg::Value::Object(artifact)));
				}
				if let Some(path) = &node.path {
					let path = path.to_string_lossy().to_string();
					children.push(("path".to_owned(), tg::Value::String(path)));
				}
				children
			},
		};
		symlink.unload();

		// Send the update.
		let handle = handle.clone();
		let update = move |node: Rc<RefCell<Node>>| {
			node.borrow_mut().guard.replace(guard);
			for (name, child) in children {
				let item = tg::Referent::with_item(Item::Value(child));
				let child = Self::create_node(&handle, &node, Some(name), Some(item));
				node.borrow_mut().children.push(child);
			}
		};
		update_sender.send(Box::new(update)).unwrap();

		Ok(())
	}

	async fn expand_task(
		handle: &H,
		counter: UpdateCounter,
		referent: tg::Referent<Item>,
		update_sender: NodeUpdateSender,
	) {
		let result = match referent.item() {
			Item::Process(process) => {
				let referent = referent.clone().map(|_| process.clone());
				Self::expand_process(handle, counter.clone(), referent, update_sender.clone()).await
			},
			Item::Value(value) => {
				Self::expand_value(
					handle,
					counter.clone(),
					value.clone(),
					update_sender.clone(),
				)
				.await
			},
			Item::Package(package) => {
				let referent = referent.clone().map(|_| package.0.clone());
				Self::expand_package(handle, counter.clone(), referent, update_sender.clone()).await
			},
			Item::Tag(tag) => {
				Self::expand_tag(handle, counter.clone(), tag, update_sender.clone()).await
			},
		};
		if let Err(error) = result {
			let guard = counter.guard();
			let update = move |node: Rc<RefCell<Node>>| {
				node.borrow_mut().indicator.replace(Indicator::Error);
				node.borrow_mut().title = error.to_string();
				node.borrow_mut().guard.replace(guard);
			};
			update_sender.send(Box::new(update)).ok();
		}
	}

	async fn expand_template(
		handle: &H,
		template: tg::Template,
		update_sender: NodeUpdateSender,
		guard: UpdateGuard,
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
					tg::template::Component::Placeholder(placeholder) => {
						map.insert(
							"kind".to_owned(),
							tg::Value::String("placeholder".to_owned()),
						);
						map.insert("value".to_owned(), tg::Value::Placeholder(placeholder));
					},
				}
				tg::Value::Map(map)
			})
			.collect();
		let value = tg::Value::Array(array);
		let handle = handle.clone();
		let update = move |node: Rc<RefCell<Node>>| {
			node.borrow_mut().guard.replace(guard);
			let item = tg::Referent::with_item(Item::Value(value));
			let child =
				Self::create_node(&handle, &node, Some("components".to_owned()), Some(item));
			node.borrow_mut().children.push(child);
		};
		update_sender.send(Box::new(update)).unwrap();
		Ok(())
	}

	async fn expand_value(
		handle: &H,
		counter: UpdateCounter,
		value: tg::Value,
		update_sender: NodeUpdateSender,
	) -> tg::Result<()> {
		let guard = counter.guard();
		match value {
			tg::Value::Array(array) => {
				Self::expand_array(handle, array, update_sender, guard).await?;
			},
			tg::Value::Map(map) => {
				Self::expand_map(handle, map, update_sender, guard).await?;
			},
			tg::Value::Object(object) => {
				Self::expand_object(handle, object, update_sender, guard).await?;
			},
			tg::Value::Mutation(mutation) => {
				Self::expand_mutation(handle, mutation, update_sender, guard).await?;
			},
			tg::Value::Template(template) => {
				Self::expand_template(handle, template, update_sender, guard).await?;
			},
			_ => (),
		}
		Ok(())
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
				(ct::event::KeyCode::Char('y'), ct::event::KeyModifiers::NONE) => {
					self.yank();
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

	fn item_label(referent: &tg::Referent<Item>) -> Option<String> {
		if let Some(tag) = referent.tag() {
			return Some(tag.to_string());
		}
		if let Some(id) = referent.id() {
			return Some(id.to_string());
		}
		if let Some(path) = referent.path() {
			return Some(path.display().to_string());
		}
		None
	}

	fn item_title(referent: &tg::Referent<Item>) -> String {
		match referent.item() {
			Item::Package(package) => package.0.id().to_string(),
			Item::Tag(pattern) => pattern.to_string(),
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
				tg::Value::Object(object) => Self::object_id(object),
				tg::Value::Bytes(_) => "bytes".to_owned(),
				tg::Value::Mutation(_) => "mutation".to_owned(),
				tg::Value::Template(_) => "template".to_owned(),
				tg::Value::Placeholder(placeholder) => {
					format!("placeholder(\"{}\")", placeholder.name)
				},
			},
			Item::Process(_) => String::new(),
		}
	}

	pub fn new(
		handle: &H,
		referent: tg::Referent<Item>,
		options: Options,
		data: data::UpdateSender,
		viewer: super::UpdateSender<H>,
	) -> Self {
		let counter = UpdateCounter::new();
		let expanded_nodes = Rc::new(RefCell::new(HashSet::new()));
		let options = Rc::new(options);
		let (update_sender, update_receiver) = std::sync::mpsc::channel();
		let label = Self::item_label(&referent);
		let title = Self::item_title(&referent);
		let expand = match referent.item() {
			Item::Package(package) if options.expand_packages => expanded_nodes
				.borrow_mut()
				.insert(NodeID::Package(package.0.id())),
			Item::Process(process) if options.expand_processes => expanded_nodes
				.borrow_mut()
				.insert(NodeID::Process(process.id().clone())),
			Item::Tag(pattern) if options.expand_tags => expanded_nodes
				.borrow_mut()
				.insert(NodeID::Tag(pattern.to_string())),
			Item::Value(tg::Value::Object(object)) if options.expand_objects => expanded_nodes
				.borrow_mut()
				.insert(NodeID::Object(object.id())),
			Item::Value(_) => true,
			_ => false,
		};

		let expand_task = if expand {
			let counter = counter.clone();
			let guard = counter.guard();
			let handle = handle.clone();
			let referent = referent.clone();
			let update_sender = update_sender.clone();
			let task: Task<()> = Task::spawn_local(|_| async move {
				Self::expand_task(&handle, counter, referent, update_sender).await;
				drop(guard);
			});
			Some(task)
		} else {
			None
		};

		let update_task = if let Item::Process(process) = referent.item() {
			// Create the update task.
			let update_task = Task::spawn_local({
				let counter = counter.clone();
				let guard = counter.guard();
				let process = referent.clone().map(|_| process.clone());
				let handle = handle.clone();
				let options = options.clone();
				let update_sender = update_sender.clone();
				|_| async move {
					Self::process_update_task(
						&handle,
						counter,
						&process,
						options.as_ref(),
						update_sender,
					)
					.await
					.ok();
					drop(guard);
				}
			});
			Some(update_task)
		} else {
			None
		};
		let displayed_at_least_once = Some(counter.guard());

		let root = Rc::new(RefCell::new(Node {
			counter: counter.clone(),
			children: Vec::new(),
			depth: 1,
			expanded: Some(expand),
			expanded_nodes,
			expand_task,
			guard: displayed_at_least_once,
			indicator: None,
			referent: Some(referent),
			label,
			log_task: None,
			options: options.clone(),
			parent: None,
			title,
			update_receiver,
			update_sender,
			update_task,
		}));
		let roots = vec![root.clone()];

		Self {
			handle: handle.clone(),
			counter,
			data,
			rect: None,
			roots,
			scroll: 0,
			selected: root.clone(),
			selected_task: None,
			viewer,
		}
	}

	pub fn ensure_root_selected(&mut self) {
		let root = self.roots.first().unwrap().clone();
		self.select(root);
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

	fn object_id(object: &tg::Object) -> String {
		object
			.state()
			.try_get_id()
			.map_or_else(|| "object".to_owned(), |id| id.to_string())
	}

	fn pop(&mut self) {
		if self.roots.len() > 1 {
			self.roots.pop();
		}
	}

	async fn process_log_task(
		handle: &H,
		process: tg::Process,
		update_sender: NodeUpdateSender,
	) -> tg::Result<()> {
		let mut log = process
			.log(handle, tg::process::log::get::Arg::default())
			.await?;
		while let Some(chunk) = log.try_next().await? {
			let chunk = String::from_utf8_lossy(&chunk.bytes);
			for line in chunk.lines() {
				let line = line.to_owned();
				let handle = handle.clone();
				let update = move |node: Rc<RefCell<Node>>| {
					// Create the log node if necessary.
					let log_node = node
						.borrow()
						.children
						.iter()
						.position(|node| node.borrow().label.as_deref() == Some("log"));
					let log_node = log_node.unwrap_or_else(|| {
						// Create the log node.
						let child = Self::create_node(&handle, &node, Some("log".to_owned()), None);

						// Find where to insert it.
						let has_children_node =
							node.borrow().children.first().is_some_and(|node| {
								node.borrow().label.as_deref() == Some("children")
							});
						let index = if has_children_node { 1 } else { 0 };

						// Insert the new node and return the index.
						node.borrow_mut().children.insert(index, child);
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

	async fn process_title(handle: &H, process: &tg::Referent<tg::Process>) -> Option<String> {
		// Use the name if provided.
		if let Some(name) = process.name() {
			return Some(name.to_owned());
		}

		// Get the original commands' executable.
		let command = process.item.command(handle).await.ok()?.clone();
		let executable = command.executable(handle).await.ok()?.clone();

		// Handle paths.
		if let Ok(path) = executable.try_unwrap_path_ref() {
			return Some(path.path.display().to_string());
		}

		// Use the referent if its fields are set.
		let title = match (process.path(), process.tag()) {
			(Some(path), None) => path.display().to_string(),
			(Some(path), Some(tag)) => format!("{tag}:{}", path.display()),
			(None, Some(tag)) => tag.to_string(),
			_ => {
				if let Some(object) = executable.objects().first() {
					object.id().to_string()
				} else {
					String::new()
				}
			},
		};

		// Handle exports.
		let export = executable
			.try_unwrap_module_ref()
			.ok()
			.and_then(|exe| exe.export.as_ref());
		if let Some(export) = export {
			return Some(format!("{title}#{export}"));
		}

		Some(title)
	}

	async fn process_update_task(
		handle: &H,
		counter: UpdateCounter,
		process: &tg::Referent<tg::Process>,
		options: &Options,
		update_sender: NodeUpdateSender,
	) -> tg::Result<()>
	where
		H: tg::Handle,
	{
		if let Some(title) = Self::process_title(handle, process).await {
			let guard = counter.guard();
			update_sender
				.send(Box::new(|node| {
					node.borrow_mut().title = title;
					node.borrow_mut().guard.replace(guard);
				}))
				.unwrap();
		}

		// Create the status stream.
		let mut status = process.item.status(handle).await?;
		while let Some(status) = status.try_next().await? {
			let guard = counter.guard();
			let indicator = match status {
				tg::process::Status::Created => Indicator::Created,
				tg::process::Status::Enqueued => Indicator::Enqueued,
				tg::process::Status::Dequeued => Indicator::Dequeued,
				tg::process::Status::Started => Indicator::Started,
				tg::process::Status::Finished => {
					// Remove the child if necessary.
					if options.collapse_process_children {
						let update = move |node: Rc<RefCell<Node>>| {
							// Get the parent if it exists.
							let Some(parent) =
								node.borrow().parent.as_ref().and_then(Weak::upgrade)
							else {
								return;
							};

							// Find this process as a child of the parent.
							let Some(index) = parent
								.borrow()
								.children
								.iter()
								.position(|child| Rc::ptr_eq(child, &node))
							else {
								return;
							};

							// Mark the parent as dirty.
							parent.borrow_mut().guard.replace(guard);

							// Remove this node from its parent.
							parent.borrow_mut().children.remove(index);
						};
						update_sender.send(Box::new(update)).unwrap();
						return Ok(());
					}

					let state = process.item.load(handle).await?;
					let failed =
						state.error.is_some() || state.exit.as_ref().is_some_and(|code| *code != 0);
					if failed {
						Indicator::Failed
					} else {
						Indicator::Succeeded
					}
				},
			};
			let update = move |node: Rc<RefCell<Node>>| {
				node.borrow_mut().indicator.replace(indicator);
			};
			update_sender.send(Box::new(update)).ok();
		}

		// Check if the process was canceled.
		let arg = tg::process::get::Arg::default();
		if handle
			.try_get_process(process.item.id(), arg)
			.await?
			.and_then(|output| output.data.error)
			.is_some_and(|error| match error {
				tg::Either::Left(data) => {
					matches!(data.code, Some(tg::error::Code::Cancellation))
				},
				tg::Either::Right(_) => false,
			}) {
			let update = move |node: Rc<RefCell<Node>>| {
				node.borrow_mut().indicator.replace(Indicator::Canceled);
			};
			update_sender.send(Box::new(update)).ok();
		}

		Ok(())
	}

	fn push(&mut self) {
		if !Rc::ptr_eq(self.roots.last().unwrap(), &self.selected) {
			self.roots.push(self.selected.clone());
		}
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

			let disclosure = match node.borrow().expanded {
				Some(true) => "▼",
				Some(false) => "▶",
				None => "•",
			};
			line.push_span(disclosure);
			line.push_span(" ");

			let indicator = match node.borrow().indicator {
				None => None,
				Some(Indicator::Created) => Some("⟳".blue()),
				Some(Indicator::Enqueued) => Some("⟳".yellow()),
				Some(Indicator::Dequeued) => Some("•".yellow()),
				Some(Indicator::Started) => {
					let position = (now / (1000 / 10)) % 10;
					let position = position.to_usize().unwrap();
					Some(SPINNER[position].to_string().blue())
				},
				Some(Indicator::Canceled) => Some("⦻".yellow()),
				Some(Indicator::Failed) => Some("✗".red()),
				Some(Indicator::Succeeded) => Some("✓".green()),
				Some(Indicator::Error) => Some("?".red()),
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

		self.rect.replace(rect);
	}

	fn select(&mut self, node: Rc<RefCell<Node>>) {
		self.selected = node.clone();
		let Some(referent) = node.borrow().referent.clone() else {
			return;
		};
		let handle = self.handle.clone();
		let data = self.data.clone();
		let viewer = self.viewer.clone();
		let task = Task::spawn_local(|_| {
			async move {
				// Update the log view if the selected item is a process.
				let process =
					node.borrow()
						.referent
						.as_ref()
						.and_then(|referent| match referent.item() {
							Item::Process(process) => Some(process.clone()),
							_ => None,
						});
				{
					let handle = handle.clone();
					let update = move |viewer: &mut super::Viewer<H>| {
						if let Some(process) = process {
							let log = Log::new(&handle, &process);
							viewer.log.replace(log);
						} else {
							viewer.log.take();
						}
					};
					viewer.send(Box::new(update)).unwrap();
				}

				// Update the data view.
				let content_future = {
					let handle = handle.clone();
					async move {
						match referent.item {
							Item::Process(process) => handle
								.get_process(process.id())
								.await
								.and_then(|output| {
									serde_json::to_string_pretty(&output).map_err(|source| {
										tg::error!(!source, "failed to serialize the process data")
									})
								})
								.unwrap_or_else(|error| error.to_string()),
							Item::Value(tg::Value::Object(tg::Object::Blob(blob))) => {
								super::util::format_blob(&handle, &blob)
									.await
									.unwrap_or_else(|error| error.to_string())
							},
							Item::Value(value) => {
								let value = match value {
									tg::Value::Object(object) => {
										object.load(&handle).await.ok();
										tg::Value::Object(object)
									},
									value => value,
								};
								let options = tg::value::print::Options {
									depth: Some(1),
									style: tg::value::print::Style::Pretty { indentation: "  " },
									blobs: false,
									indent: 0,
								};
								value.print(options)
							},
							Item::Package(package) => {
								package.0.load(&handle).await.ok();
								let value = tg::Value::Object(package.0);
								let options = tg::value::print::Options {
									depth: Some(1),
									style: tg::value::print::Style::Pretty { indentation: "  " },
									blobs: false,
									indent: 0,
								};
								value.print(options)
							},
							Item::Tag(tag) => tag.to_string(),
						}
					}
				};
				let timeout = tokio::time::sleep(std::time::Duration::from_millis(20));
				match future::select(pin!(content_future), pin!(timeout)).await {
					future::Either::Left((content, _)) => {
						data.send(Box::new(move |this| this.set_contents(content)))
							.unwrap();
					},
					future::Either::Right(((), future)) => {
						data.send(Box::new(move |this| {
							this.set_contents("...loading...".to_owned());
						}))
						.unwrap();
						let contents = future.await;
						data.send(Box::new(move |this| {
							this.set_contents(contents);
						}))
						.unwrap();
					},
				}
			}
		});

		if let Some(task) = self.selected_task.replace(task) {
			task.abort();
		}
	}

	fn top(&mut self) {
		let nodes = self.nodes();
		self.select(nodes.first().unwrap().clone());
		self.scroll = 0;
	}

	fn up(&mut self) {
		let nodes = self.nodes();
		let index = nodes
			.iter()
			.position(|node| Rc::ptr_eq(node, &self.selected))
			.unwrap();
		let index = index.saturating_sub(1);
		self.select(nodes[index].clone());
		if index < self.scroll {
			self.scroll = self.scroll.saturating_sub(1);
		}
	}

	pub fn update(&mut self) {
		// Note we can't use .nodes() here because update() may create new ones.
		let mut stack = vec![self.roots.last().unwrap().clone()];
		while let Some(node) = stack.pop() {
			loop {
				let Ok(update) = node.borrow_mut().update_receiver.try_recv() else {
					break;
				};
				update(node.clone());
			}
			stack.extend(node.borrow().children.iter().rev().cloned());
		}
	}

	pub fn clear_guards(&mut self) {
		for node in self.nodes() {
			node.borrow_mut().guard.take();
		}
	}

	fn yank(&self) {
		let Some(referent) = self.selected.borrow().referent.clone() else {
			return;
		};
		let contents = match referent.item() {
			Item::Package(package) => package.0.id().to_string(),
			Item::Tag(tag) => tag.to_string(),
			Item::Process(process) => process.id().to_string(),
			Item::Value(value) => {
				if let tg::Value::Object(object) = value {
					Self::object_id(object)
				} else {
					let options = tg::value::print::Options {
						depth: Some(0),
						style: tg::value::print::Style::Compact,
						blobs: false,
						indent: 0,
					};
					value.print(options)
				}
			},
		};
		let Ok(mut tty) = std::fs::OpenOptions::new()
			.read(true)
			.write(true)
			.open("/dev/tty")
		else {
			return;
		};
		let encoded = data_encoding::BASE64.encode(contents.as_bytes());
		write!(tty, "\x1B]52;c;{encoded}\x07").ok();
		tty.flush().ok();
	}
}

impl<H> Drop for Tree<H> {
	fn drop(&mut self) {
		if let Some(task) = self.selected_task.take() {
			task.abort();
		}
	}
}

impl Drop for Node {
	fn drop(&mut self) {
		if let Some(task) = self.expand_task.take() {
			task.abort();
		}
		if let Some(task) = self.log_task.take() {
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

struct PackageVisitor {
	dependencies: BTreeMap<tg::Reference, Option<tg::file::Dependency>>,
}

impl<H> tg::object::Visitor<H> for PackageVisitor
where
	H: tg::Handle,
{
	async fn visit_directory(
		&mut self,
		_handle: &H,
		_directory: tg::Referent<&tg::Directory>,
	) -> tg::Result<bool> {
		Ok(true)
	}
	async fn visit_file(&mut self, handle: &H, file: tg::Referent<&tg::File>) -> tg::Result<bool> {
		let dependencies = file.item().dependencies(handle).await?;
		self.dependencies.extend(
			dependencies
				.into_iter()
				.filter(|(reference, _)| reference.item().is_tag()),
		);
		Ok(file.path().is_some())
	}
	async fn visit_symlink(
		&mut self,
		_handle: &H,
		_symlink: tg::Referent<&tg::Symlink>,
	) -> tg::Result<bool> {
		Ok(true)
	}
}
