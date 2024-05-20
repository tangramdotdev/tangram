use either::Either;
use futures::{future, StreamExt};
use num::ToPrimitive;
use ratatui as tui;
use std::{
	io::SeekFrom,
	pin::pin,
	sync::{Arc, RwLock, Weak},
};
use tangram_client as tg;
use tangram_futures::task::Task;
use tui::prelude::*;

pub struct Tree<H> {
	state: RwLock<TreeState<H>>,
}

struct TreeState<H> {
	rect: Rect,
	root: Vec<Arc<Node<H>>>,
	scroll: usize,
	selected: Arc<Node<H>>,
}

struct Node<H> {
	handle: H,
	index: usize,
	state: RwLock<NodeState<H>>,
}

#[allow(clippy::struct_excessive_bools)]
struct NodeState<H> {
	is_root: bool,
	build_children: Option<Vec<Arc<Node<H>>>>,
	object_children: Option<Vec<Arc<Node<H>>>>,
	expand_build_children: bool,
	expand_object_children: bool,
	indicator: Option<Indicator>,
	kind: Kind,
	parent: Option<Weak<Node<H>>>,
	selected: bool,
	title: Option<String>,
	build_children_task: Option<Task<()>>,
	object_children_task: Option<Task<()>>,
	status_task: Option<Task<()>>,
	title_task: Option<Task<()>>,
}

#[derive(Copy, Clone, Debug)]
enum Indicator {
	Errored,
	Created,
	Dequeued,
	Started,
	Canceled,
	Failed,
	Succeeded,
}

// multiroot
#[derive(Clone)]
enum Kind {
	Root,
	Build(tg::Build),
	Value {
		name: Option<String>,
		value: tg::Value,
	},
	Package {
		dependency: tg::Dependency,
		artifact: Option<tg::Artifact>,
		lock: tg::Lock,
	},
}

impl<H> Tree<H>
where
	H: tg::Handle,
{
	/// Create a new tree widget.
	pub fn new(handle: &H, roots: &[Either<tg::Build, tg::Value>], rect: Rect) -> Arc<Self> {
		// Create the root node.
		let root = Node::new(handle, None, 0, Kind::Root);

		// Create the root's children.
		let build_children = roots
			.iter()
			.enumerate()
			.filter_map(|(index, object)| {
				let build = object.as_ref().left()?.clone();
				let kind = Kind::Build(build);
				Some(Node::new(handle, Some(&root), index, kind))
			})
			.collect::<Vec<_>>();
		let object_children = roots
			.iter()
			.enumerate()
			.filter_map(|(index, object)| {
				let value = object.as_ref().right()?.clone();
				let kind = Kind::Value { name: None, value };
				Some(Node::new(handle, Some(&root), index, kind))
			})
			.collect::<Vec<_>>();

		// Select the first item
		let selected = build_children
			.first()
			.cloned()
			.unwrap_or_else(|| object_children.first().unwrap().clone());
		selected.state.write().unwrap().selected = true;

		// Update the root's state.
		root.state.write().unwrap().is_root = true;
		root.state
			.write()
			.unwrap()
			.build_children
			.replace(build_children);
		root.state
			.write()
			.unwrap()
			.object_children
			.replace(object_children);
		root.state.write().unwrap().expand_build_children = true;
		root.state.write().unwrap().expand_object_children = true;

		// Create the tree.
		let root = vec![root.clone()];
		let scroll = 0;
		let state = RwLock::new(TreeState {
			rect,
			root,
			scroll,
			selected,
		});
		Arc::new(Self { state })
	}

	/// Scroll up one item in the tree.
	pub fn up(&self) {
		self.select(false);
	}

	/// Scroll down one item in the tree.
	pub fn down(&self) {
		self.select(true);
	}

	/// Expand any build children of the node.
	pub fn expand_build_children(&self) {
		self.state.read().unwrap().selected.expand_build_children();
	}

	/// Expand any object children of the node.
	pub fn expand_object_children(&self) {
		self.state.read().unwrap().selected.expand_object_children();
	}

	/// Collapse any build children of the node.
	pub fn collapse_build_children(&self) {
		self.state
			.read()
			.unwrap()
			.selected
			.collapse_build_children();
	}

	/// Collapse any object children of the node.
	pub fn collapse_object_children(&self) {
		self.state
			.read()
			.unwrap()
			.selected
			.collapse_object_children();
	}

	/// Make the currently selected node the root.
	pub fn push(&self) {
		let mut state = self.state.write().unwrap();
		let new_root = state.selected.clone();
		state
			.root
			.last()
			.as_mut()
			.unwrap()
			.state
			.write()
			.unwrap()
			.is_root = false;
		state.root.push(new_root);
		state
			.root
			.last()
			.as_mut()
			.unwrap()
			.state
			.write()
			.unwrap()
			.is_root = true;
	}

	/// Return the previous root.
	pub fn pop(&self) {
		let mut state = self.state.write().unwrap();
		if state.root.len() > 1 {
			state
				.root
				.last()
				.as_mut()
				.unwrap()
				.state
				.write()
				.unwrap()
				.is_root = false;
			state.root.pop();
			state
				.root
				.last()
				.as_mut()
				.unwrap()
				.state
				.write()
				.unwrap()
				.is_root = true;
		}
	}

	fn root(&self) -> Arc<Node<H>> {
		self.state.read().unwrap().root.last().unwrap().clone()
	}

	fn selected(&self) -> Arc<Node<H>> {
		self.state.read().unwrap().selected.clone()
	}

	fn expanded_nodes(&self) -> Vec<Arc<Node<H>>> {
		let mut nodes = Vec::new();
		let mut stack = vec![self.root()];

		while let Some(node) = stack.pop() {
			let state = node.state.read().unwrap();

			// Add the node if it's not a root node.
			if !matches!(state.kind, Kind::Root) {
				nodes.push(node.clone());
			}

			// Add the object children.
			let object_children = state.object_children.iter().flatten().rev().cloned();
			if state.expand_object_children {
				stack.extend(object_children);
			}

			// Add build children.
			let build_children = state.build_children.iter().flatten().rev().cloned();
			if state.expand_build_children {
				stack.extend(build_children);
			}
		}

		nodes
	}

	fn select(&self, down: bool) {
		let expanded_nodes = self.expanded_nodes();

		// Calculate the index of the new selected node.
		let mut state = self.state.write().unwrap();
		let previous_selected_index = expanded_nodes
			.iter()
			.position(|item| Arc::ptr_eq(item, &state.selected))
			.unwrap();
		let new_selected_index = if down {
			(previous_selected_index + 1).min(expanded_nodes.len() - 1)
		} else {
			previous_selected_index.saturating_sub(1)
		};
		if new_selected_index == previous_selected_index {
			return;
		}

		// Update the nodes.
		expanded_nodes[previous_selected_index]
			.state
			.write()
			.unwrap()
			.selected = false;
		expanded_nodes[new_selected_index]
			.state
			.write()
			.unwrap()
			.selected = true;
		state.selected = expanded_nodes[new_selected_index].clone();

		// Update the scroll if necessary.
		let height = state.rect.height.to_usize().unwrap();
		if new_selected_index > state.scroll {
			state.scroll = state.scroll.saturating_sub(1);
		} else if (new_selected_index - state.scroll) > height {
			state.scroll += 1;
		}
	}

	pub fn render(&self, area: Rect, buf: &mut Buffer) {
		// Flatten the tree.
		let nodes = self.expanded_nodes();

		// Get the nodes we care about.
		let state = self.state.read().unwrap();
		let height = area.height.to_usize().unwrap();
		let nodes = nodes.iter().skip(state.scroll).take(height);

		// Render the tree.
		for (area, node) in area.rows().zip(nodes) {
			node.render(area, buf);
		}
	}

	pub fn resize(&self, area: Rect) {
		self.state.write().unwrap().rect = area;
	}

	pub fn get_selected(&self) -> Either<tg::Build, tg::Value> {
		match self.selected().state.read().unwrap().kind.clone() {
			Kind::Root => unreachable!(),
			Kind::Build(build) => Either::Left(build),
			Kind::Value { value, .. } => Either::Right(value),
			Kind::Package { lock, .. } => Either::Right(tg::Value::Object(lock.into())),
		}
	}

	pub fn stop(&self) {
		self.root().stop();
	}

	pub async fn wait(&self) {
		self.root().wait().await;
	}
}

impl<H> Node<H>
where
	H: tg::Handle,
{
	// Create a new tree node.
	fn new(handle: &H, parent: Option<&Arc<Self>>, index: usize, kind: Kind) -> Arc<Self> {
		// Create the node.
		let handle = handle.clone();
		let parent = parent.map(Arc::downgrade);
		let state = RwLock::new(NodeState {
			is_root: false,
			build_children: None,
			object_children: None,
			expand_build_children: false,
			expand_object_children: false,
			indicator: None,
			kind,
			parent,
			selected: false,
			title: None,
			build_children_task: None,
			object_children_task: None,
			status_task: None,
			title_task: None,
		});
		let node = Arc::new(Self {
			handle,
			index,
			state,
		});

		// Spawn the tasks.
		let mut state = node.state.write().unwrap();
		let status_task = node.status_task();
		state.status_task.replace(status_task);

		let title_task = node.title_task();
		state.title_task.replace(title_task);
		drop(state);

		// Return the node.
		node
	}

	fn expand_build_children(self: &Arc<Self>) {
		let mut state = self.state.write().unwrap();
		if state.expand_build_children {
			return;
		}
		state.expand_build_children = true;
		state.build_children.replace(Vec::new());
		if let Some(task) = state
			.build_children_task
			.replace(self.build_children_task())
		{
			task.abort();
		}
	}

	fn expand_object_children(self: &Arc<Self>) {
		let mut state = self.state.write().unwrap();
		if state.expand_object_children {
			return;
		}

		state.expand_object_children = true;
		state.object_children.replace(Vec::new());
		if let Some(task) = state
			.object_children_task
			.replace(self.object_children_task())
		{
			task.abort();
		}
	}

	fn collapse_build_children(self: &Arc<Self>) {
		let mut state = self.state.write().unwrap();
		state.expand_build_children = false;
		if let Some(task) = state.build_children_task.take() {
			task.abort();
		}
		state
			.build_children
			.take()
			.into_iter()
			.flatten()
			.for_each(|node| {
				node.stop();
				node.collapse_build_children();
			});
	}

	fn collapse_object_children(self: &Arc<Self>) {
		let mut state = self.state.write().unwrap();
		state.expand_object_children = false;
		if let Some(task) = state.object_children_task.take() {
			task.abort();
		}
		state
			.object_children
			.take()
			.into_iter()
			.flatten()
			.for_each(|node| {
				node.stop();
				node.collapse_object_children();
			});
	}

	fn ancestors(self: &Arc<Self>) -> Vec<Arc<Self>> {
		let mut ancestors = Vec::new();
		let mut node = self.clone();
		loop {
			if node.state.read().unwrap().is_root {
				break;
			}
			let Some(parent) = node
				.state
				.read()
				.unwrap()
				.parent
				.as_ref()
				.map(|node| node.upgrade().unwrap())
			else {
				break;
			};
			ancestors.push(parent.clone());
			node = parent;
		}
		ancestors
	}

	fn parent(&self) -> Option<Arc<Self>> {
		self.state.read().unwrap().parent.as_ref()?.upgrade()
	}

	fn is_last_child(&self) -> bool {
		let state = self.state.read().unwrap();
		let Some(parent) = self.parent() else {
			return true;
		};
		let parent_state = parent.state.read().unwrap();
		match (&parent_state.kind, &state.kind) {
			(Kind::Build(_), Kind::Build(_)) if parent_state.expand_object_children => false,
			(Kind::Build(_) | Kind::Root, Kind::Build(_)) => {
				self.index == parent_state.build_children.as_ref().map_or(0, Vec::len) - 1
			},
			_ => self.index == parent_state.object_children.as_ref().map_or(0, Vec::len) - 1,
		}
	}

	fn render(self: &Arc<Self>, area: Rect, buf: &mut Buffer) {
		let ancestors = self.ancestors();
		let state = self.state.read().unwrap();

		let mut prefix = String::new();
		for node in ancestors.iter().rev().skip(1) {
			prefix.push_str(if node.is_last_child() { "  " } else { "│ " });
		}
		if !state.is_root {
			prefix.push_str(if self.is_last_child() {
				"└─"
			} else {
				"├─"
			});
		}
		let disclosure = if state.expand_build_children | state.expand_object_children {
			"▼"
		} else {
			"▶"
		};

		let indicator = match state.indicator {
			None => " ".red(),
			Some(Indicator::Errored) => "!".red(),
			Some(Indicator::Created) => "⟳".yellow(),
			Some(Indicator::Dequeued) => "•".yellow(),
			Some(Indicator::Started) => {
				const SPINNER: [char; 10] = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];
				let now = std::time::SystemTime::now()
					.duration_since(std::time::UNIX_EPOCH)
					.unwrap()
					.as_millis();
				let position = (now / (1000 / 10)) % 10;
				let position = position.to_usize().unwrap();
				SPINNER[position].to_string().blue()
			},
			Some(Indicator::Canceled) => "⦻".yellow(),
			Some(Indicator::Failed) => "✗".red(),
			Some(Indicator::Succeeded) => "✓".green(),
		};

		let title = state.title.as_deref().unwrap_or("<unknown>");

		let style = if state.selected {
			Style::default().bg(Color::White).fg(Color::Black)
		} else {
			Style::default()
		};
		let line = Line::from(vec![
			prefix.into(),
			disclosure.into(),
			" ".into(),
			indicator,
			" ".into(),
			title.into(),
		])
		.style(style);
		tui::widgets::Paragraph::new(line).render(area, buf);
	}

	// Spawn a task to update the node's object children.
	fn object_children_task(self: &Arc<Self>) -> Task<()> {
		Task::spawn({
			let node = self.clone();
			|_| async move {
				if let Ok(children) = node.try_get_object_children().await {
					node.state
						.write()
						.unwrap()
						.object_children
						.replace(children);
				} else {
					let mut state = node.state.write().unwrap();
					state.indicator.replace(Indicator::Errored);
					state.title.replace("failed to get object children".into());
				}
			}
		})
	}

	async fn try_get_object_children(self: &Arc<Self>) -> tg::Result<Vec<Arc<Self>>> {
		let kind = self.state.read().unwrap().kind.clone();
		match kind {
			Kind::Root => unreachable!(),
			Kind::Build(build) => {
				let target = build.target(&self.handle).await?;
				let child = Self::new(
					&self.handle,
					Some(self),
					0,
					Kind::Value {
						name: Some("target".into()),
						value: tg::Value::Object(target.into()),
					},
				);
				Ok(vec![child])
			},
			Kind::Value {
				value: tg::Value::Object(tg::Object::Branch(object)),
				..
			} => {
				let mut children = Vec::new();
				for (index, child) in object.children(&self.handle).await?.iter().enumerate() {
					let child = Self::new(
						&self.handle,
						Some(self),
						index,
						Kind::Value {
							name: None,
							value: child.blob.clone().into(),
						},
					);
					children.push(child);
				}
				Ok(children)
			},
			Kind::Value {
				value: tg::Value::Object(tg::Object::Directory(object)),
				..
			} => {
				let mut children = Vec::new();
				for (index, (name, artifact)) in
					object.entries(&self.handle).await?.iter().enumerate()
				{
					let child = Self::new(
						&self.handle,
						Some(self),
						index,
						Kind::Value {
							name: Some(name.clone()),
							value: tg::Value::Object(artifact.clone().into()),
						},
					);
					children.push(child);
				}
				Ok(children)
			},
			Kind::Value {
				value: tg::Value::Object(tg::Object::File(object)),
				..
			} => {
				let contents = object.contents(&self.handle).await?;
				let child = Self::new(
					&self.handle,
					Some(self),
					0,
					Kind::Value {
						name: Some("contents".into()),
						value: contents.into(),
					},
				);
				Ok(vec![child])
			},
			Kind::Value {
				value: tg::Value::Object(tg::Object::Symlink(object)),
				..
			} => {
				let mut children = Vec::new();
				let artifact = object.artifact(&self.handle).await?.clone();
				if let Some(artifact) = artifact {
					let child = Self::new(
						&self.handle,
						Some(self),
						children.len(),
						Kind::Value {
							name: Some("artifact".into()),
							value: tg::Value::Object(artifact.into()),
						},
					);
					children.push(child);
				}
				let path = object.path(&self.handle).await?.clone();
				if let Some(path) = path {
					let child = Self::new(
						&self.handle,
						Some(self),
						children.len(),
						Kind::Value {
							name: Some("path".into()),
							value: path.into(),
						},
					);
					children.push(child);
				}
				Ok(children)
			},
			Kind::Value {
				value: tg::Value::Object(tg::Object::Target(object)),
				..
			} => {
				let executable = &*object.executable(&self.handle).await?;
				let executable = Self::new(
					&self.handle,
					Some(self),
					0,
					Kind::Value {
						name: Some("executable".into()),
						value: executable.clone().into(),
					},
				);

				let args = &*object.args(&self.handle).await?;
				let args = Self::new(
					&self.handle,
					Some(self),
					1,
					Kind::Value {
						name: Some("args".into()),
						value: args.clone().into(),
					},
				);

				let env = &*object.env(&self.handle).await?;
				let env = Self::new(
					&self.handle,
					Some(self),
					2,
					Kind::Value {
						name: Some("env".into()),
						value: env.clone().into(),
					},
				);

				let mut children = vec![executable, args, env];

				let lock = &*object.lock(&self.handle).await?;
				if let Some(lock) = lock {
					let lock = Self::new(
						&self.handle,
						Some(self),
						children.len(),
						Kind::Value {
							name: Some("lock".into()),
							value: tg::Value::Object(lock.clone().into()),
						},
					);
					children.push(lock);
				}

				Ok(children)
			},
			Kind::Value {
				value: tg::Value::Object(tg::Object::Lock(object)),
				..
			} => {
				let dependencies = object.dependencies(&self.handle).await?;
				let mut children = Vec::with_capacity(dependencies.len());
				for (index, dependency) in dependencies.into_iter().enumerate() {
					let (artifact, lock) = object.get(&self.handle, &dependency).await?;
					let child = Self::new(
						&self.handle,
						Some(self),
						index,
						Kind::Package {
							dependency,
							artifact: artifact.map(tg::Artifact::from),
							lock,
						},
					);
					children.push(child);
				}
				Ok(children)
			},
			Kind::Value {
				value: tg::Value::Map(value),
				..
			} => {
				let mut children = Vec::with_capacity(value.len());
				for (name, value) in value {
					let child = Self::new(
						&self.handle,
						Some(self),
						children.len(),
						Kind::Value {
							name: Some(name),
							value,
						},
					);
					children.push(child);
				}
				Ok(children)
			},
			Kind::Value {
				value: tg::Value::Array(value),
				..
			} => {
				let mut children = Vec::with_capacity(value.len());
				for value in value {
					let child = Self::new(
						&self.handle,
						Some(self),
						children.len(),
						Kind::Value { name: None, value },
					);
					children.push(child);
				}
				Ok(children)
			},
			Kind::Value {
				value: tg::Value::Template(value),
				..
			} => {
				let mut children = Vec::new();
				for (index, child) in value.artifacts().enumerate() {
					let child = Self::new(
						&self.handle,
						Some(self),
						index,
						Kind::Value {
							name: None,
							value: tg::Value::Object(child.clone().into()),
						},
					);
					children.push(child);
				}
				Ok(children)
			},
			Kind::Package { artifact, lock, .. } => {
				let mut children = Vec::new();
				if let Some(artifact) = artifact {
					let child = Self::new(
						&self.handle,
						Some(self),
						0,
						Kind::Value {
							name: Some("artifact".into()),
							value: tg::Value::Object(artifact.into()),
						},
					);
					children.push(child);
				}
				let dependencies = lock.dependencies(&self.handle).await?;
				for dependency in dependencies {
					let index = children.len();
					let (artifact, lock) = lock.get(&self.handle, &dependency).await?;
					let child = Self::new(
						&self.handle,
						Some(self),
						index,
						Kind::Package {
							dependency,
							artifact: artifact.map(tg::Artifact::from),
							lock,
						},
					);
					children.push(child);
				}
				Ok(children)
			},
			Kind::Value { .. } => Ok(Vec::new()),
		}
	}

	// Spawn a task to update the node's build children.
	fn build_children_task(self: &Arc<Self>) -> Task<()> {
		Task::spawn({
			let node = self.clone();
			|stop| async move {
				let kind = node.state.read().unwrap().kind.clone();
				let Kind::Build(build) = kind else {
					return;
				};
				let arg = tg::build::children::Arg {
					position: Some(SeekFrom::Start(0)),
					..Default::default()
				};
				let Ok(mut stream) = build.children(&node.handle, arg).await else {
					return;
				};

				// Read children from the stream. If an error occurs, attempt to reconnect.
				loop {
					let stopped = stop.stopped();
					let child = match future::select(pin!(stopped), stream.next()).await {
						// If the task is stopped or the stream is ended, return.
						future::Either::Left(_) | future::Either::Right((None, _)) => {
							return;
						},

						// If the stream errors, attempt to reconnect.
						future::Either::Right((Some(Err(error)), _)) => {
							tracing::error!(%error, "error reading from stream");
							return;
						},

						// Otherwise add the child to the tree.
						future::Either::Right((Some(Ok(child)), _)) => child,
					};

					// Create a new tree node for the child.
					let mut state = node.state.write().unwrap();
					let children = state.build_children.as_mut().unwrap();
					let child_node = Self::new(
						&node.handle,
						Some(&node),
						children.len(),
						Kind::Build(child),
					);

					// Add the new child node.
					children.push(child_node);
				}
			}
		})
	}

	// Spawn a task to update the node's status indicator.
	fn status_task(self: &Arc<Self>) -> Task<()> {
		Task::spawn({
			let node = self.clone();
			|stop| async move {
				let Kind::Build(build) = node.state.read().unwrap().kind.clone() else {
					return;
				};
				let arg = tg::build::status::Arg::default();
				let Ok(mut stream) = build.status(&node.handle, arg).await else {
					return;
				};

				loop {
					let stopped = stop.stopped();
					let status = match future::select(pin!(stopped), stream.next()).await {
						future::Either::Left(_) | future::Either::Right((None, _)) => return,
						future::Either::Right((Some(Err(error)), _)) => {
							tracing::error!(%error, "error reading from stream");
							break;
						},
						future::Either::Right((Some(Ok(status)), _)) => status,
					};
					let indicator = match status {
						tg::build::Status::Created => Indicator::Created,
						tg::build::Status::Dequeued => Indicator::Dequeued,
						tg::build::Status::Started => Indicator::Started,
						tg::build::Status::Finished => {
							let outcome = build.outcome(&node.handle).await;
							match outcome {
								Ok(tg::build::Outcome::Canceled) => Indicator::Canceled,
								Ok(tg::build::Outcome::Succeeded(_)) => Indicator::Succeeded,
								Ok(tg::build::Outcome::Failed(_)) => Indicator::Failed,
								Err(_) => Indicator::Errored,
							}
						},
					};
					node.state.write().unwrap().indicator.replace(indicator);
				}
			}
		})
	}

	// Spawn a task to update the node's title.
	fn title_task(self: &Arc<Self>) -> Task<()> {
		Task::spawn({
			let node = self.clone();
			|_| async move {
				let kind = node.state.read().unwrap().kind.clone();
				let result = match kind {
					Kind::Root => return,
					Kind::Build(build) => node.set_build_title(build).await,
					Kind::Value { name, value } => node.set_value_title(name, value).await,
					Kind::Package { dependency, .. } => {
						node.state
							.write()
							.unwrap()
							.title
							.replace(dependency.to_string());
						Ok(())
					},
				};
				if let Err(error) = result {
					let mut state = node.state.write().unwrap();
					state.indicator.replace(Indicator::Errored);
					state.title.replace(error.to_string());
				}
			}
		})
	}

	async fn set_build_title(&self, build: tg::Build) -> tg::Result<()> {
		let target = build.target(&self.handle).await?;

		// Get the package metadata
		let package = target.package(&self.handle).await?;
		let metadata = if let Some(package) = package {
			tg::package::try_get_metadata(&self.handle, &package.into())
				.await
				.ok()
				.flatten()
		} else {
			None
		};

		// Get the package name and version.
		let package = metadata.map(|metadata| {
			let name = metadata.name.as_deref().unwrap_or("<unknown>");
			let version = metadata.version.as_deref().unwrap_or("<unknown>");
			format!("{name}@{version}")
		});

		// Get the target name.
		let name = target
			.args(&self.handle)
			.await?
			.first()
			.and_then(|arg| arg.try_unwrap_string_ref().ok())
			.cloned();

		// Update the tree state.
		match (package, name) {
			(Some(package), Some(name)) => {
				self.state
					.write()
					.unwrap()
					.title
					.replace(format!("{package} {name}"));
			},
			(None, Some(name)) => {
				self.state.write().unwrap().title.replace(name);
			},
			_ => (),
		}

		Ok(())
	}

	async fn set_value_title(&self, name: Option<String>, value: tg::Value) -> tg::Result<()> {
		let mut title = String::new();
		if let Some(name) = name {
			title.push_str(&name);
			title.push_str(": ");
		}
		match value {
			tg::Value::Array(_) => title.push_str("array"),
			tg::Value::Map(_) => title.push_str("map"),
			tg::Value::Mutation(_) => title.push_str("mutation"),
			tg::Value::Path(value) => title.push_str(value.as_ref()),
			tg::Value::Object(value) => {
				let id = value.id(&self.handle, None).await?;
				title.push_str(&id.to_string());
			},
			value => title.push_str(&value.to_string()),
		}
		let mut state = self.state.write().unwrap();
		state.title.replace(title);
		Ok(())
	}

	// Stop running tasks.
	pub fn stop(&self) {
		let state = self.state.read().unwrap();
		if let Some(task) = &state.title_task {
			task.stop();
		}
		if let Some(task) = &state.status_task {
			task.stop();
		}
		if let Some(task) = &state.build_children_task {
			task.stop();
		}
		if let Some(task) = &state.object_children_task {
			task.stop();
		}

		// Stop the children.
		let children = state
			.build_children
			.iter()
			.flatten()
			.chain(state.object_children.iter().flatten());
		for child in children {
			child.stop();
		}
	}

	// Wait for running tasks to complete.
	pub async fn wait(self: &Arc<Self>) {
		// Wait for all the tasks.
		let title_task = self.state.write().unwrap().title_task.take();
		if let Some(task) = title_task {
			task.wait().await;
		}

		let status_task = self.state.write().unwrap().status_task.take();
		if let Some(task) = status_task {
			task.wait().await;
		}

		let build_children_task = self.state.write().unwrap().build_children_task.take();
		if let Some(task) = build_children_task {
			task.wait().await;
		}

		let object_children_task = self.state.write().unwrap().object_children_task.take();
		if let Some(task) = object_children_task {
			task.wait().await;
		}

		// Wait for the chilren.
		let children = {
			let mut state = self.state.write().unwrap();
			let build_children = state.build_children.take().into_iter().flatten();
			let object_children = state.object_children.take().into_iter().flatten();
			build_children.chain(object_children)
		};

		for child in children {
			Box::pin(child.wait()).await;
		}
	}
}
