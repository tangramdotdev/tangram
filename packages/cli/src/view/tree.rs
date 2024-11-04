use super::commands::Commands;
use futures::{
	future::{self, BoxFuture},
	stream::FuturesUnordered,
	FutureExt as _, StreamExt as _,
};
use num::ToPrimitive;
use ratatui::{
	buffer::Buffer,
	layout::{Position, Rect},
	style::{Color, Style},
	text::Line,
	widgets::{Paragraph, Widget},
};
use std::{
	collections::BTreeMap,
	fmt::Write as _,
	pin,
	sync::{Arc, RwLock, Weak},
};
use tangram_client as tg;
use tangram_either::Either;
use tangram_futures::task::Task;

pub struct Tree<H> {
	pub(super) commands: Arc<Commands<H>>,
	state: RwLock<State<H>>,
}

struct State<H> {
	rect: Rect,
	roots: Vec<Arc<RwLock<Node<H>>>>,
	scroll: usize,
	selected: Arc<RwLock<Node<H>>>,
}

struct Node<H> {
	// Static data
	options: Options,
	provider: Provider,

	// Dynamic data
	build_children: Option<Vec<Arc<RwLock<Self>>>>,
	indicator: Option<Indicator>,
	log: Option<String>,
	object_children: Option<Vec<Arc<RwLock<Self>>>>,
	title: Option<String>,

	// View data
	is_root: bool,
	parent: Option<Weak<RwLock<Self>>>,
	selected: bool,

	// Tasks
	build_children_task: Option<Task<()>>,
	log_task: Option<Task<()>>,
	object_children_task: Option<Task<()>>,
	status_task: Option<Task<()>>,
	title_task: Option<Task<()>>,
}

#[derive(Copy, Clone, Debug)]
pub struct Options {
	pub builds: bool,
	pub collapse_builds_on_success: bool,
	pub depth: Option<u32>,
	pub objects: bool,
}

#[derive(Default)]
pub struct Provider {
	build_children: Option<Method<tokio::sync::mpsc::UnboundedSender<Self>, ()>>,
	item: Option<Either<tg::Build, tg::Value>>,
	log: Option<Method<tokio::sync::watch::Sender<String>, ()>>,
	name: Option<String>,
	object_children: Option<Method<(), Vec<Self>>>,
	status: Option<Method<tokio::sync::watch::Sender<Indicator>, ()>>,
	title: Option<Method<(), String>>,
}

#[derive(Copy, Clone, Debug)]
pub enum Indicator {
	Created,
	Dequeued,
	Started,
	Canceled,
	Failed,
	Succeeded,
}

type Method<T, U> = Box<dyn FnMut(T) -> BoxFuture<'static, U> + Send + Sync>;

impl<H> Tree<H>
where
	H: tg::Handle,
{
	pub fn new(handle: &H, item: Either<tg::Build, tg::Object>, options: Options) -> Self {
		let provider = match item {
			Either::Left(build) => Provider::build(handle, None, None, build),
			Either::Right(object) => Provider::object(handle, None, &object),
		};
		let roots = vec![Node::new(None, provider, options)];
		let selected = roots[0].clone();
		selected.write().unwrap().selected = true;
		selected.write().unwrap().is_root = true;
		let state = State {
			rect: Rect::default(),
			scroll: 0,
			roots,
			selected,
		};
		let commands = Commands::tree();
		Self {
			commands,
			state: RwLock::new(state),
		}
	}

	pub fn stop(&self) {
		for root in &self.state.read().unwrap().roots {
			root.read().unwrap().stop();
		}
	}

	pub async fn wait(&self) {
		let roots = self.state.write().unwrap().roots.clone();
		roots
			.into_iter()
			.map(Node::wait)
			.collect::<FuturesUnordered<_>>()
			.collect::<()>()
			.await;
	}

	pub fn is_finished(&self) -> bool {
		let indicator = self
			.state
			.read()
			.unwrap()
			.roots
			.last()
			.unwrap()
			.read()
			.unwrap()
			.indicator;
		indicator.map_or(true, |indicator| {
			matches!(
				indicator,
				Indicator::Canceled | Indicator::Failed | Indicator::Succeeded
			)
		})
	}
}

impl<H> Tree<H>
where
	H: tg::Handle,
{
	/// Scroll to the top of the tree.
	pub fn top(&self) {
		let nodes = self.expanded_nodes();
		let mut state = self.state.write().unwrap();
		state.selected.write().unwrap().selected = false;
		state.selected = nodes[0].clone();
		state.selected.write().unwrap().selected = true;
		state.scroll = 0;
	}

	/// Scroll to the bottom of the tree.
	pub fn bottom(&self) {
		let nodes = self.expanded_nodes();
		let mut state = self.state.write().unwrap();
		state.scroll = nodes
			.len()
			.saturating_sub(state.rect.height.to_usize().unwrap().saturating_sub(2));
		state.selected.write().unwrap().selected = false;
		state.selected = nodes.last().unwrap().clone();
		state.selected.write().unwrap().selected = true;
	}

	/// Scroll up one item in the tree.
	pub fn up(&self) {
		self.select(false);
	}

	/// Scroll down one item in the tree.
	pub fn down(&self) {
		self.select(true);
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
			drop(state);
			self.pop();
			return;
		}

		// Update the nodes.
		expanded_nodes[previous_selected_index]
			.write()
			.unwrap()
			.selected = false;
		expanded_nodes[new_selected_index].write().unwrap().selected = true;
		state.selected = expanded_nodes[new_selected_index].clone();

		// Update the scroll if necessary.
		let height = state.rect.height.to_usize().unwrap().saturating_sub(2);
		if new_selected_index >= state.scroll && (new_selected_index - state.scroll) >= height {
			state.scroll += 1;
		} else if new_selected_index < state.scroll {
			state.scroll = state.scroll.saturating_sub(1);
		}
	}

	fn expanded_nodes(&self) -> Vec<Arc<RwLock<Node<H>>>> {
		let mut nodes = Vec::new();
		let root = self.state.read().unwrap().roots.last().unwrap().clone();
		let mut stack = vec![root];

		while let Some(node) = stack.pop() {
			nodes.push(node.clone());

			let node = node.read().unwrap();

			// Add the object children.
			let object_children = node.object_children.iter().flatten().rev().cloned();
			if node.options.objects {
				stack.extend(object_children);
			}

			// Add build children.
			let build_children = node.build_children.iter().flatten().rev().cloned();
			if node.options.builds {
				stack.extend(build_children);
			}
		}

		nodes
	}
}

impl<H> Tree<H> {
	/// Make the currently selected node the root.
	pub fn push(&self) {
		let mut state = self.state.write().unwrap();
		let new_root = state.selected.clone();
		if new_root.read().unwrap().is_root {
			return;
		}
		state
			.roots
			.last()
			.as_mut()
			.unwrap()
			.write()
			.unwrap()
			.is_root = false;
		state.roots.push(new_root);
		state
			.roots
			.last()
			.as_mut()
			.unwrap()
			.write()
			.unwrap()
			.is_root = true;
	}

	/// Return the previous root.
	pub fn pop(&self) {
		let mut state = self.state.write().unwrap();
		if state.roots.len() > 1 {
			state.roots.last().unwrap().write().unwrap().is_root = false;
			state.roots.pop();
			state.roots.last().unwrap().write().unwrap().is_root = true;
			state.roots.last().unwrap().write().unwrap().selected = true;
			state.selected.write().unwrap().selected = false;
			state.selected = state.roots.last().unwrap().clone();
		}
	}
}

impl<H> Tree<H>
where
	H: tg::Handle,
{
	/// Expand children.
	pub fn expand(&self, options: Options) {
		Node::expand(&self.selected(), options);
	}

	/// Collapse any children of the selected node.
	pub fn collapse_children(&self) {
		let selected = self.selected();
		if selected.read().unwrap().is_collapsed() {
			if selected.read().unwrap().is_root {
				self.pop();
				return;
			}
			let Some(parent) = selected.read().unwrap().parent().clone() else {
				return;
			};

			selected.write().unwrap().selected = false;
			parent.write().unwrap().selected = true;
			self.state.write().unwrap().selected = parent.clone();
			let expanded_nodes = self.expanded_nodes();
			let index = expanded_nodes
				.iter()
				.position(|node| Arc::ptr_eq(node, &parent))
				.unwrap();
			if index < self.state.read().unwrap().scroll {
				self.state.write().unwrap().scroll = index;
			}
		} else {
			selected.write().unwrap().collapse_build_children();
			selected.write().unwrap().collapse_object_children();
		}
	}

	fn selected(&self) -> Arc<RwLock<Node<H>>> {
		self.state.read().unwrap().selected.clone()
	}

	pub fn get_selected(&self) -> Either<tg::Build, tg::Value> {
		self.selected()
			.read()
			.unwrap()
			.provider
			.item
			.clone()
			.unwrap()
	}

	pub fn hit_test(&self, x: u16, y: u16) -> bool {
		self.state.read().unwrap().rect.contains(Position { x, y })
	}
}

impl<H> Node<H>
where
	H: tg::Handle,
{
	fn new(
		parent: Option<Arc<RwLock<Self>>>,
		provider: Provider,
		options: Options,
	) -> Arc<RwLock<Self>> {
		let node = Node {
			parent: parent.map(|parent| Arc::downgrade(&parent)),
			provider,
			options,
			title: None,
			build_children: None,
			object_children: None,
			indicator: None,
			log: None,
			selected: false,
			is_root: false,
			build_children_task: None,
			object_children_task: None,
			status_task: None,
			title_task: None,
			log_task: None,
		};
		let node = Arc::new(RwLock::new(node));
		if options.builds {
			Self::spawn_build_children_task(&node);
		}
		if options.objects {
			Self::spawn_object_children_task(&node);
		}
		Self::spawn_title_task(&node);
		Self::spawn_status_task(&node);
		Self::spawn_log_task(&node);
		node
	}
}

impl<H> Node<H>
where
	H: tg::Handle,
{
	fn ancestors(self_: &Arc<RwLock<Self>>) -> Vec<Arc<RwLock<Self>>> {
		let mut ancestors = Vec::new();
		let mut node = self_.clone();
		loop {
			let Some(parent) = node.read().unwrap().parent() else {
				break;
			};
			ancestors.push(parent.clone());
			node = parent;
		}
		ancestors
	}

	fn parent(&self) -> Option<Arc<RwLock<Self>>> {
		self.parent.as_ref().map(|p| p.upgrade().unwrap())
	}

	fn is_last_child(self_: &Arc<RwLock<Self>>) -> bool {
		let Some(parent) = self_.read().unwrap().parent() else {
			return true;
		};
		let parent = parent.read().unwrap();
		if let Some(build_children) = &parent.build_children {
			if let Some(last_child) = build_children.last() {
				if Arc::ptr_eq(last_child, self_) {
					return true;
				}
			}
		}
		if let Some(object_children) = &parent.object_children {
			if let Some(last_child) = object_children.last() {
				if Arc::ptr_eq(last_child, self_) {
					return true;
				}
			}
		}
		false
	}
}

impl<H> Node<H>
where
	H: tg::Handle,
{
	fn expand(self_: &Arc<RwLock<Self>>, options: Options) {
		self_.write().unwrap().options = options;
		if matches!(options.depth, Some(0)) {
			return;
		}
		if options.builds {
			self_.write().unwrap().build_children.replace(Vec::new());
			Self::spawn_build_children_task(self_);
		}
		if options.objects {
			self_.write().unwrap().object_children.replace(Vec::new());
			Self::spawn_object_children_task(self_);
		}
	}

	fn is_collapsed(&self) -> bool {
		!self.options.builds && !self.options.objects
	}

	fn collapse_build_children(&mut self) {
		self.options.builds = false;
		if let Some(task) = self.build_children_task.take() {
			task.abort();
		}
		for node in self.build_children.take().into_iter().flatten() {
			let mut node = node.write().unwrap();
			node.stop();
			node.collapse_build_children();
		}
	}

	fn collapse_object_children(&mut self) {
		self.options.objects = false;
		if let Some(task) = self.object_children_task.take() {
			task.abort();
		}
		for node in self.object_children.take().into_iter().flatten() {
			let mut node = node.write().unwrap();
			node.stop();
			node.collapse_object_children();
		}
	}

	fn stop(&self) {
		if let Some(task) = &self.title_task {
			task.stop();
		}
		if let Some(task) = &self.status_task {
			task.stop();
		}
		if let Some(task) = &self.build_children_task {
			task.stop();
		}
		if let Some(task) = &self.object_children_task {
			task.stop();
		}
		if let Some(task) = &self.log_task {
			task.stop();
		}

		// Stop the children.
		let children = self
			.build_children
			.iter()
			.flatten()
			.chain(self.object_children.iter().flatten());
		for child in children {
			child.read().unwrap().stop();
		}
	}

	async fn wait(self_: Arc<RwLock<Self>>) {
		let task = self_.write().unwrap().build_children_task.take();
		if let Some(task) = task {
			task.wait().await.unwrap();
		}
		let task = self_.write().unwrap().object_children_task.take();
		if let Some(task) = task {
			task.wait().await.unwrap();
		}
		let task = self_.write().unwrap().title_task.take();
		if let Some(task) = task {
			task.wait().await.unwrap();
		}
		let task = self_.write().unwrap().status_task.take();
		if let Some(task) = task {
			task.wait().await.unwrap();
		}
		let task = self_.write().unwrap().log_task.take();
		if let Some(task) = task {
			task.wait().await.unwrap();
		}
		let build_children = self_
			.write()
			.unwrap()
			.build_children
			.clone()
			.into_iter()
			.flatten();
		let object_children = self_
			.write()
			.unwrap()
			.object_children
			.clone()
			.into_iter()
			.flatten();
		build_children
			.chain(object_children)
			.map(|child| Self::wait(child))
			.collect::<FuturesUnordered<_>>()
			.collect::<()>()
			.await;
	}
}

impl<H> Node<H>
where
	H: tg::Handle,
{
	fn spawn_build_children_task(self_: &Arc<RwLock<Self>>) {
		let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
		let fut = {
			let mut self_ = self_.write().unwrap();
			let Some(build_children) = &mut self_.provider.build_children else {
				return;
			};
			build_children(sender)
		};
		let task = Task::spawn(|stop| {
			let subtask = Task::spawn(|_| fut);
			let parent = self_.clone();
			async move {
				if matches!(parent.read().unwrap().options.depth, Some(0)) {
					parent.write().unwrap().build_children.take();
					return;
				}
				loop {
					let stop = pin::pin!(stop.wait());
					let recv = pin::pin!(receiver.recv());
					match future::select(stop, recv).await {
						future::Either::Left(_) => {
							break;
						},
						future::Either::Right((recv, _)) => {
							let Some(provider) = recv else {
								break;
							};
							let mut options = parent.read().unwrap().options;
							options.depth = options.depth.map(|d| d.saturating_sub(1));
							let child = Self::new(Some(parent.clone()), provider, options);

							let mut parent = parent.write().unwrap();
							if parent.build_children.is_none() {
								parent.build_children.replace(Vec::new());
							}
							parent.build_children.as_mut().unwrap().push(child);
						},
					}
				}
				subtask.abort();
			}
		});
		if let Some(task) = self_.write().unwrap().build_children_task.replace(task) {
			task.abort();
		}
	}

	fn spawn_object_children_task(self_: &Arc<RwLock<Self>>) {
		let fut = {
			let mut self_ = self_.write().unwrap();
			let Some(object_children) = self_.provider.object_children.as_mut() else {
				return;
			};
			object_children(())
		};
		let task = Task::spawn({
			let parent = self_.clone();
			move |_| async move {
				let mut options = parent.read().unwrap().options;
				if matches!(options.depth, Some(0)) {
					parent.write().unwrap().object_children.take();
					return;
				}
				options.depth = options.depth.map(|d| d.saturating_sub(1));
				let children = fut
					.await
					.into_iter()
					.map(|provider| Self::new(Some(parent.clone()), provider, options))
					.collect::<Vec<_>>();
				parent.write().unwrap().object_children.replace(children);
			}
		});
		if let Some(task) = self_.write().unwrap().object_children_task.replace(task) {
			task.abort();
		}
	}

	fn spawn_title_task(self_: &Arc<RwLock<Self>>) {
		let fut = {
			let Some(title) = &mut self_.write().unwrap().provider.title else {
				return;
			};
			title(())
		};

		let task = Task::spawn(|stop| {
			let self_ = self_.clone();
			async move {
				let stop = pin::pin!(stop.wait());
				if let future::Either::Right((title, _)) = future::select(stop, fut).await {
					self_.write().unwrap().title.replace(title);
				}
			}
		});

		self_.write().unwrap().title_task.replace(task);
	}

	fn spawn_status_task(self_: &Arc<RwLock<Self>>) {
		let (sender, mut receiver) = tokio::sync::watch::channel(Indicator::Created);
		let fut = {
			let mut self_ = self_.write().unwrap();
			let Some(status) = &mut self_.provider.status else {
				return;
			};
			status(sender)
		};

		let subtask = Task::spawn(|_| fut);
		let task = Task::spawn({
			let self_ = self_.clone();
			move |stop| async move {
				loop {
					let indicator = *receiver.borrow_and_update();
					self_.write().unwrap().indicator.replace(indicator);
					if matches!(
						indicator,
						Indicator::Succeeded | Indicator::Failed | Indicator::Canceled
					) {
						break;
					}

					let changed = pin::pin!(receiver.changed());
					let stopped = pin::pin!(stop.wait());
					if let future::Either::Left((Ok(()), _)) =
						future::select(changed, stopped).await
					{
						continue;
					}
					break;
				}
				subtask.abort();

				// Remove the node from the tree if necessary.
				let child = self_.read().unwrap();
				let parent = child.parent();
				let indicator = child.indicator;
				let options = child.options;
				match (parent, indicator) {
					(Some(parent), Some(Indicator::Succeeded))
						if options.collapse_builds_on_success
							&& !child.selected && !child.is_root =>
					{
						let mut parent = parent.write().unwrap();
						let index = parent
							.build_children
							.as_ref()
							.unwrap()
							.iter()
							.position(|child| Arc::ptr_eq(&self_, child));
						if let Some(index) = index {
							parent.build_children.as_mut().unwrap().remove(index);
						}
					},
					_ => (),
				}
			}
		});

		self_.write().unwrap().status_task.replace(task);
	}

	fn spawn_log_task(self_: &Arc<RwLock<Self>>) {
		let (sender, mut receiver) = tokio::sync::watch::channel(String::new());

		let fut = {
			let mut self_ = self_.write().unwrap();
			let Some(log) = &mut self_.provider.log else {
				return;
			};
			log(sender)
		};

		let subtask = Task::spawn(|_| fut);
		let task = Task::spawn({
			let self_ = self_.clone();
			move |stop| async move {
				loop {
					let last_log_line = receiver.borrow_and_update().clone();
					self_.write().unwrap().log.replace(last_log_line);

					let changed = pin::pin!(receiver.changed());
					let stopped = pin::pin!(stop.wait());
					if let future::Either::Left((Ok(()), _)) =
						future::select(changed, stopped).await
					{
						continue;
					}
					break;
				}
				subtask.abort();
				self_.write().unwrap().log.take();
			}
		});

		self_.write().unwrap().log_task.replace(task);
	}
}

impl<H> Tree<H>
where
	H: tg::Handle,
{
	pub fn to_tree(&self) -> crate::tree::Tree {
		let state = self.state.read().unwrap();
		let root = state.roots.last().unwrap().clone();
		let tree = root.read().unwrap().to_tree();
		tree
	}
}

impl<H> Node<H>
where
	H: tg::Handle,
{
	pub fn to_tree(&self) -> crate::tree::Tree {
		use crossterm::style::Stylize as _;

		let mut title = String::new();
		match self.indicator {
			Some(Indicator::Created) => write!(title, "{} ", "⟳".yellow()).unwrap(),
			Some(Indicator::Dequeued) => write!(title, "{} ", "•".yellow()).unwrap(),
			Some(Indicator::Started) => {
				const SPINNER: [char; 10] = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];
				let now = std::time::SystemTime::now()
					.duration_since(std::time::UNIX_EPOCH)
					.unwrap()
					.as_millis();
				let position = (now / (1000 / 10)) % 10;
				let position = position.to_usize().unwrap();
				write!(title, "{} ", SPINNER[position].to_string().blue()).unwrap();
			},
			Some(Indicator::Canceled) => write!(title, "{} ", "⦻".yellow()).unwrap(),
			Some(Indicator::Failed) => write!(title, "{} ", "✗".red()).unwrap(),
			Some(Indicator::Succeeded) => write!(title, "{} ", "✓".green()).unwrap(),
			None => (),
		}

		if let Some(name) = &self.provider.name {
			write!(title, "{name}: ").unwrap();
		}

		write!(title, "{}", self.title.as_deref().unwrap_or("<unknown>")).unwrap();

		if let Some(log) = &self.log {
			write!(title, " {log}").unwrap();
		}

		if self.options.depth == Some(0) {
			return crate::tree::Tree {
				title,
				children: Vec::new(),
			};
		};
		let mut children = Vec::new();
		if self.options.builds {
			let build_children = self
				.build_children
				.iter()
				.flatten()
				.map(|child| child.read().unwrap().to_tree());
			children.extend(build_children);
		}
		if self.options.objects {
			let object_children = self
				.object_children
				.iter()
				.flatten()
				.map(|child| child.read().unwrap().to_tree());
			children.extend(object_children);
		}

		crate::tree::Tree { title, children }
	}
}

impl<H> Tree<H>
where
	H: tg::Handle,
{
	pub fn render(&self, area: Rect, buf: &mut Buffer) {
		// Flatten the tree.
		let nodes = self.expanded_nodes();

		// Get the nodes we care about.
		let state = self.state.read().unwrap();
		let height = area.height.to_usize().unwrap();
		let nodes = nodes.iter().skip(state.scroll).take(height);

		// Render the tree.
		for (area, node) in area.rows().zip(nodes) {
			Node::render(node, area, buf);
		}
	}

	pub fn resize(&self, area: Rect) {
		self.state.write().unwrap().rect = area;
	}
}

impl<H> Node<H>
where
	H: tg::Handle,
{
	fn render(self_: &Arc<RwLock<Self>>, area: Rect, buf: &mut Buffer) {
		use ratatui::style::Stylize as _;

		let ancestors = Self::ancestors(self_);
		let is_last_child = Self::is_last_child(self_);
		let node = self_.read().unwrap();

		let mut prefix = String::new();
		for node in ancestors.iter().rev().skip(1) {
			let is_last_child = Self::is_last_child(node);
			prefix.push_str(if is_last_child { "  " } else { "│ " });
		}

		if !node.is_root {
			prefix.push_str(if is_last_child { "└─" } else { "├─" });
		}

		let disclosure = if node.build_children.is_some() || node.object_children.is_some() {
			"▼"
		} else {
			"▶"
		};

		let indicator = match node.indicator {
			None => " ".red(),
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

		let mut title = String::new();
		if let Some(name) = &node.provider.name {
			title.push_str(name);
			title.push_str(": ");
		}
		title.push_str(node.title.as_deref().unwrap_or("<unknown>"));

		let style = if node.selected {
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
		Paragraph::new(line).render(area, buf);
	}
}

impl Provider {
	#[allow(clippy::needless_pass_by_value)]
	pub fn build<H>(
		handle: &H,
		parent: Option<tg::Build>,
		name: Option<String>,
		build: tg::Build,
	) -> Self
	where
		H: tg::Handle,
	{
		let title = {
			let handle = handle.clone();
			let build = build.clone();
			Box::new(move |()| {
				let parent = parent.clone();
				let handle = handle.clone();
				let build = build.clone();
				let fut = async move {
					match build_title(&handle, parent, build).await {
						Ok(title) => title,
						Err(error) => error.to_string(),
					}
				};
				Box::pin(fut) as BoxFuture<'static, _>
			})
		};

		let build_children = {
			let handle = handle.clone();
			let build = build.clone();
			Box::new(move |sender: tokio::sync::mpsc::UnboundedSender<Self>| {
				let handle = handle.clone();
				// let parent = parent
				let build = build.clone();
				let sender = sender.clone();
				let fut = async move {
					let Ok(mut children) = build
						.children(&handle, tg::build::children::get::Arg::default())
						.await
					else {
						return;
					};
					while let Some(child) = children.next().await {
						let Ok(child) = child else {
							continue;
						};
						let child = Self::build(&handle, Some(build.clone()), None, child);
						sender.send(child).ok();
					}
				};
				fut.boxed()
			})
		};

		let object_children = {
			let handle = handle.clone();
			let build = build.clone();
			Box::new(move |()| {
				let handle = handle.clone();
				let build = build.clone();
				let fut = async move {
					let Ok(target) = build.target(&handle).await else {
						return Vec::new();
					};
					let child = Self::object(&handle, Some("target".to_owned()), &target.into());
					vec![child]
				};
				fut.boxed()
			})
		};

		let status = {
			let handle = handle.clone();
			let build = build.clone();
			Box::new(move |watch: tokio::sync::watch::Sender<_>| {
				let handle = handle.clone();
				let build = build.clone();
				let fut = async move {
					let Ok(mut status) = build.status(&handle).await else {
						return;
					};
					while let Some(status) = status.next().await {
						let Ok(status) = status else {
							break;
						};
						let indicator = match status {
							tg::build::Status::Created => Indicator::Created,
							tg::build::Status::Dequeued => Indicator::Dequeued,
							tg::build::Status::Finished => break,
							tg::build::Status::Started => Indicator::Started,
						};
						watch.send(indicator).ok();
					}
					let outcome = build.outcome(&handle).await;
					let indicator = match outcome {
						Ok(tg::build::Outcome::Canceled) => Indicator::Canceled,
						Ok(tg::build::Outcome::Failed(_)) | Err(_) => Indicator::Failed,
						Ok(tg::build::Outcome::Succeeded(_)) => Indicator::Succeeded,
					};
					watch.send(indicator).ok();
				};
				fut.boxed()
			})
		};

		let log = {
			let handle = handle.clone();
			let build = build.clone();
			Box::new(move |watch: tokio::sync::watch::Sender<String>| {
				let handle = handle.clone();
				let build = build.clone();
				let fut = async move {
					let mut last_line = String::new();
					let Ok(mut log) = build
						.log(
							&handle,
							tg::build::log::get::Arg {
								position: Some(std::io::SeekFrom::Start(0)),
								..Default::default()
							},
						)
						.await
					else {
						return;
					};
					while let Some(chunk) = log.next().await {
						let Ok(chunk) = chunk else {
							break;
						};
						let chunk = String::from_utf8_lossy(&chunk.bytes);
						last_line.push_str(&chunk);
						last_line = last_line
							.lines()
							.last()
							.unwrap_or(last_line.as_str())
							.to_owned();
						watch.send(last_line.clone()).ok();
						last_line.push('\n');
					}
				};
				fut.boxed()
			})
		};

		Self {
			item: Some(Either::Left(build.clone())),
			name,
			title: Some(title),
			build_children: Some(build_children),
			object_children: Some(object_children),
			status: Some(status),
			log: Some(log),
		}
	}

	pub fn value<H>(handle: &H, name: Option<String>, value: &tg::Value) -> Self
	where
		H: tg::Handle,
	{
		match value {
			tg::Value::Array(value) => Self::array(handle, name, value),
			tg::Value::Map(value) => Self::map(handle, name, value),
			tg::Value::Mutation(value) => Self::mutation(handle, name, value),
			tg::Value::Object(value) => Self::object(handle, name, value),
			value => {
				let value_ = value.clone();
				let title = Box::new(move |()| {
					let fut = std::future::ready(value_.to_string());
					Box::pin(fut) as BoxFuture<'static, _>
				});
				Self {
					item: Some(Either::Right(value.clone())),
					name,
					title: Some(title),
					..Default::default()
				}
			},
		}
	}

	pub fn array<H>(handle: &H, name: Option<String>, value: &[tg::Value]) -> Self
	where
		H: tg::Handle,
	{
		let title = Box::new(|()| {
			let fut = async move { String::new() };
			Box::pin(fut) as BoxFuture<'static, _>
		});

		let object_children = {
			let handle = handle.clone();
			let value = value.to_owned();
			Box::new(move |()| {
				let handle = handle.clone();
				let value = value.clone();
				let fut = async move {
					value
						.iter()
						.enumerate()
						.map(|(index, value)| Self::value(&handle, Some(index.to_string()), value))
						.collect::<Vec<_>>()
				};
				fut.boxed()
			})
		};

		Self {
			item: Some(Either::Right(tg::Value::Array(value.to_owned()))),
			name,
			title: Some(title),
			object_children: Some(object_children),
			..Default::default()
		}
	}

	pub fn map<H>(handle: &H, name: Option<String>, value: &BTreeMap<String, tg::Value>) -> Self
	where
		H: tg::Handle,
	{
		let title = Box::new(|()| {
			let fut = async move { String::new() };
			Box::pin(fut) as BoxFuture<'static, _>
		});

		let object_children = {
			let handle = handle.clone();
			let value = value.clone();
			Box::new(move |()| {
				let handle = handle.clone();
				let value = value.clone();
				let fut = async move {
					value
						.iter()
						.map(|(name, value)| Self::value(&handle, Some(name.clone()), value))
						.collect::<Vec<_>>()
				};
				fut.boxed()
			})
		};

		Self {
			item: Some(Either::Right(tg::Value::Map(value.clone()))),
			name,
			title: Some(title),
			object_children: Some(object_children),
			..Default::default()
		}
	}

	pub fn mutation<H>(_handle: &H, name: Option<String>, value: &tg::Mutation) -> Self
	where
		H: tg::Handle,
	{
		let title = Box::new(move |()| {
			let fut = async move { "mutation".to_owned() };
			Box::pin(fut) as BoxFuture<'static, _>
		});

		Self {
			item: Some(Either::Right(tg::Value::Mutation(value.clone()))),
			name,
			title: Some(title),
			..Default::default()
		}
	}

	pub fn object<H>(handle: &H, name: Option<String>, object: &tg::Object) -> Self
	where
		H: tg::Handle,
	{
		let title = {
			let handle = handle.clone();
			let object = object.clone();
			Box::new(move |()| {
				let handle = handle.clone();
				let object = object.clone();
				let fut = async move {
					match object.id(&handle).await {
						Ok(id) => id.to_string(),
						Err(error) => error.to_string(),
					}
				};
				Box::pin(fut) as BoxFuture<'static, _>
			})
		};

		let object_children = {
			let handle = handle.clone();
			let object = object.clone();
			Box::new(move |()| {
				let handle = handle.clone();
				let object = object.clone();
				let fut = async move {
					object_children(&handle, &object)
						.await
						.into_iter()
						.map(|(name, value)| Self::value(&handle, name, &value))
						.collect::<Vec<_>>()
				};
				fut.boxed()
			})
		};

		Self {
			item: Some(Either::Right(tg::Value::Object(object.clone()))),
			name,
			title: Some(title),
			object_children: Some(object_children),
			..Default::default()
		}
	}
}

async fn build_title<H>(
	handle: &H,
	parent: Option<tg::Build>,
	build: tg::Build,
) -> tg::Result<String>
where
	H: tg::Handle,
{
	// let mut title = String::new();

	// // Get the target.
	// let target = build.target(handle).await?;
	// let host = target.host(handle).await?;

	// // If this is a builtin, use the first arg.
	// if host.as_str() == "builtin" {
	// 	let name = target
	// 		.args(handle)
	// 		.await?
	// 		.first()
	// 		.and_then(|arg| arg.try_unwrap_string_ref().ok())
	// 		.cloned()
	// 		.ok_or_else(|| tg::error!("expected a string"))?;
	// 	write!(title, "{name}").unwrap();
	// 	return Ok(title);
	// }

	// // Get the referrer if this is not a root.
	// 'a: {
	// 	let Some(parent) = parent else {
	// 		break 'a;
	// 	};
	// 	// Get the referrer.
	// 	let referrer = parent
	// 		.target(handle)
	// 		.await?
	// 		.executable(handle)
	// 		.await?
	// 		.clone()
	// 		.ok_or_else(|| tg::error!("expected an object"))?;
	// 	let referrer = match referrer {
	// 		tg::Artifact::Directory(_) => return Err(tg::error!("expected a file or symlink")),
	// 		tg::Artifact::File(file) => file,
	// 		tg::Artifact::Symlink(symlink) => {
	// 			let directory = symlink
	// 				.artifact(handle)
	// 				.await?
	// 				.ok_or_else(|| tg::error!("expected an object"))?
	// 				.clone()
	// 				.try_unwrap_directory()
	// 				.map_err(|_| tg::error!("expected a directory"))?;
	// 			let subpath = symlink
	// 				.subpath(handle)
	// 				.await?
	// 				.ok_or_else(|| tg::error!("expected a path"))?;
	// 			directory
	// 				.get(handle, &subpath)
	// 				.await?
	// 				.try_unwrap_file()
	// 				.map_err(|_| tg::error!("expected a file"))?
	// 		},
	// 	};

	// 	// Get the object.
	// 	let executable = target
	// 		.executable(handle)
	// 		.await?
	// 		.clone()
	// 		.ok_or_else(|| tg::error!("expected an object"))?;
	// 	let object: tg::object::Id = match executable {
	// 		tg::Artifact::Directory(_) => return Err(tg::error!("expected a file or symlink")),
	// 		tg::Artifact::File(file) => file.id(handle).await?.into(),
	// 		tg::Artifact::Symlink(symlink) => symlink
	// 			.artifact(handle)
	// 			.await?
	// 			.ok_or_else(|| tg::error!("expected an object"))?
	// 			.id(handle)
	// 			.await?
	// 			.into(),
	// 	};

	// 	// Get the referrer's dependencies.
	// 	let dependencies: Vec<_> = referrer
	// 		.dependencies(handle)
	// 		.await?
	// 		.into_iter()
	// 		.map(|(reference, dependency)| async move {
	// 			let id = dependency.object.id(handle).await?;
	// 			Ok::<_, tg::Error>((reference, id))
	// 		})
	// 		.collect::<FuturesUnordered<_>>()
	// 		.try_collect()
	// 		.await?;

	// 	// Find the object in the dependencies.
	// 	if let Some(reference) = dependencies
	// 		.iter()
	// 		.find_map(|(reference, id)| (id == &object).then_some(reference))
	// 	{
	// 		write!(title, "{reference}").unwrap();
	// 	}
	// }

	// if host.as_str() == "js" {
	// 	let name = target
	// 		.args(handle)
	// 		.await?
	// 		.first()
	// 		.and_then(|arg| arg.try_unwrap_string_ref().ok())
	// 		.cloned();
	// 	if let Some(name) = name {
	// 		write!(title, "#{name}").unwrap();
	// 	}
	// }
	// Ok(title)
	todo!()
}

async fn object_children(
	handle: &impl tg::Handle,
	object: &tg::Object,
) -> Vec<(Option<String>, tg::Value)> {
	let Ok(object) = object.object(handle).await else {
		return Vec::new();
	};
	match object {
		tg::object::Object::Branch(object) => {
			let branches = object
				.children
				.iter()
				.map(|child| {
					let child = match &child.blob {
						tg::Blob::Branch(branch) => {
							tg::Value::Object(tg::Object::Branch(branch.clone()))
						},
						tg::Blob::Leaf(leaf) => tg::Value::Object(tg::Object::Leaf(leaf.clone())),
					};
					(None, child)
				})
				.collect();
			branches
		},
		tg::object::Object::Leaf(_) => Vec::new(),
		tg::object::Object::Directory(object) => match object.as_ref() {
			tg::directory::Object::Graph { graph, node } => {
				vec![
					(
						Some("node".into()),
						tg::Value::Number(node.to_f64().unwrap()),
					),
					(None, tg::Value::Object(tg::Object::Graph(graph.clone()))),
				]
			},
			tg::directory::Object::Normal { entries } => entries
				.iter()
				.map(|(name, child)| (Some(name.clone()), tg::Value::Object(child.clone().into())))
				.collect(),
		},
		tg::object::Object::File(object) => match object.as_ref() {
			tg::file::Object::Graph { graph, node } => {
				vec![
					(
						Some("node".into()),
						tg::Value::Number(node.to_f64().unwrap()),
					),
					(None, tg::Value::Object(tg::Object::Graph(graph.clone()))),
				]
			},
			tg::file::Object::Normal {
				contents,
				dependencies,
				..
			} => {
				let mut children = vec![(
					Some("contents".into()),
					tg::Value::Object(contents.clone().into()),
				)];
				if !dependencies.is_empty() {
					let dependencies = dependencies
						.iter()
						.map(|(reference, referent)| {
							let mut name = reference.to_string();
							if let Some(tag) = &referent.tag {
								write!(name, "@{tag}").unwrap();
							}
							(name, tg::Value::Object(referent.item.clone()))
						})
						.collect();
					children.push((Some("dependencies".into()), tg::Value::Map(dependencies)));
				}
				children
			},
		},
		tg::object::Object::Symlink(object) => match object.as_ref() {
			tg::symlink::Object::Graph { graph, node } => {
				vec![
					(
						Some("node".into()),
						tg::Value::Number(node.to_f64().unwrap()),
					),
					(None, tg::Value::Object(tg::Object::Graph(graph.clone()))),
				]
			},
			tg::symlink::Object::Normal { artifact, .. } => {
				let mut children = Vec::new();
				if let Some(child) = artifact {
					children.push((None, tg::Value::Object(child.clone().into())));
				}
				children
			},
		},
		tg::object::Object::Graph(graph) => graph
			.nodes
			.iter()
			.enumerate()
			.map(|(index, node)| match node {
				tg::graph::Node::Directory(directory) => {
					let entries = directory
						.entries
						.iter()
						.map(|(name, entry)| {
							let child = match entry {
								Either::Left(index) => tg::Value::Number(index.to_f64().unwrap()),
								Either::Right(artifact) => {
									tg::Value::Object(artifact.clone().into())
								},
							};
							(name.clone(), child)
						})
						.collect();
					let value = [("entries".to_owned(), tg::Value::Map(entries))]
						.into_iter()
						.collect();
					(Some(index.to_string()), tg::Value::Map(value))
				},
				tg::graph::Node::File(file) => {
					let mut value = BTreeMap::new();
					let contents = tg::Value::Object(file.contents.clone().into());
					value.insert("contents".into(), contents);

					if !file.dependencies.is_empty() {
						let dependencies = file
							.dependencies
							.iter()
							.map(|(reference, referent)| {
								let mut value = BTreeMap::new();
								let object = match &referent.item {
									Either::Left(index) => {
										tg::Value::Number(index.to_f64().unwrap())
									},
									Either::Right(object) => tg::Value::Object(object.clone()),
								};
								value.insert("object".to_owned(), object);
								if let Some(tag) = &referent.tag {
									value.insert(
										"tag".to_owned(),
										tg::Value::String(tag.to_string()),
									);
								}
								(reference.to_string(), tg::Value::Map(value))
							})
							.collect();
						value.insert("dependencies".into(), tg::Value::Map(dependencies));
					}
					(Some(index.to_string()), tg::Value::Map(value))
				},
				tg::graph::Node::Symlink(symlink) => {
					let mut value = BTreeMap::new();
					if let Some(artifact) = &symlink.artifact {
						let object = match artifact {
							Either::Left(index) => tg::Value::Number(index.to_f64().unwrap()),
							Either::Right(artifact) => tg::Value::Object(artifact.clone().into()),
						};
						value.insert("artifact".into(), object);
					}
					(Some(index.to_string()), tg::Value::Map(value))
				},
			})
			.collect(),
		tg::object::Object::Target(target) => {
			let mut children = vec![
				(Some("args".into()), tg::Value::Array(target.args.clone())),
				(Some("env".into()), tg::Value::Map(target.env.clone())),
			];
			if let Some(executable) = &target.executable {
				let object = todo!();
				children.push((Some("executable".into()), object));
			}
			children
		},
	}
}
