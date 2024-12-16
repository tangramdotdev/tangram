use super::{Item, Options};
use crossterm as ct;
use futures::{Future, TryFutureExt as _};
use num::ToPrimitive as _;
use ratatui::{self as tui, prelude::*};
use std::{
	cell::RefCell,
	fmt::Write as _,
	rc::{Rc, Weak},
};
use tangram_client as tg;
use tangram_futures::task::Task;

const SPINNER: [char; 10] = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];

pub struct Tree<H> {
	handle: H,
	options: Options,
	rect: Option<Rect>,
	roots: Vec<Rc<RefCell<Node>>>,
	selected: Rc<RefCell<Node>>,
	scroll: usize,
}

struct Node {
	children: Vec<Rc<RefCell<Self>>>,
	expanded: bool,
	indicator: Option<Indicator>,
	item: Item,
	label: Option<String>,
	parent: Option<Weak<RefCell<Self>>>,
	ready_sender: tokio::sync::watch::Sender<bool>,
	task: Option<Task<()>>,
	title: String,
	update_receiver: NodeUpdateReceiver,
	update_sender: NodeUpdateSender,
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
			if let Some(task) = node.task.take() {
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
		item: &Item,
	) -> Rc<RefCell<Node>> {
		let (update_sender, update_receiver) = std::sync::mpsc::channel();
		let (ready_sender, _) = tokio::sync::watch::channel(false);
		let parent = Rc::downgrade(parent);
		let title = Self::item_title(item);
		Rc::new(RefCell::new(Node {
			children: Vec::new(),
			expanded: false,
			indicator: None,
			item: item.clone(),
			label,
			parent: Some(parent),
			ready_sender,
			task: None,
			title,
			update_receiver,
			update_sender,
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
		let children_task = Task::spawn_local({
			let handle = self.handle.clone();
			let item = node.item.clone();
			let update_sender = node.update_sender.clone();
			let ready_sender = node.ready_sender.clone();
			move |_| async move { Self::task(&handle, item, update_sender, ready_sender).await }
		});
		node.task.replace(children_task);
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

	async fn handle_array(
		_handle: &H,
		array: tg::value::Array,
		update_sender: NodeUpdateSender,
	) -> tg::Result<()> {
		let update = |node: Rc<RefCell<Node>>| {
			let children = array.into_iter().map(|value| {
				let item = Item::Value(value);
				Self::create_node(&node, None, &item)
			});
			node.borrow_mut().children.extend(children);
		};
		update_sender.send(Box::new(update)).unwrap();
		Ok(())
	}

	async fn handle_branch(
		handle: &H,
		branch: tg::Branch,
		update_sender: NodeUpdateSender,
	) -> tg::Result<()> {
		Ok(())
	}

	async fn handle_build(
		handle: &H,
		build: tg::Build,
		update_sender: NodeUpdateSender,
	) -> tg::Result<()> {
		Ok(())
	}

	async fn handle_directory(
		handle: &H,
		directory: tg::Directory,
		update_sender: NodeUpdateSender,
	) -> tg::Result<()> {
		let entries = directory.entries(handle).await?;
		let entries = entries
			.into_iter()
			.map(|(name, artifact)| (name, artifact.into()))
			.collect();
		let value = tg::Value::Map(entries);
		let update = |node: Rc<RefCell<Node>>| {
			let item = Item::Value(value);
			let child = Self::create_node(&node, Some("entries".to_owned()), &item);
			node.borrow_mut().children.push(child);
		};
		update_sender.send(Box::new(update)).unwrap();
		Ok(())
	}

	async fn handle_file(
		handle: &H,
		file: tg::File,
		update_sender: NodeUpdateSender,
	) -> tg::Result<()> {
		Ok(())
	}

	async fn handle_graph(
		handle: &H,
		graph: tg::Graph,
		update_sender: NodeUpdateSender,
	) -> tg::Result<()> {
		Ok(())
	}

	async fn handle_leaf(
		_handle: &H,
		_leaf: tg::Leaf,
		_update_sender: NodeUpdateSender,
	) -> tg::Result<()> {
		Ok(())
	}

	async fn handle_map(
		_handle: &H,
		map: tg::value::Map,
		update_sender: NodeUpdateSender,
	) -> tg::Result<()> {
		let update = |node: Rc<RefCell<Node>>| {
			let children = map.into_iter().map(|(key, value)| {
				let item = Item::Value(value);
				Self::create_node(&node, Some(key), &item)
			});
			node.borrow_mut().children.extend(children);
		};
		update_sender.send(Box::new(update)).unwrap();
		Ok(())
	}

	async fn handle_mutation(
		handle: &H,
		value: tg::Mutation,
		update_sender: NodeUpdateSender,
	) -> tg::Result<()> {
		Ok(())
	}

	async fn handle_object(
		handle: &H,
		object: tg::Object,
		update_sender: NodeUpdateSender,
	) -> tg::Result<()> {
		match object {
			tg::Object::Leaf(leaf) => {
				Self::handle_leaf(handle, leaf, update_sender).await?;
			},
			tg::Object::Branch(branch) => {
				Self::handle_branch(handle, branch, update_sender).await?;
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
		Ok(())
	}

	async fn handle_target(
		handle: &H,
		target: tg::Target,
		update_sender: NodeUpdateSender,
	) -> tg::Result<()> {
		Ok(())
	}

	async fn handle_template(
		handle: &H,
		template: tg::Template,
		update_sender: NodeUpdateSender,
	) -> tg::Result<()> {
		Ok(())
	}

	async fn handle_value(
		handle: &H,
		value: tg::Value,
		update_sender: NodeUpdateSender,
	) -> tg::Result<()> {
		match value {
			tg::Value::Array(array) => {
				Self::handle_array(handle, array, update_sender).await?;
			},
			tg::Value::Map(map) => {
				Self::handle_map(handle, map, update_sender).await?;
			},
			tg::Value::Object(object) => {
				Self::handle_object(handle, object, update_sender).await?;
			},
			tg::Value::Mutation(mutation) => {
				Self::handle_mutation(handle, mutation, update_sender).await?;
			},
			tg::Value::Template(template) => {
				Self::handle_template(handle, template, update_sender).await?;
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
		let (update_sender, update_receiver) = std::sync::mpsc::channel();
		let (ready_sender, _) = tokio::sync::watch::channel(false);
		let title = Self::item_title(&item);
		let root = Rc::new(RefCell::new(Node {
			children: Vec::new(),
			expanded: false,
			indicator: None,
			item,
			label: None,
			parent: None,
			ready_sender,
			task: None,
			title,
			update_receiver,
			update_sender,
		}));
		let roots = vec![root.clone()];
		let selected = root.clone();
		Self {
			handle: handle.clone(),
			options,
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

	async fn task(
		handle: &H,
		item: Item,
		update_sender: NodeUpdateSender,
		ready_sender: tokio::sync::watch::Sender<bool>,
	) {
		let result = match item {
			Item::Build(build) => Self::handle_build(handle, build, update_sender).await,
			Item::Value(value) => Self::handle_value(handle, value, update_sender).await,
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
		if let Some(task) = self.task.take() {
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
