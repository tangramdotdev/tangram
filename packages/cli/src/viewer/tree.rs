use super::{Item, Options, data, log::Log};
use copypasta::ClipboardProvider as _;
use crossterm as ct;
use futures::{TryStreamExt as _, future};
use num::ToPrimitive as _;
use ratatui::{self as tui, prelude::*};
use std::{
	cell::RefCell,
	collections::BTreeMap,
	pin::pin,
	rc::{Rc, Weak},
};
use tangram_client::{self as tg, prelude::*};
use tangram_either::Either;
use tangram_futures::task::Task;

const SPINNER: [char; 10] = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];

pub struct Tree<H> {
	handle: H,
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
	depth: usize,
	expand_task: Option<Task<()>>,
	expanded: Option<bool>,
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

pub struct Display {
	title: String,
	children: Vec<Self>,
}

impl<H> Tree<H>
where
	H: tg::Handle,
{
	fn yank(&self) {
		let Some(referent) = self.selected.borrow().referent.clone() else {
			return;
		};
		let contents = match &referent.item {
			Item::Process(process) => process.id().to_string(),
			Item::Value(value) => {
				if let tg::Value::Object(object) = value {
					Self::object_id(object)
				} else {
					let options = tg::value::print::Options {
						depth: 0,
						style: tg::value::print::Style::Compact,
					};
					value.print(options)
				}
			},
		};
		let Ok(mut context) = copypasta::ClipboardContext::new() else {
			return;
		};
		context.set_contents(contents).ok();
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

	fn select(&mut self, node: Rc<RefCell<Node>>) {
		self.selected = node.clone();
		let Some(referent) = node.borrow().referent.clone() else {
			return;
		};
		let handle = self.handle.clone();
		let data = self.data.clone();
		let viewer = self.viewer.clone();
		let task = Task::spawn_local(|_| async move {
			// Update the log view if the selected item is a process.
			let process =
				node.borrow()
					.referent
					.as_ref()
					.and_then(|referent| match &referent.item {
						Item::Process(process) => Some(process.clone()),
						Item::Value(_) => None,
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
								serde_json::to_string_pretty(&output.data).map_err(|source| {
									tg::error!(!source, "failed to serialize the process data")
								})
							})
							.unwrap_or_else(|error| error.to_string()),
						Item::Value(tg::Value::Object(tg::Object::Blob(blob))) => {
							super::blob::format(&handle, &blob)
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
								depth: 1,
								style: tg::value::print::Style::Pretty { indentation: "  " },
							};
							value.print(options)
						},
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
		});

		if let Some(task) = self.selected_task.replace(task) {
			task.abort();
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
		let depth = parent.borrow().depth + 1;
		let (update_sender, update_receiver) = std::sync::mpsc::channel();
		let options = parent.borrow().options.clone();
		let parent = Rc::downgrade(parent);
		let title = referent
			.as_ref()
			.map_or(String::new(), |referent| Self::item_title(&referent.item));

		let expand_task = match (&referent, options.expand_on_create) {
			(Some(referent), true) => {
				let handle = handle.clone();
				let referent = referent.clone();
				let update_sender = update_sender.clone();
				let task = Task::spawn_local(|_| async move {
					Self::expand_task(&handle, referent, update_sender).await;
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
			) => Some(false),
			_ => None,
		};

		Rc::new(RefCell::new(Node {
			children: Vec::new(),
			depth,
			expand_task,
			expanded,
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
		Self::display_node(self.roots.last().unwrap(), now)
	}

	fn display_node(node: &Rc<RefCell<Node>>, now: u128) -> Display {
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
			let handle = self.handle.clone();
			let update_sender = node.update_sender.clone();
			let referent = referent.clone();
			move |_| async move { Self::expand_task(&handle, referent, update_sender).await }
		});
		node.expand_task.replace(children_task);
		node.expanded.replace(true);
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

	async fn expand_array(
		handle: &H,
		array: tg::value::Array,
		update_sender: NodeUpdateSender,
	) -> tg::Result<()> {
		let handle = handle.clone();
		let update = move |node: Rc<RefCell<Node>>| {
			for value in array {
				let item = tg::Referent::with_item(Item::Value(value));
				let child = Self::create_node(&handle, &node, None, Some(item));
				node.borrow_mut().children.push(child);
			}
		};
		update_sender.send(Box::new(update)).unwrap();
		Ok(())
	}

	async fn expand_blob(
		handle: &H,
		blob: tg::Blob,
		update_sender: NodeUpdateSender,
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
		};
		update_sender.send(Box::new(update)).unwrap();
		Ok(())
	}

	async fn process_update_task(
		handle: &H,
		process: &tg::Referent<tg::Process>,
		options: &Options,
		update_sender: NodeUpdateSender,
	) -> tg::Result<()>
	where
		H: tg::Handle,
	{
		if let Some(title) = Self::process_title(handle, process).await {
			update_sender
				.send(Box::new(|node| {
					node.borrow_mut().title = title;
				}))
				.unwrap();
		}

		// Create the status stream.
		let mut status = process.item.status(handle).await?;
		while let Some(status) = status.try_next().await? {
			let indicator = match status {
				tg::process::Status::Created => Indicator::Created,
				tg::process::Status::Enqueued => Indicator::Enqueued,
				tg::process::Status::Dequeued => Indicator::Dequeued,
				tg::process::Status::Started => Indicator::Started,
				tg::process::Status::Finished => {
					// Remove the child if necessary.
					if options.condensed_processes {
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
		if handle
			.try_get_process(process.item.id())
			.await?
			.and_then(|output| output.data.error)
			.is_some_and(|error| matches!(error.code, Some(tg::error::Code::Cancelation)))
		{
			let update = move |node: Rc<RefCell<Node>>| {
				node.borrow_mut().indicator.replace(Indicator::Canceled);
			};
			update_sender.send(Box::new(update)).ok();
		}

		Ok(())
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

	async fn expand_process(
		handle: &H,
		referent: tg::Referent<tg::Process>,
		update_sender: NodeUpdateSender,
	) -> tg::Result<()> {
		let process = referent.item.clone();

		// Create the log task.
		let log_task = Task::spawn_local({
			let process = process.clone();
			let handle = handle.clone();
			let update_sender = update_sender.clone();
			|_| async move {
				Self::process_log_task(&handle, process, update_sender)
					.await
					.ok();
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
				Box::new(move |node| {
					if !node.borrow().options.condensed_processes {
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

		// Get the dependencies of the parent executable, if they exist.
		let dependencies = if let Some(file) = referent
			.item
			.command(handle)
			.await?
			.executable(handle)
			.await?
			.try_unwrap_module_ref()
			.ok()
			.and_then(|module| {
				module
					.module
					.referent
					.item
					.try_unwrap_object_ref()
					.ok()?
					.try_unwrap_file_ref()
					.ok()
			}) {
			file.dependencies(handle)
				.await?
				.into_values()
				.map(|referent| {
					let tg::Referent { item, path, tag } = referent;
					let id = item.id();
					tg::Referent {
						item: id,
						path,
						tag,
					}
				})
				.collect()
		} else {
			Vec::default()
		};

		// Create the children stream.
		let mut children = process
			.children(handle, tg::process::children::get::Arg::default())
			.await?;
		while let Some(process) = children.try_next().await? {
			// Get the executable of the child process.
			let executable = process
				.load(handle)
				.await?
				.command
				.executable(handle)
				.await?
				.clone();

			// Get the ID of the executable, if it exists.
			let dependency_id = match executable {
				tg::command::Executable::Artifact(artifact) => Some(artifact.artifact.id().into()),
				tg::command::Executable::Module(tg::command::ModuleExecutable {
					module:
						tg::Module {
							referent:
								tg::Referent {
									item: tg::module::Item::Object(object),
									..
								},
							..
						},
					..
				}) => Some(object.id()),
				_ => None,
			};

			// Lookup the path, subpath, and tag of the executable from the referrers' dependencies.
			let (path, tag) = if let Some(id) = dependency_id {
				dependencies
					.iter()
					.find_map(|referent| {
						(referent.item == id).then(|| (referent.path.clone(), referent.tag.clone()))
					})
					.unwrap_or((None, None))
			} else {
				(None, None)
			};

			// Inherit the parent's path/tag if necessary.
			let path = path.or_else(|| referent.path.clone());
			let tag = tag.or_else(|| referent.tag.clone());

			// Check the status of the process.
			let finished = process
				.status(handle)
				.await?
				.try_next()
				.await?
				.is_none_or(|status| matches!(status, tg::process::Status::Finished));

			// Post the update.
			let handle = handle.clone();
			let update = move |node: Rc<RefCell<Node>>| {
				if node.borrow().options.condensed_processes && finished {
					return;
				}
				if !node.borrow().options.condensed_processes
					&& node
						.borrow()
						.children
						.first()
						.is_none_or(|node| node.borrow().label.as_deref() != Some("children"))
				{
					let child =
						Self::create_node(&handle, &node, Some("children".to_owned()), None);
					node.borrow_mut().children.insert(0, child);
				}

				let parent = if node.borrow().options.condensed_processes {
					node
				} else {
					node.borrow().children[0].clone()
				};

				// Create the child.
				let referent = tg::Referent {
					item: Item::Process(process.clone()),
					path: path.clone(),
					tag: tag.clone(),
				};
				let child = Self::create_node(&handle, &parent, None, Some(referent));

				// Create the update task.
				let update_task = Task::spawn_local({
					let options = child.borrow().options.clone();
					let update_sender = child.borrow().update_sender.clone();
					let process = tg::Referent {
						item: process.clone(),
						path,
						tag,
					};
					|_| async move {
						Self::process_update_task(
							&handle,
							&process,
							options.as_ref(),
							update_sender,
						)
						.await
						.ok();
					}
				});
				child.borrow_mut().update_task.replace(update_task);

				// Add the child to the children node.
				parent.borrow_mut().children.push(child);
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

	async fn process_title(handle: &H, process: &tg::Referent<tg::Process>) -> Option<String> {
		// Get the original commands' executable.
		let command = process.item.command(handle).await.ok()?.clone();
		let executable = command.executable(handle).await.ok()?.clone();

		// Handle paths.
		if let Ok(path) = executable.try_unwrap_path_ref() {
			return Some(path.path.display().to_string());
		}

		// Use the referent if its fields are set.
		let title = match (&process.path, &process.tag) {
			(Some(path), None) => path.display().to_string(),
			(Some(subpath), Some(tag)) => format!("{tag}:{}", subpath.display()),
			(None, Some(tag)) => tag.to_string(),
			_ => {
				if let Some(object) = executable.object().first() {
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

	async fn expand_directory(
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
		let handle = handle.clone();
		let update = move |node: Rc<RefCell<Node>>| {
			for (name, child) in children {
				let item = tg::Referent::with_item(Item::Value(child));
				let child = Self::create_node(&handle, &node, Some(name), Some(item));
				node.borrow_mut().children.push(child);
			}
		};
		update_sender.send(Box::new(update)).unwrap();
		Ok(())
	}

	async fn expand_file(
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
						if let Some(tag) = referent.tag {
							map.insert("tag".to_owned(), tg::Value::String(tag.to_string()));
						}
						(reference.to_string(), tg::Value::Map(map))
					})
					.collect::<BTreeMap<_, _>>();
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
		let handle = handle.clone();
		let value = tg::Value::Array(nodes);
		let update = move |node: Rc<RefCell<Node>>| {
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
	) -> tg::Result<()> {
		let handle = handle.clone();
		let update = move |node: Rc<RefCell<Node>>| {
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
	) -> tg::Result<()> {
		match object {
			tg::Object::Blob(blob) => {
				Self::expand_blob(handle, blob, update_sender).await?;
			},
			tg::Object::Directory(directory) => {
				Self::expand_directory(handle, directory, update_sender).await?;
			},
			tg::Object::File(file) => {
				Self::expand_file(handle, file, update_sender).await?;
			},
			tg::Object::Symlink(symlink) => {
				Self::expand_symlink(handle, symlink, update_sender).await?;
			},
			tg::Object::Graph(graph) => {
				Self::expand_graph(handle, graph, update_sender).await?;
			},
			tg::Object::Command(command) => {
				Self::expand_command(handle, command, update_sender).await?;
			},
		}
		Ok(())
	}

	async fn expand_symlink(
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
		let handle = handle.clone();
		let update = move |node: Rc<RefCell<Node>>| {
			for (name, child) in children {
				let item = tg::Referent::with_item(Item::Value(child));
				let child = Self::create_node(&handle, &node, Some(name), Some(item));
				node.borrow_mut().children.push(child);
			}
		};
		update_sender.send(Box::new(update)).unwrap();

		Ok(())
	}

	async fn expand_command(
		handle: &H,
		command: tg::Command,
		update_sender: NodeUpdateSender,
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
				if let Some(subpath) = &executable.subpath {
					let subpath = subpath.to_string_lossy().to_string();
					map.insert("subpath".to_owned(), tg::Value::String(subpath));
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
						tg::module::Item::Path(path) => {
							tg::Value::String(path.to_string_lossy().into_owned())
						},
						tg::module::Item::Object(object) => tg::Value::Object(object),
					},
				);
				if let Some(path) = &executable.module.referent.path {
					let path = path.to_string_lossy().to_string();
					referent.insert("path".to_owned(), tg::Value::String(path));
				}
				if let Some(tag) = &executable.module.referent.tag {
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
		};
		update_sender.send(Box::new(update)).unwrap();
		Ok(())
	}

	async fn expand_template(
		handle: &H,
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
		let handle = handle.clone();
		let update = move |node: Rc<RefCell<Node>>| {
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
			Item::Process(_) => String::new(),
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
			},
		}
	}

	fn object_id(object: &tg::Object) -> String {
		match object {
			tg::Object::Blob(blob) => blob
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
			tg::Object::Command(command) => command
				.state()
				.read()
				.unwrap()
				.id
				.as_ref()
				.map_or_else(|| "object".to_owned(), ToString::to_string),
		}
	}

	pub fn new(
		handle: &H,
		referent: tg::Referent<Item>,
		options: Options,
		data: data::UpdateSender,
		viewer: super::UpdateSender<H>,
	) -> Self {
		let options = Rc::new(options);
		let (update_sender, update_receiver) = std::sync::mpsc::channel();
		let title = Self::item_title(&referent.item);
		let expand_task = if options.expand_on_create {
			let handle = handle.clone();
			let referent = referent.clone();
			let update_sender = update_sender.clone();
			let task: Task<()> = Task::spawn_local(|_| async move {
				Self::expand_task(&handle, referent, update_sender).await;
			});
			Some(task)
		} else {
			None
		};

		let update_task = if let Item::Process(process) = &referent.item {
			// Create the update task.
			let update_task = Task::spawn_local({
				let process = tg::Referent {
					item: process.clone(),
					path: referent.path.clone(),
					tag: referent.tag.clone(),
				};
				let handle = handle.clone();
				let options = options.clone();
				let update_sender = update_sender.clone();
				|_| async move {
					Self::process_update_task(&handle, &process, options.as_ref(), update_sender)
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
			depth: 1,
			expanded: Some(expand_task.is_some()),
			expand_task,
			indicator: None,
			referent: Some(referent),
			label: None,
			log_task: None,
			options: options.clone(),
			parent: None,
			title,
			update_receiver,
			update_sender,
			update_task,
		}));
		let roots = vec![root.clone()];
		let mut tree = Self {
			handle: handle.clone(),
			data,
			rect: None,
			roots,
			scroll: 0,
			selected: root.clone(),
			selected_task: None,
			viewer,
		};
		tree.select(root);
		tree
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

	pub fn render(&mut self, rect: Rect, buffer: &mut Buffer) {
		use tui::style::Stylize;

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

	async fn expand_task(
		handle: &H,
		referent: tg::Referent<Item>,
		update_sender: NodeUpdateSender,
	) {
		let tg::Referent { item, path, tag } = referent;
		let result = match item {
			Item::Process(process) => {
				let referent = tg::Referent {
					item: process,
					path,
					tag,
				};
				Self::expand_process(handle, referent, update_sender.clone()).await
			},
			Item::Value(value) => Self::expand_value(handle, value, update_sender.clone()).await,
		};
		if let Err(error) = result {
			let update = move |node: Rc<RefCell<Node>>| {
				node.borrow_mut().indicator.replace(Indicator::Error);
				node.borrow_mut().title = error.to_string();
			};
			update_sender.send(Box::new(update)).ok();
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
