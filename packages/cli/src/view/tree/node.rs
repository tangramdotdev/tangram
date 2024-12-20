use super::{provider::Provider, Options};
use futures::{future, stream::FuturesUnordered, StreamExt as _};
use num::ToPrimitive as _;
use ratatui::{self as tui, prelude::*};
use std::{
	pin,
	sync::{Arc, RwLock, Weak},
};
use tangram_client as tg;
use tangram_futures::task::Task;

pub struct Node<H> {
	pub options: super::Options,
	pub provider: super::Provider,

	pub build_children: Option<Vec<Arc<RwLock<Self>>>>,
	pub indicator: Option<Indicator>,
	pub log: Option<String>,
	pub object_children: Option<Vec<Arc<RwLock<Self>>>>,
	pub title: Option<String>,

	pub is_root: bool,
	pub parent: Option<Weak<RwLock<Self>>>,
	pub selected: bool,

	pub build_children_task: Option<Task<()>>,
	pub log_task: Option<Task<()>>,
	pub object_children_task: Option<Task<()>>,
	pub status_task: Option<Task<()>>,
	pub title_task: Option<Task<()>>,
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

impl<H> Node<H>
where
	H: tg::Handle,
{
	pub fn ancestors(self_: &Arc<RwLock<Self>>) -> Vec<Arc<RwLock<Self>>> {
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

	pub fn collapse_build_children(&mut self) {
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

	pub fn collapse_object_children(&mut self) {
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

	pub fn expand(self_: &Arc<RwLock<Self>>, options: Options) {
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

	pub fn is_collapsed(&self) -> bool {
		!self.options.builds && !self.options.objects
	}

	pub fn is_last_child(self_: &Arc<RwLock<Self>>) -> bool {
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

	pub fn new(
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
		Self::spawn_title_task(&node);
		Self::spawn_status_task(&node);
		Self::spawn_log_task(&node);
		if options.builds {
			Self::spawn_build_children_task(&node);
		}
		if options.objects {
			Self::spawn_object_children_task(&node);
		}
		node
	}

	pub fn parent(&self) -> Option<Arc<RwLock<Self>>> {
		self.parent.as_ref().and_then(Weak::upgrade)
	}

	pub fn render(self_: &Arc<RwLock<Self>>, area: Rect, buf: &mut Buffer) {
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
		tui::widgets::Paragraph::new(line).render(area, buf);
	}

	fn spawn_build_children_task(self_: &Arc<RwLock<Self>>) {
		let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
		let future = {
			let mut self_ = self_.write().unwrap();
			let Some(build_children) = &mut self_.provider.build_children else {
				return;
			};
			build_children(sender)
		};
		let task = Task::spawn(|stop| {
			let subtask = Task::spawn(|_| future);
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

	fn spawn_log_task(self_: &Arc<RwLock<Self>>) {
		let (sender, mut receiver) = tokio::sync::watch::channel(String::new());

		let future = {
			let mut self_ = self_.write().unwrap();
			let Some(log) = &mut self_.provider.log else {
				return;
			};
			log(sender)
		};

		let subtask = Task::spawn(|_| future);
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

	fn spawn_object_children_task(self_: &Arc<RwLock<Self>>) {
		let future = {
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
				let children = future
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

	fn spawn_status_task(self_: &Arc<RwLock<Self>>) {
		let (sender, mut receiver) = tokio::sync::watch::channel(Indicator::Created);
		let future = {
			let mut self_ = self_.write().unwrap();
			let Some(status) = &mut self_.provider.status else {
				return;
			};
			status(sender)
		};

		let subtask = Task::spawn(|_| future);
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

	fn spawn_title_task(self_: &Arc<RwLock<Self>>) {
		let future = {
			let Some(title) = &mut self_.write().unwrap().provider.title else {
				return;
			};
			title(())
		};

		let task = Task::spawn(|stop| {
			let self_ = self_.clone();
			async move {
				let stop = pin::pin!(stop.wait());
				if let future::Either::Right((title, _)) = future::select(stop, future).await {
					self_.write().unwrap().title.replace(title);
				}
			}
		});

		self_.write().unwrap().title_task.replace(task);
	}

	pub fn stop(&self) {
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

	pub async fn wait(self_: Arc<RwLock<Self>>) {
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
