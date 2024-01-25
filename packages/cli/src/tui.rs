use crossterm as ct;
use either::Either;
use futures::{stream::FusedStream, StreamExt, TryStreamExt};
use num::ToPrimitive;
use ratatui as tui;
use std::{
	collections::VecDeque,
	sync::{Arc, Weak},
};
use tangram_client as tg;
use tangram_error::{Result, WrapErr};
use tui::{layout::Rect, style::Stylize, widgets::Widget};

pub struct Tui {
	#[allow(dead_code)]
	options: Options,
	stop: tokio::sync::watch::Sender<bool>,
	task: Option<tokio::task::JoinHandle<std::io::Result<Terminal>>>,
}

type Backend = tui::backend::CrosstermBackend<std::fs::File>;

type Terminal = tui::Terminal<Backend>;

struct App {
	direction: tui::layout::Direction,
	layout: tui::layout::Layout,
	log: Log,
	tg: Box<dyn tg::Handle>,
	tree: Tree,
}

struct Tree {
	rect: tui::layout::Rect,
	root: TreeItem,
	scroll: usize,
	selected: TreeItem,
}

#[derive(Clone)]
struct TreeItem {
	inner: Arc<TreeItemInner>,
}

struct TreeItemInner {
	build: tg::Build,
	children_task: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
	index: usize,
	indicator_task: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
	parent: Option<Weak<TreeItemInner>>,
	state: std::sync::Mutex<TreeItemState>,
	tg: Box<dyn tg::Handle>,
	title_task: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

struct TreeItemState {
	children: Option<Vec<TreeItem>>,
	expanded: bool,
	indicator: TreeItemIndicator,
	selected: bool,
	title: Option<String>,
}

enum TreeItemIndicator {
	Errored,
	Queued,
	Running,
	Terminated,
	Canceled,
	Failed,
	Succeeded,
}

#[derive(Clone)]
struct Log {
	inner: Arc<LogInner>,
}

struct LogInner {
	// The build we're logging.
	build: tg::build::Build,

	// The current contents of the log.
	chunks: tokio::sync::Mutex<Vec<tg::build::log::Chunk>>,

	// The bounding box of the log view.
	rect: tokio::sync::watch::Sender<Rect>,

	// Channel used to send UI events.
	sender: tokio::sync::mpsc::UnboundedSender<LogUpdate>,

	// The lines of text that will be displayed.
	lines: std::sync::Mutex<Vec<String>>,

	// The log's task
	task: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,

	// The client.
	tg: Box<dyn tg::Handle>,
}

// Represents the current state of log's scroll, pointing to a newline character within a log chunk.
#[derive(Clone, Debug)]
struct Scroll {
	width: usize,
	chunk: usize,
	byte: usize,
	cursor: unicode_segmentation::GraphemeCursor,
}

enum LogUpdate {
	Up,
	Down,
}

const SPINNER: [char; 10] = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];

#[derive(Clone, Debug, Default)]
pub struct Options {
	pub exit: bool,
}

impl Tui {
	pub async fn start(tg: &dyn tg::Handle, build: &tg::Build, options: Options) -> Result<Self> {
		// Create the terminal.
		let tty = tokio::fs::OpenOptions::new()
			.read(true)
			.write(true)
			.open("/dev/tty")
			.await
			.wrap_err("Failed to open /dev/tty.")?;
		let tty = tty.into_std().await;
		let backend = Backend::new(tty);
		let mut terminal =
			Terminal::new(backend).wrap_err("Failed to create the terminal backend.")?;
		ct::terminal::enable_raw_mode().wrap_err("Failed to enable the terminal's raw mode")?;
		ct::execute!(
			terminal.backend_mut(),
			ct::event::EnableMouseCapture,
			ct::terminal::EnterAlternateScreen,
		)
		.wrap_err("Failed to setup the terminal.")?;

		// Create the stop flag.
		let (stop, _) = tokio::sync::watch::channel(false);

		// Spawn the task.
		let task = tokio::task::spawn_blocking({
			let tg = tg.clone_box();
			let build = build.clone();
			let stop = stop.subscribe();
			move || {
				// Create the app.
				let rect = terminal.get_frame().size();
				let mut app = App::new(tg.as_ref(), &build, rect);

				// Run the event loop.
				while !*stop.borrow() {
					// Wait for and handle an event.
					if ct::event::poll(std::time::Duration::from_millis(16))? {
						let event = ct::event::read()?;

						// Quit the TUI if requested.
						if let ct::event::Event::Key(event) = event {
							if options.exit
								&& (event.code == ct::event::KeyCode::Char('q')
									|| (event.code == ct::event::KeyCode::Char('c')
										&& event.modifiers == ct::event::KeyModifiers::CONTROL))
							{
								break;
							}
						}

						// Handle the event.
						app.event(&event);
					}

					// Render.
					terminal.draw(|frame| app.render(frame.size(), frame.buffer_mut()))?;
				}

				Ok(terminal)
			}
		});

		Ok(Self {
			options,
			stop,
			task: Some(task),
		})
	}

	pub fn stop(&self) {
		self.stop.send_replace(true);
	}

	pub async fn join(mut self) -> Result<()> {
		// Get the task.
		let Some(task) = self.task.take() else {
			return Ok(());
		};

		// Join the task and get the terminal.
		let mut terminal = task.await.unwrap().wrap_err("The task did not succeed.")?;

		// Reset the terminal.
		terminal.clear().wrap_err("Failed to clear the terminal.")?;
		ct::execute!(
			terminal.backend_mut(),
			ct::event::DisableMouseCapture,
			ct::terminal::LeaveAlternateScreen
		)
		.wrap_err("Failed to reset the terminal.")?;
		ct::terminal::disable_raw_mode().wrap_err("Failed to disable the terminal's raw mode.")?;

		Ok(())
	}
}

impl App {
	fn new(tg: &dyn tg::Handle, build: &tg::Build, rect: tui::layout::Rect) -> Self {
		let tg = tg.clone_box();
		let direction = tui::layout::Direction::Horizontal;
		let layout = tui::layout::Layout::default()
			.direction(direction)
			.margin(0)
			.constraints([
				tui::layout::Constraint::Percentage(50),
				tui::layout::Constraint::Length(1),
				tui::layout::Constraint::Min(1),
			]);
		let layouts = layout.split(rect);
		let log = Log::new(tg.as_ref(), build, layouts[2]);
		let root = TreeItem::new(tg.as_ref(), build, None, 0, true);
		root.expand();
		let tree = Tree::new(root, layouts[0]);
		Self {
			direction,
			layout,
			log,
			tg,
			tree,
		}
	}

	fn event(&mut self, event: &ct::event::Event) {
		match event {
			ct::event::Event::Key(event) => self.key(*event),
			ct::event::Event::Mouse(event) => self.mouse(*event),
			ct::event::Event::Resize(width, height) => {
				self.resize(tui::layout::Rect::new(0, 0, *width, *height));
			},
			_ => (),
		}
	}

	fn key(&mut self, event: ct::event::KeyEvent) {
		match event.code {
			ct::event::KeyCode::Char('c')
				if event.modifiers == ct::event::KeyModifiers::CONTROL =>
			{
				self.quit();
			},
			ct::event::KeyCode::Char('c') => {
				self.cancel();
			},
			ct::event::KeyCode::Left | ct::event::KeyCode::Char('h') => {
				self.collapse();
			},
			ct::event::KeyCode::Down | ct::event::KeyCode::Char('j') => {
				self.down();
			},
			ct::event::KeyCode::Up | ct::event::KeyCode::Char('k') => {
				self.up();
			},
			ct::event::KeyCode::Right | ct::event::KeyCode::Char('l') => {
				self.expand();
			},
			ct::event::KeyCode::Char('q') => {
				self.quit();
			},
			ct::event::KeyCode::Char('r') => {
				self.rotate();
			},
			ct::event::KeyCode::Char('[') => {
				self.log.up();
			},
			ct::event::KeyCode::Char(']') => {
				self.log.down();
			},
			_ => (),
		}
	}

	fn mouse(&mut self, event: ct::event::MouseEvent) {
		match event.kind {
			ct::event::MouseEventKind::ScrollDown => {
				self.log.down();
			},
			ct::event::MouseEventKind::ScrollUp => {
				self.log.up();
			},
			_ => (),
		}
	}

	fn resize(&mut self, rect: tui::layout::Rect) {
		let rects = self.layout.split(rect);
		self.log.resize(rects[2]);
	}

	fn down(&mut self) {
		self.select(true);
	}

	fn up(&mut self) {
		self.select(false);
	}

	fn select(&mut self, down: bool) {
		let expanded_items = self.tree.expanded_items();
		let previous_selected_index = expanded_items
			.iter()
			.position(|item| Arc::ptr_eq(&item.inner, &self.tree.selected.inner))
			.unwrap();
		let new_selected_index = if down {
			(previous_selected_index + 1).min(expanded_items.len() - 1)
		} else {
			previous_selected_index.saturating_sub(1)
		};
		let height = self.tree.rect.height.to_usize().unwrap();
		if new_selected_index < self.tree.scroll {
			self.tree.scroll -= 1;
		} else if new_selected_index >= self.tree.scroll + height {
			self.tree.scroll += 1;
		}
		let new_selected_item = expanded_items[new_selected_index].clone();
		self.tree.selected.inner.state.lock().unwrap().selected = false;
		new_selected_item.inner.state.lock().unwrap().selected = true;
		self.log = Log::new(
			self.tg.as_ref(),
			&new_selected_item.inner.build,
			self.log.rect(),
		);
		self.tree.selected = new_selected_item;
	}

	fn expand(&mut self) {
		self.tree.selected.expand();
	}

	fn collapse(&mut self) {
		if self.tree.selected.inner.state.lock().unwrap().expanded {
			self.tree.selected.collapse();
		} else {
			let parent = self
				.tree
				.selected
				.inner
				.parent
				.as_ref()
				.map(|parent| TreeItem {
					inner: parent.upgrade().unwrap(),
				});
			if let Some(parent) = parent {
				self.tree.selected.inner.state.lock().unwrap().selected = false;
				parent.inner.state.lock().unwrap().selected = true;
				self.log = Log::new(self.tg.as_ref(), &parent.inner.build, self.log.rect());
				self.tree.selected = parent;
			}
		}
	}

	fn rotate(&mut self) {
		self.direction = match self.direction {
			tui::layout::Direction::Horizontal => tui::layout::Direction::Vertical,
			tui::layout::Direction::Vertical => tui::layout::Direction::Horizontal,
		}
	}

	fn cancel(&mut self) {
		let build = self.tree.selected.inner.build.clone();
		let tg = self.tg.clone_box();
		tokio::spawn(async move { build.cancel(tg.as_ref()).await.ok() });
	}

	fn quit(&mut self) {
		let build = self.tree.root.inner.build.clone();
		let tg = self.tg.clone_box();
		tokio::spawn(async move { build.cancel(tg.as_ref()).await.ok() });
	}

	fn render(&self, rect: tui::layout::Rect, buf: &mut tui::buffer::Buffer) {
		let rects = self.layout.split(rect);

		self.tree.render(rects[0], buf);

		let borders = match self.direction {
			tui::layout::Direction::Horizontal => tui::widgets::Borders::LEFT,
			tui::layout::Direction::Vertical => tui::widgets::Borders::BOTTOM,
		};
		let block = tui::widgets::Block::default().borders(borders);
		block.render(rects[1], buf);

		self.log.render(rects[2], buf);
	}
}

impl Tree {
	fn new(root: TreeItem, rect: tui::layout::Rect) -> Self {
		Self {
			rect,
			root: root.clone(),
			scroll: 0,
			selected: root,
		}
	}

	fn expanded_items(&self) -> Vec<TreeItem> {
		let mut items = Vec::new();
		let mut stack = VecDeque::from(vec![self.root.clone()]);
		while let Some(item) = stack.pop_front() {
			items.push(item.clone());
			if item.inner.state.lock().unwrap().expanded {
				for child in item
					.inner
					.state
					.lock()
					.unwrap()
					.children
					.iter()
					.flatten()
					.rev()
				{
					stack.push_front(child.clone());
				}
			}
		}
		items
	}

	fn render(&self, rect: tui::layout::Rect, buf: &mut tui::buffer::Buffer) {
		let mut stack = VecDeque::from(vec![self.root.clone()]);
		let mut index = 0;
		while let Some(item) = stack.pop_front() {
			if item.inner.state.lock().unwrap().expanded {
				for child in item
					.inner
					.state
					.lock()
					.unwrap()
					.children
					.iter()
					.flatten()
					.rev()
				{
					stack.push_front(child.clone());
				}
			}
			if index >= self.scroll && index < self.scroll + rect.height.to_usize().unwrap() {
				let rect = tui::layout::Rect {
					x: rect.x,
					y: rect.y + (index - self.scroll).to_u16().unwrap(),
					width: rect.width,
					height: 1,
				};
				item.render(rect, buf);
			}
			index += 1;
		}
	}
}

impl TreeItem {
	fn new(
		tg: &dyn tg::Handle,
		build: &tg::Build,
		parent: Option<Weak<TreeItemInner>>,
		index: usize,
		selected: bool,
	) -> Self {
		let state = TreeItemState {
			children: None,
			expanded: false,
			indicator: TreeItemIndicator::Running,
			selected,
			title: None,
		};
		let inner = Arc::new(TreeItemInner {
			build: build.clone(),
			children_task: std::sync::Mutex::new(None),
			index,
			indicator_task: std::sync::Mutex::new(None),
			parent,
			state: std::sync::Mutex::new(state),
			tg: tg.clone_box(),
			title_task: std::sync::Mutex::new(None),
		});

		let item = Self { inner };

		let task = tokio::task::spawn({
			let item = item.clone();
			async move {
				let arg = tg::build::status::GetArg::default();
				let Ok(mut stream) = item.inner.build.status(item.inner.tg.as_ref(), arg).await
				else {
					item.inner.state.lock().unwrap().indicator = TreeItemIndicator::Errored;
					return;
				};
				loop {
					let Ok(status) = stream.try_next().await else {
						item.inner.state.lock().unwrap().indicator = TreeItemIndicator::Errored;
						return;
					};
					let Some(status) = status else {
						break;
					};
					match status {
						tg::build::Status::Queued => {
							item.inner.state.lock().unwrap().indicator = TreeItemIndicator::Queued;
						},
						tg::build::Status::Running => {
							item.inner.state.lock().unwrap().indicator = TreeItemIndicator::Running;
						},
						tg::build::Status::Finished => {
							break;
						},
					}
				}
				let Ok(outcome) = item.inner.build.outcome(item.inner.tg.as_ref()).await else {
					item.inner.state.lock().unwrap().indicator = TreeItemIndicator::Errored;
					return;
				};
				let indicator = match outcome {
					tg::build::Outcome::Terminated => TreeItemIndicator::Terminated,
					tg::build::Outcome::Canceled => TreeItemIndicator::Canceled,
					tg::build::Outcome::Failed(_) => TreeItemIndicator::Failed,
					tg::build::Outcome::Succeeded(_) => TreeItemIndicator::Succeeded,
				};
				item.inner.state.lock().unwrap().indicator = indicator;
			}
		});
		item.inner.indicator_task.lock().unwrap().replace(task);

		let task = tokio::task::spawn({
			let item = item.clone();
			async move {
				let title = title(item.inner.tg.as_ref(), &item.inner.build)
					.await
					.ok()
					.flatten();
				item.inner.state.lock().unwrap().title = title;
			}
		});
		item.inner.title_task.lock().unwrap().replace(task);

		item
	}

	fn ancestors(&self) -> Vec<TreeItem> {
		let mut ancestors = Vec::new();
		let mut parent = self.inner.parent.as_ref().map(|parent| TreeItem {
			inner: parent.upgrade().unwrap(),
		});
		while let Some(parent_) = parent {
			ancestors.push(parent_.clone());
			parent = parent_.inner.parent.as_ref().map(|parent| TreeItem {
				inner: parent.upgrade().unwrap(),
			});
		}
		ancestors
	}

	fn expand(&self) {
		self.inner.state.lock().unwrap().expanded = true;
		self.inner
			.state
			.lock()
			.unwrap()
			.children
			.replace(Vec::new());
		let children_task = tokio::task::spawn({
			let item = self.clone();
			async move {
				let Ok(mut children) = item
					.inner
					.build
					.children(
						item.inner.tg.as_ref(),
						tg::build::children::GetArg::default(),
					)
					.await
				else {
					return;
				};
				while let Some(Ok(child)) = children.next().await {
					let tg = item.inner.tg.clone_box();
					let parent = Some(Arc::downgrade(&item.inner));
					let index = item
						.inner
						.state
						.lock()
						.unwrap()
						.children
						.as_ref()
						.unwrap()
						.len();
					let selected = false;
					let child = TreeItem::new(tg.as_ref(), &child, parent, index, selected);
					item.inner
						.state
						.lock()
						.unwrap()
						.children
						.as_mut()
						.unwrap()
						.push(child);
				}
			}
		});
		self.inner
			.children_task
			.lock()
			.unwrap()
			.replace(children_task);
	}

	fn collapse(&self) {
		self.inner.state.lock().unwrap().expanded = false;
		self.inner.state.lock().unwrap().children.take();
		if let Some(children_task) = self.inner.children_task.lock().unwrap().take() {
			children_task.abort();
		}
	}

	fn render(&self, rect: tui::layout::Rect, buf: &mut tui::buffer::Buffer) {
		let mut prefix = String::new();
		for item in self.ancestors().iter().rev().skip(1) {
			let parent = item.inner.parent.clone().unwrap();
			let parent_children_count = parent
				.upgrade()
				.unwrap()
				.state
				.lock()
				.unwrap()
				.children
				.as_ref()
				.map_or(0, Vec::len);
			let last = item.inner.index == parent_children_count - 1;
			prefix.push_str(if last { "  " } else { "│ " });
		}
		if let Some(parent) = self.inner.parent.as_ref() {
			let parent_children_count = parent
				.upgrade()
				.unwrap()
				.state
				.lock()
				.unwrap()
				.children
				.as_ref()
				.map_or(0, Vec::len);
			let last = self.inner.index == parent_children_count - 1;
			prefix.push_str(if last { "└─" } else { "├─" });
		}
		let disclosure = if self.inner.state.lock().unwrap().expanded {
			"▼"
		} else {
			"▶"
		};
		let indicator = match self.inner.state.lock().unwrap().indicator {
			TreeItemIndicator::Errored => "!".red(),
			TreeItemIndicator::Queued => "⟳".yellow(),
			TreeItemIndicator::Running => {
				let now = std::time::SystemTime::now()
					.duration_since(std::time::UNIX_EPOCH)
					.unwrap()
					.as_millis();
				let position = (now / (1000 / 10)) % 10;
				let position = position.to_usize().unwrap();
				SPINNER[position].to_string().blue()
			},
			TreeItemIndicator::Terminated => "⦻".red(),
			TreeItemIndicator::Canceled => "⦻".yellow(),
			TreeItemIndicator::Failed => "✗".red(),
			TreeItemIndicator::Succeeded => "✓".green(),
		};
		let title = self
			.inner
			.state
			.lock()
			.unwrap()
			.title
			.clone()
			.unwrap_or_else(|| "<unknown>".to_owned());
		let title = tui::text::Line::from(vec![
			prefix.into(),
			disclosure.into(),
			" ".into(),
			indicator,
			" ".into(),
			title.into(),
		]);
		let style = if self.inner.state.lock().unwrap().selected {
			tui::style::Style::default()
				.bg(tui::style::Color::White)
				.fg(tui::style::Color::Black)
		} else {
			tui::style::Style::default()
		};
		let paragraph = tui::widgets::Paragraph::new(title).style(style);
		paragraph.render(rect, buf);
	}
}

impl Drop for TreeItemInner {
	fn drop(&mut self) {
		if let Some(task) = self.children_task.lock().unwrap().take() {
			task.abort();
		}
		if let Some(task) = self.indicator_task.lock().unwrap().take() {
			task.abort();
		}
		if let Some(task) = self.title_task.lock().unwrap().take() {
			task.abort();
		}
	}
}

async fn title(tg: &dyn tg::Handle, build: &tg::Build) -> Result<Option<String>> {
	// Get the target.
	let target = build.target(tg).await?;

	// Get the package.
	let Some(package) = target.package(tg).await? else {
		return Ok(None);
	};

	// Get the metadata.
	let metadata = tg::package::get_metadata(tg, package).await?;

	// Construct the title.
	let mut title = String::new();
	title.push_str(metadata.name.as_deref().unwrap_or("<unknown>"));
	if let Some(version) = &metadata.version {
		title.push_str(&format!("@{version}"));
	}
	if let Some(name) = target.name(tg).await? {
		title.push_str(&format!(":{name}"));
	}

	Ok(Some(title))
}

impl Drop for LogInner {
	fn drop(&mut self) {
		if let Some(task) = self.task.lock().unwrap().take() {
			task.abort();
		}
	}
}

impl Log {
	// Create a new log data stream.
	#[allow(clippy::too_many_lines)]
	fn new(tg: &dyn tg::Handle, build: &tg::Build, rect: Rect) -> Self {
		let build = build.clone();
		let chunks = tokio::sync::Mutex::new(Vec::new());
		let tg = tg.clone_box();

		let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
		let (rect, mut rect_watch) = tokio::sync::watch::channel(rect);
		let lines = std::sync::Mutex::new(Vec::new());

		let log = Log {
			inner: Arc::new(LogInner {
				build,
				chunks,
				lines,
				rect,
				sender,
				task: std::sync::Mutex::new(None),
				tg,
			}),
		};

		// Create the log task.
		let task = tokio::task::spawn({
			let log = log.clone();
			async move {
				let mut scroll = None;
				let area = log.rect().area().to_i64().unwrap();
				let arg = tg::build::log::GetArg {
					limit: Some(-3 * area / 2),
					offset: None,
					size: None,
					timeout: None,
				};
				let stream = log
					.inner
					.build
					.log(log.inner.tg.as_ref(), arg)
					.await
					.expect("Failed to get log stream.");

				let mut stream = Some(stream.fuse());
				loop {
					if let Some(stream_) = stream.as_mut() {
						tokio::select! {
							Some(chunk) = stream_.next(), if !stream_.is_terminated() => {
								let Ok(chunk) = chunk else {
									return;
								};
								log.add_chunk(chunk).await;
							},
							result = receiver.recv() => match result.unwrap() {
								LogUpdate::Down => {
									let result = log.down_impl(&mut scroll).await;
									if result.is_err() {
										return;
									}
									if scroll.is_none() && stream.is_none() {
										stream = None;
									}
								}
								LogUpdate::Up => {
									let result = log.up_impl(&mut scroll).await;
									if result.is_err() {
										return;
									}
								}
							},
							_ = rect_watch.changed() => {
								if let Some(scroll) = scroll.as_mut() {
									let rect = log.rect();
									scroll.width = rect.width.to_usize().unwrap();
								}
							}
						};
					} else {
						tokio::select! {
							result = receiver.recv() => match result.unwrap() {
								LogUpdate::Down => {
									let result = log.down_impl(&mut scroll).await;
									if result.is_err() {
										return;
									}
									if scroll.is_none() && stream.is_none() {
										stream = None;
									}
								}
								LogUpdate::Up => {
									let result = log.up_impl(&mut scroll).await;
									if result.is_err() {
										return;
									}
								}
							},
							_ = rect_watch.changed() => {
								if let Some(scroll) = scroll.as_mut() {
									let rect = log.rect();
									scroll.width = rect.width.to_usize().unwrap();
								}
							}
						}
						if scroll.is_none() {
							let arg = tg::build::log::GetArg {
								limit: Some(-3 * area / 2),
								offset: None,
								size: None,
								timeout: None,
							};
							let stream_ = log
								.inner
								.build
								.log(log.inner.tg.as_ref(), arg)
								.await
								.expect("Failed to get log stream.");
							stream = Some(stream_.fuse());
						}
					}
					log.update_lines(scroll.clone()).await;
				}
			}
		});
		log.inner.task.lock().unwrap().replace(task);
		log
	}

	// Log a new chunk from the stream.
	async fn add_chunk(&self, chunk: tg::build::log::Chunk) {
		// Get some metadata about this log chunk.
		let chunk_position = chunk.offset;
		let chunk_size = chunk.data.len().to_u64().unwrap();

		let mut chunks = self.inner.chunks.lock().await;

		// Short circuit for the common case that we're appending to the log.
		if chunks.is_empty() || chunks.last().unwrap().offset < chunk.offset {
			chunks.push(chunk);
			return;
		};

		// Find where this log chunk needs to be inserted.
		let index = chunks
			.iter()
			.position(|existing| existing.offset > chunk.offset)
			.unwrap_or(chunks.len() - 1);
		chunks.insert(index, chunk);

		// Check if we need to truncate the next chunk.
		let next_pos = chunk_position + chunk_size;
		let next_chunk = &mut chunks[index + 1];
		if next_pos > next_chunk.offset {
			let new_length =
				next_chunk.data.len() - (next_pos - next_chunk.offset).to_usize().unwrap();
			if new_length > 0 {
				next_chunk.data.truncate(new_length);
			} else {
				chunks.remove(index + 1);
			}
		}
	}

	// Handle a scroll up event.
	async fn up_impl(&self, scroll: &mut Option<Scroll>) -> Result<()> {
		let rect = self.rect();
		let height = rect.height.to_usize().unwrap();
		let width = rect.width.to_usize().unwrap();

		let Some(scroll) = scroll.as_mut() else {
			let chunks = self.inner.chunks.lock().await;
			if chunks.is_empty() {
				return Ok(());
			}
			let mut inner = Scroll::new(
				width,
				chunks.len() - 1,
				chunks.last().unwrap().data.len(),
				&chunks,
			);
			inner.scroll_up(height + 2, &chunks);
			scroll.replace(inner);
			return Ok(());
		};

		// Attempt to scroll up by one, and pull in new data if necessary.
		if scroll.scroll_up(2, &self.inner.chunks.lock().await) == 0 {
			let pos = self.inner.chunks.lock().await.first().unwrap().offset;
			if pos == 0 {
				return Ok(());
			}
			let len = 3 * self.rect().area().to_i64().unwrap() / 2;
			let arg = tg::build::log::GetArg {
				limit: Some(len),
				offset: Some(pos),
				size: None,
				timeout: None,
			};
			let new_chunks = self
				.inner
				.build
				.log(self.inner.tg.as_ref(), arg)
				.await?
				.try_collect::<Vec<_>>()
				.await?;
			for chunk in new_chunks {
				self.add_chunk(chunk).await;
			}

			let scroll_pos = pos + scroll.byte.to_u64().unwrap();
			let chunks = self.inner.chunks.lock().await;
			let chunk = chunks
				.iter()
				.position(|e| {
					e.offset <= scroll_pos
						&& (e.offset + e.data.len().to_u64().unwrap()) >= scroll_pos
				})
				.unwrap();
			let byte = (scroll_pos - chunks[chunk].offset).to_usize().unwrap();
			scroll.chunk = chunk;
			scroll.byte = byte;
			scroll.scroll_up(2, &chunks);
		}

		Ok(())
	}

	// Handle a scroll down event.
	async fn down_impl(&self, scroll: &mut Option<Scroll>) -> Result<()> {
		let chunks = self.inner.chunks.lock().await;
		let height = self.rect().height.to_usize().unwrap();
		let is_tailing = scroll.as_mut().map_or(true, |scroll| {
			scroll.scroll_down(1, &chunks);
			scroll.clone().scroll_down(height, &chunks) < height
		});
		if is_tailing {
			scroll.take();
		}
		Ok(())
	}

	// Update the rendered lines.
	async fn update_lines(&self, scroll: Option<Scroll>) {
		let chunks = self.inner.chunks.lock().await;
		if chunks.is_empty() {
			self.inner.lines.lock().unwrap().clear();
			return;
		}

		// Get enough lines to fill the rectangle
		let rect = self.rect();
		let height = rect.height.to_usize().unwrap();
		let width = rect.width.to_usize().unwrap();
		let scroll = scroll.unwrap_or_else(|| {
			let chunk = chunks.len() - 1;
			let byte = chunks.last().unwrap().data.len();
			let mut scroll = Scroll::new(width, chunk, byte, chunks.as_ref());
			scroll.scroll_up(height + 1, &chunks);
			scroll
		});

		let lines = scroll.read_lines(height + 1, &chunks);
		*self.inner.lines.lock().unwrap() = lines;
	}

	// Get the bounding box of the log widget.
	fn rect(&self) -> tui::layout::Rect {
		*self.inner.rect.borrow()
	}

	// Issue a scroll up event.
	fn up(&self) {
		self.inner.sender.send(LogUpdate::Up).ok();
	}

	// Issue a scroll down event.
	fn down(&self) {
		self.inner.sender.send(LogUpdate::Down).ok();
	}

	fn resize(&self, rect: Rect) {
		self.inner.rect.send(rect).ok();
	}

	fn render(&self, rect: tui::layout::Rect, buf: &mut tui::buffer::Buffer) {
		let lines = self.inner.lines.lock().unwrap();
		for (y, line) in (0..rect.height).zip(lines.iter()) {
			buf.set_line(rect.x, rect.y + y, &tui::text::Line::raw(line), rect.width);
		}
	}
}

impl Scroll {
	fn new(width: usize, chunk: usize, byte: usize, chunks: &[tg::build::log::Chunk]) -> Self {
		let offset = chunks[chunk].offset.to_usize().unwrap() + byte;
		let length = chunks
			.last()
			.map(|chunk| chunk.offset.to_usize().unwrap() + chunk.data.len())
			.unwrap();
		let cursor = unicode_segmentation::GraphemeCursor::new(offset, length, true);
		Self {
			width,
			chunk,
			byte,
			cursor,
		}
	}

	// Increment the scroll position by one UTF8 grapheme cluster and add the intermediate results to end of the buffer. Returns Some(true) if successful, Some(false) if additional pre-context is required, or `None` if we receive invalid UTF-8.
	#[allow(clippy::too_many_lines)]
	fn advance(
		&mut self,
		forward: bool,                    // Advance forward if true, backward if false.
		chunks: &[tg::build::log::Chunk], // The chunks to use as a corpus.
		buffer: &mut Vec<u8>,             // The output buffer to write to.
	) -> Option<bool> {
		let (old_chunk, old_byte) = (self.chunk, self.byte);
		loop {
			// Handle boundary conditions.
			if self.is_at_end(chunks) && forward {
				break;
			}

			// Get the current chunk and utf8 string at a current position.
			let (utf8_str, chunk_start) = self.get_utf8_str(chunks)?;
			let utf8: &str = utf8_str
				.as_ref()
				.map_left(|l| *l)
				.map_right(AsRef::<str>::as_ref)
				.either_into();

			// Advance the cursor by one grapheme in the desired direction.
			let result = if forward {
				self.cursor.next_boundary(utf8, chunk_start)
			} else {
				self.cursor.prev_boundary(utf8, chunk_start)
			};

			match result {
				Ok(Some(new_pos)) => {
					// Update the chunk if necessary.
					if new_pos < chunks[self.chunk].offset.to_usize().unwrap()
						|| new_pos
							>= chunks[self.chunk].offset.to_usize().unwrap()
								+ chunks[self.chunk].data.len()
					{
						self.chunk = chunks
							.iter()
							.enumerate()
							.find_map(|(idx, chunk)| {
								(new_pos >= chunk.offset.to_usize().unwrap()
									&& new_pos
										< chunk.offset.to_usize().unwrap() + chunk.data.len())
								.then_some(idx)
							})
							.unwrap_or(chunks.len() - 1);
					}
					self.byte = new_pos - chunks[self.chunk].offset.to_usize().unwrap();
					break;
				},
				Ok(None) => {
					if forward {
						self.byte = chunks[self.chunk].data.len();
					} else {
						self.byte = chunks[self.chunk].first_codepoint().unwrap();
					}
					break;
				},
				Err(unicode_segmentation::GraphemeIncomplete::NextChunk) => {
					debug_assert!(forward);
					let last_codepoint = chunks[self.chunk].last_codepoint().unwrap();
					if self.byte == last_codepoint {
						self.chunk += 1;
						self.byte = chunks[self.chunk].first_codepoint().unwrap();
					} else {
						self.byte = last_codepoint;
					}
				},
				Err(unicode_segmentation::GraphemeIncomplete::PrevChunk) => {
					debug_assert!(!forward);
					let first_codepoint = chunks[self.chunk].first_codepoint().unwrap();
					if self.byte == first_codepoint {
						if self.chunk == 0 {
							return Some(false);
						}
						self.chunk -= 1;
						self.byte = chunks[self.chunk].last_codepoint().unwrap();
					} else {
						self.byte = chunks[self.chunk].prev_codepoint(self.byte).unwrap();
					}
				},
				Err(unicode_segmentation::GraphemeIncomplete::PreContext(end)) => {
					let Some((string, start)) = self.get_pre_context(chunks, end) else {
						return Some(false);
					};
					self.cursor.provide_context(string, start);
				},
				_ => unreachable!(),
			}
		}

		// Append this grapheme to the end of the buffer, accounting for overlap.
		let (new_chunk, new_byte) = (self.chunk, self.byte);
		if forward {
			if new_chunk == old_chunk {
				let bytes = &chunks[new_chunk].data[old_byte..new_byte];
				buffer.extend_from_slice(bytes);
			} else {
				let bytes = &chunks[old_chunk].data[old_byte..];
				buffer.extend_from_slice(bytes);
				let bytes = &chunks[new_chunk].data[..new_byte];
				buffer.extend_from_slice(bytes);
			}
		} else {
			let mid = buffer.len();
			if new_chunk == old_chunk {
				let bytes = &chunks[new_chunk].data[new_byte..old_byte];
				buffer.extend_from_slice(bytes);
			} else {
				let bytes = &chunks[new_chunk].data[new_byte..];
				buffer.extend_from_slice(bytes);
				let bytes = &chunks[old_chunk].data[..old_byte];
				buffer.extend_from_slice(bytes);
			}
			buffer.rotate_left(mid);
		}
		Some(true)
	}

	fn is_at_end(&self, chunks: &[tg::build::log::Chunk]) -> bool {
		let chunk = &chunks[self.chunk];
		self.byte == chunk.data.len()
	}

	fn is_at_start(&self, chunks: &[tg::build::log::Chunk]) -> bool {
		self.chunk == 0 && chunks[self.chunk].first_codepoint() == Some(self.byte)
	}

	/// Scroll down by `height` num lines, wrapping on grapheme clusters, and return the number of lines that were scrolled.
	fn scroll_down(&mut self, height: usize, chunks: &[tg::build::log::Chunk]) -> usize {
		let mut buffer = Vec::with_capacity(3 * self.width / 2);

		// Advance the cursor by `height` number of lines, accounting for word wrap.
		let mut count = 0;
		let mut last_width = 0;
		while count < height {
			if self.is_at_end(chunks) {
				return count;
			}

			// Advance the cursor by width, or until we hit a newline.
			buffer.clear();
			let mut width = 0;
			let skip = loop {
				if self.advance(true, chunks, &mut buffer).is_none() {
					return count;
				}
				width += 1;
				if buffer.ends_with(b"\n") {
					break last_width == self.width && width == 1;
				} else if width == self.width {
					break false;
				}
			};

			// If we hit a new line, and the last line's width was equal to the word wrap size, then we have to skip this line.
			last_width = width;
			if !skip {
				count += 1;
			}
		}
		height
	}

	/// Scroll up by height num lines, wrapping on grapheme clusters, returning the number of lines scrolled.
	fn scroll_up(&mut self, height: usize, chunks: &[tg::build::log::Chunk]) -> usize {
		let mut buffer = Vec::with_capacity(3 * self.width / 2);
		let mut last_line_wrapped = false;
		for count in 0..height {
			buffer.clear();
			let mut width = 0;
			last_line_wrapped = loop {
				if matches!(self.advance(false, chunks, &mut buffer), Some(false) | None) {
					return count;
				}
				if buffer.starts_with(b"\n") || buffer.starts_with(b"\r\n") {
					break false;
				}
				width += 1;
				if width == self.width {
					break true;
				}
			};
			if self.is_at_start(chunks) {
				return count;
			}
		}

		// Make sure to advance by one grapheme cluster if the start of the last line was a newline character.
		// TODO: remove this allocation
		if !last_line_wrapped {
			let mut buffer = Vec::with_capacity(4);
			self.advance(true, chunks, &mut buffer);
		}

		height
	}

	fn read_lines(&self, count: usize, chunks: &[tg::build::log::Chunk]) -> Vec<String> {
		let mut scroll = self.clone();

		let mut lines = Vec::with_capacity(count);
		let mut buffer = Vec::with_capacity(3 * self.width / 2);

		let mut last_line_length = 0;
		let mut n = 0;
		while n < count {
			if scroll.is_at_end(chunks) {
				break;
			}

			// Read at most `width` graphemes.
			let mut width = 0;
			buffer.clear();
			let skip = loop {
				if scroll.advance(true, chunks, &mut buffer).is_none() {
					// Handle invalid utf8.
					buffer.extend_from_slice("�".as_bytes());
					if let Some(next_codepoint) = chunks[scroll.chunk].next_codepoint(scroll.byte) {
						scroll.byte = next_codepoint;
					} else {
						scroll.byte += 1;
						if scroll.byte == chunks[scroll.chunk].data.len()
							&& scroll.chunk < chunks.len() - 1
						{
							scroll.chunk += 1;
						}
						let pos = chunks[scroll.chunk].offset.to_usize().unwrap() + scroll.byte;
						scroll.cursor.set_cursor(pos);
					}
				}

				// Check if we need to break out of the loop. If the last line width was equal to scroll.width and the buffer is just one newline, it means we can skip the line.
				width += 1;
				if buffer.ends_with(b"\r\n") {
					buffer.shrink_to(buffer.len() - 2);
					break last_line_length == scroll.width && width == 1;
				} else if buffer.ends_with(b"\n") {
					buffer.pop();
					break last_line_length == scroll.width && width == 1;
				} else if width == scroll.width {
					break false;
				}
			};
			last_line_length = width;
			if !skip {
				let line = String::from_utf8(buffer.clone()).unwrap();
				lines.push(line.replace('\t', " "));
				n += 1;
			}
		}

		lines
	}

	// Get the UTF8-validated string that contains the current scroll position.
	fn get_utf8_str<'a>(
		&self,
		chunks: &'a [tg::build::log::Chunk], // The chunks to use as a corpus.
	) -> Option<(Either<&'a str, String>, usize)> {
		let first_codepoint = chunks[self.chunk].first_codepoint()?;
		let last_codepoint = chunks[self.chunk].last_codepoint()?;

		// Special case: we're reading past the end of the buffer.
		if self.is_at_end(chunks) {
			let chunk = chunks.last().unwrap();
			let chunk_start = chunk.offset.to_usize().unwrap() + chunk.data.len();
			Some((Either::Left(""), chunk_start))
		} else if self.byte == last_codepoint {
			let first_byte = chunks[self.chunk].data[self.byte];
			let mut buf = Vec::with_capacity(4);
			let num_bytes = if 0b1111_0000 & first_byte == 0b1111_0000 {
				4
			} else if 0b1110_0000 & first_byte == 0b1110_0000 {
				3
			} else if 0b1100_0000 & first_byte == 0b1100_0000 {
				2
			} else {
				1
			};
			for n in 0..num_bytes {
				if self.byte + n < chunks[self.chunk].data.len() {
					buf.push(chunks[self.chunk].data[self.byte + n]);
				} else {
					let byte = self.byte + n - chunks[self.chunk].data.len();
					buf.push(chunks[self.chunk + 1].data[byte]);
				}
			}
			let chunk_start = chunks[self.chunk].offset.to_usize().unwrap() + self.byte;
			let string = String::from_utf8(buf).ok()?;
			Some((Either::Right(string), chunk_start))
		} else {
			let bytes = &chunks[self.chunk].data[first_codepoint..last_codepoint];
			let utf8 = std::str::from_utf8(bytes).ok()?;
			let chunk_start = chunks[self.chunk].offset.to_usize().unwrap() + first_codepoint;
			Some((Either::Left(utf8), chunk_start))
		}
	}

	// Helper: get a utf8 string that ends at `end`.
	fn get_pre_context<'a>(
		&self,
		chunks: &'a [tg::build::log::Chunk],
		end: usize,
	) -> Option<(&'a str, usize)> {
		let chunk = chunks[..=self.chunk]
			.iter()
			.rev()
			.find(|chunk| chunk.offset.to_usize().unwrap() < end)?;
		let end_byte = end - chunk.offset.to_usize().unwrap();
		for start_byte in 0..chunk.data.len() {
			let bytes = &chunk.data[start_byte..end_byte];
			if let Ok(string) = std::str::from_utf8(bytes) {
				return Some((string, chunk.offset.to_usize().unwrap() + start_byte));
			}
		}
		None
	}
}

// Helper to track start/end codepoints in a buffer.
trait ChunkExt {
	fn first_codepoint(&self) -> Option<usize>;
	fn last_codepoint(&self) -> Option<usize>;
	fn next_codepoint(&self, byte: usize) -> Option<usize>;
	fn prev_codepoint(&self, byte: usize) -> Option<usize>;
}

impl ChunkExt for tg::build::log::Chunk {
	fn first_codepoint(&self) -> Option<usize> {
		for (i, byte) in self.data.iter().enumerate() {
			if *byte & 0b1111_0000 == 0b1111_0000
				|| *byte & 0b1110_0000 == 0b1110_0000
				|| *byte & 0b1100_0000 == 0b1100_0000
				|| *byte & 0b1000_0000 == 0b0000_0000
			{
				return Some(i);
			}
		}
		None
	}

	fn last_codepoint(&self) -> Option<usize> {
		for (i, byte) in self.data.iter().rev().enumerate() {
			if *byte & 0b1111_0000 == 0b1111_0000
				|| *byte & 0b1110_0000 == 0b1110_0000
				|| *byte & 0b1100_0000 == 0b1100_0000
				|| *byte & 0b1000_0000 == 0b0000_0000
			{
				return Some(self.data.len() - 1 - i);
			}
		}
		None
	}

	fn next_codepoint(&self, byte: usize) -> Option<usize> {
		for i in byte..self.data.len() {
			let byte = self.data[i];
			if byte & 0b1111_0000 == 0b1111_0000
				|| byte & 0b1110_0000 == 0b1110_0000
				|| byte & 0b1100_0000 == 0b1100_0000
				|| byte & 0b1000_0000 == 0b0000_0000
			{
				return Some(i);
			}
		}
		None
	}

	fn prev_codepoint(&self, byte: usize) -> Option<usize> {
		for i in (0..byte).rev() {
			let byte = self.data[i];
			if byte & 0b1111_0000 == 0b1111_0000
				|| byte & 0b1110_0000 == 0b1110_0000
				|| byte & 0b1100_0000 == 0b1100_0000
				|| byte & 0b1000_0000 == 0b0000_0000
			{
				return Some(i);
			}
		}
		None
	}
}

#[cfg(test)]
mod tests {
	use super::Scroll;
	use tangram_client as tg;

	#[test]
	fn scrolling_logic() {
		let chunks = vec![
			tg::build::log::Chunk {
				offset: 0,
				data: b"11".to_vec().into(),
				end: false,
			},
			tg::build::log::Chunk {
				offset: 2,
				data: b"\n\n22\n".to_vec().into(),
				end: false,
			},
			tg::build::log::Chunk {
				offset: 7,
				data: b"3".to_vec().into(),
				end: false,
			},
			tg::build::log::Chunk {
				offset: 8,
				data: b"344".to_vec().into(),
				end: false,
			},
		];
		let mut scroll = Scroll::new(
			2,
			chunks.len() - 1,
			chunks.last().unwrap().data.len(),
			&chunks,
		);

		// Word wrap.
		scroll.scroll_up(2, &chunks);
		assert_eq!(scroll.chunk, 2);
		assert_eq!(scroll.byte, 0);

		let lines = scroll.read_lines(4, &chunks);
		assert_eq!(lines.len(), 2);
		assert_eq!(&lines[0], "33");
		assert_eq!(&lines[1], "44");

		// Empty lines.
		scroll.scroll_up(5, &chunks);
		assert_eq!(scroll.chunk, 0);
		assert_eq!(scroll.byte, 0);
		assert_eq!(scroll.cursor.cur_cursor(), 0);

		let lines = scroll.read_lines(5, &chunks);
		assert_eq!(lines.len(), 5);
		assert_eq!(&lines[0], "11");
		assert_eq!(&lines[1], "");
		assert_eq!(&lines[2], "22");
		assert_eq!(&lines[3], "33");
		assert_eq!(&lines[4], "44");

		// Scrolling down.
		scroll.scroll_down(2, &chunks);
		let lines = scroll.read_lines(5, &chunks);
		assert_eq!(lines.len(), 3);
		assert_eq!(&lines[0], "22");
		assert_eq!(&lines[1], "33");
		assert_eq!(&lines[2], "44");
	}

	#[allow(clippy::too_many_lines)]
	#[test]
	fn tailing() {
		let chunks = vec![
			tg::build::log::Chunk {
				offset: 0,
				data: b"\"log line 0\"\n".to_vec().into(),
				end: false,
			},
			tg::build::log::Chunk {
				offset: 13,
				data: b"\"log line 1\"\n".to_vec().into(),
				end: false,
			},
			tg::build::log::Chunk {
				offset: 26,
				data: b"\"log line 2\"\n".to_vec().into(),
				end: false,
			},
			tg::build::log::Chunk {
				offset: 39,
				data: b"\"log line 3\"\n".to_vec().into(),
				end: false,
			},
			tg::build::log::Chunk {
				offset: 52,
				data: b"\"log line 4\"\n".to_vec().into(),
				end: false,
			},
			tg::build::log::Chunk {
				offset: 65,
				data: b"\"log line 5\"\n".to_vec().into(),
				end: false,
			},
			tg::build::log::Chunk {
				offset: 78,
				data: b"\"log line 6\"\n".to_vec().into(),
				end: false,
			},
			tg::build::log::Chunk {
				offset: 91,
				data: b"\"log line 7\"\n".to_vec().into(),
				end: false,
			},
			tg::build::log::Chunk {
				offset: 104,
				data: b"\"log line 8\"\n".to_vec().into(),
				end: false,
			},
			tg::build::log::Chunk {
				offset: 117,
				data: b"\"log line 9\"\n".to_vec().into(),
				end: false,
			},
			tg::build::log::Chunk {
				offset: 130,
				data: b"\"log line 10\"\n".to_vec().into(),
				end: false,
			},
			tg::build::log::Chunk {
				offset: 144,
				data: b"\"log line 11\"\n".to_vec().into(),
				end: false,
			},
			tg::build::log::Chunk {
				offset: 158,
				data: b"\"log line 12\"\n".to_vec().into(),
				end: false,
			},
			tg::build::log::Chunk {
				offset: 172,
				data: b"\"log line 13\"\n".to_vec().into(),
				end: false,
			},
			tg::build::log::Chunk {
				offset: 186,
				data: b"\"log line 14\"\n".to_vec().into(),
				end: false,
			},
			tg::build::log::Chunk {
				offset: 200,
				data: b"\"log line 15\"\n".to_vec().into(),
				end: false,
			},
			tg::build::log::Chunk {
				offset: 214,
				data: b"\"log line 16\"\n".to_vec().into(),
				end: false,
			},
			tg::build::log::Chunk {
				offset: 228,
				data: b"\"log line 17\"\n".to_vec().into(),
				end: false,
			},
			tg::build::log::Chunk {
				offset: 242,
				data: b"\"log line 18\"\n".to_vec().into(),
				end: false,
			},
			tg::build::log::Chunk {
				offset: 256,
				data: b"\"log line 19\"\n".to_vec().into(),
				end: false,
			},
			tg::build::log::Chunk {
				offset: 270,
				data: b"\"log line 20\"\n".to_vec().into(),
				end: false,
			},
			tg::build::log::Chunk {
				offset: 284,
				data: b"\"log line 21\"\n".to_vec().into(),
				end: false,
			},
			tg::build::log::Chunk {
				offset: 298,
				data: b"\"log line 22\"\n".to_vec().into(),
				end: false,
			},
			tg::build::log::Chunk {
				offset: 312,
				data: b"\"log line 23\"\n".to_vec().into(),
				end: false,
			},
		];
		let height = 40;
		let width = 189;
		let chunk = chunks.len() - 1;
		let byte = chunks.last().unwrap().data.len() - 1;
		let mut scroll = Scroll::new(width, chunk, byte, &chunks);
		scroll.scroll_up(height, &chunks);
		let lines = scroll.read_lines(height, &chunks);
		assert_eq!(lines.len(), chunks.len());
	}

	#[test]
	fn simple_tailing() {
		let chunks = vec![
			tg::build::log::Chunk {
				offset: 0,
				data: b"\"0\"\n".to_vec().into(),
				end: false,
			},
			tg::build::log::Chunk {
				offset: 4,
				data: b"\"1\"\n".to_vec().into(),
				end: false,
			},
			tg::build::log::Chunk {
				offset: 8,
				data: b"\"2\"\n".to_vec().into(),
				end: false,
			},
			tg::build::log::Chunk {
				offset: 12,
				data: b"\"3\"\n".to_vec().into(),
				end: true,
			},
		];
		let mut scroll = Scroll::new(
			10,
			chunks.len() - 1,
			chunks.last().unwrap().data.len(),
			&chunks,
		);
		scroll.scroll_up(3, &chunks);
		assert_eq!(scroll.chunk, 2);
		assert_eq!(scroll.byte, 0);
		let lines = scroll.read_lines(3, &chunks);
		assert_eq!(&lines[0], "\"2\"");
		assert_eq!(&lines[1], "\"3\"");
	}

	#[allow(clippy::too_many_lines)]
	#[test]
	fn scroll_up() {
		let chunks = [
			tg::build::log::Chunk {
				offset: 0,
				data: b"\"0\"\n".to_vec().into(),
				end: false,
			},
			tg::build::log::Chunk {
				offset: 4,
				data: b"\"1\"\n".to_vec().into(),
				end: false,
			},
			tg::build::log::Chunk {
				offset: 8,
				data: b"\"2\"\n".to_vec().into(),
				end: false,
			},
			tg::build::log::Chunk {
				offset: 12,
				data: b"\"3\"\n".to_vec().into(),
				end: false,
			},
			tg::build::log::Chunk {
				offset: 16,
				data: b"\"4\"\n".to_vec().into(),
				end: false,
			},
			tg::build::log::Chunk {
				offset: 20,
				data: b"\"5\"\n".to_vec().into(),
				end: false,
			},
			tg::build::log::Chunk {
				offset: 24,
				data: b"\"6\"\n".to_vec().into(),
				end: false,
			},
			tg::build::log::Chunk {
				offset: 28,
				data: b"\"7\"\n".to_vec().into(),
				end: false,
			},
			tg::build::log::Chunk {
				offset: 32,
				data: b"\"8\"\n".to_vec().into(),
				end: false,
			},
			tg::build::log::Chunk {
				offset: 36,
				data: b"\"9\"\n".to_vec().into(),
				end: false,
			},
			tg::build::log::Chunk {
				offset: 40,
				data: b"\"10\"\n".to_vec().into(),
				end: false,
			},
			tg::build::log::Chunk {
				offset: 45,
				data: b"\"11\"\n".to_vec().into(),
				end: false,
			},
			tg::build::log::Chunk {
				offset: 50,
				data: b"\"12\"\n".to_vec().into(),
				end: false,
			},
			tg::build::log::Chunk {
				offset: 55,
				data: b"\"13\"\n".to_vec().into(),
				end: false,
			},
			tg::build::log::Chunk {
				offset: 60,
				data: b"\"14\"\n".to_vec().into(),
				end: false,
			},
			tg::build::log::Chunk {
				offset: 65,
				data: b"\"15\"\n".to_vec().into(),
				end: false,
			},
			tg::build::log::Chunk {
				offset: 70,
				data: b"\"16\"\n".to_vec().into(),
				end: false,
			},
			tg::build::log::Chunk {
				offset: 75,
				data: b"\"17\"\n".to_vec().into(),
				end: false,
			},
			tg::build::log::Chunk {
				offset: 80,
				data: b"\"18\"\n".to_vec().into(),
				end: false,
			},
			tg::build::log::Chunk {
				offset: 85,
				data: b"\"19\"\n".to_vec().into(),
				end: false,
			},
			tg::build::log::Chunk {
				offset: 90,
				data: b"\"20\"\n".to_vec().into(),
				end: true,
			},
		];
		let mut inner = Scroll::new(
			40,
			chunks.len() - 1,
			chunks.last().unwrap().data.len(),
			&chunks,
		);
		let height = 6;
		inner.scroll_up(height + 2, &chunks);
	}
}
