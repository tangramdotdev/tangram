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
	parent: Option<Weak<TreeItemInner>>,
	state: std::sync::Mutex<TreeItemState>,
	status_task: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
	tg: Box<dyn tg::Handle>,
	title_task: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

struct TreeItemState {
	children: Option<Vec<TreeItem>>,
	expanded: bool,
	selected: bool,
	status: TreeItemStatus,
	title: Option<String>,
}

enum TreeItemStatus {
	Unknown,
	Building,
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
	chunks: tokio::sync::Mutex<Vec<tg::build::LogChunk>>,

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
			selected,
			status: TreeItemStatus::Building,
			title: None,
		};
		let inner = Arc::new(TreeItemInner {
			build: build.clone(),
			children_task: std::sync::Mutex::new(None),
			index,
			parent,
			state: std::sync::Mutex::new(state),
			status_task: std::sync::Mutex::new(None),
			tg: tg.clone_box(),
			title_task: std::sync::Mutex::new(None),
		});

		let item = Self { inner };

		item.inner
			.status_task
			.lock()
			.unwrap()
			.replace(tokio::task::spawn({
				let item = item.clone();
				async move {
					let status = match item.inner.build.outcome(item.inner.tg.as_ref()).await {
						Err(_) => TreeItemStatus::Unknown,
						Ok(tg::build::Outcome::Terminated) => TreeItemStatus::Terminated,
						Ok(tg::build::Outcome::Canceled) => TreeItemStatus::Canceled,
						Ok(tg::build::Outcome::Failed(_)) => TreeItemStatus::Failed,
						Ok(tg::build::Outcome::Succeeded(_)) => TreeItemStatus::Succeeded,
					};
					item.inner.state.lock().unwrap().status = status;
				}
			}));

		item.inner
			.title_task
			.lock()
			.unwrap()
			.replace(tokio::task::spawn({
				let item = item.clone();
				async move {
					let title = title(item.inner.tg.as_ref(), &item.inner.build)
						.await
						.ok()
						.flatten();
					item.inner.state.lock().unwrap().title = title;
				}
			}));

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
				let Ok(mut children) = item.inner.build.children(item.inner.tg.as_ref()).await
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
		let status = match self.inner.state.lock().unwrap().status {
			TreeItemStatus::Unknown => "?".yellow(),
			TreeItemStatus::Building => {
				let now = std::time::SystemTime::now()
					.duration_since(std::time::UNIX_EPOCH)
					.unwrap()
					.as_millis();
				let position = (now / (1000 / 10)) % 10;
				let position = position.to_usize().unwrap();
				SPINNER[position].to_string().blue()
			},
			TreeItemStatus::Terminated => "⦻".red(),
			TreeItemStatus::Canceled => "⦻".yellow(),
			TreeItemStatus::Failed => "✗".red(),
			TreeItemStatus::Succeeded => "✓".green(),
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
			status,
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
		if let Some(task) = self.status_task.lock().unwrap().take() {
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

		// Create the log streaming task.
		let task = tokio::task::spawn({
			let log = log.clone();
			async move {
				let mut scroll = None;
				let area = log.rect().area().to_i64().unwrap();
				let arg = tg::build::GetLogArg {
					pos: None,
					len: Some(-3 * area / 2),
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
							let arg = tg::build::GetLogArg {
								pos: None,
								len: Some(-3 * area / 2),
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
	async fn add_chunk(&self, chunk: tg::build::LogChunk) {
		// Get some metadata about this log chunk.
		let chunk_position = chunk.pos;
		let chunk_size = chunk.bytes.len().to_u64().unwrap();

		let mut chunks = self.inner.chunks.lock().await;

		// Short circuit for the common case that we're appending to the log.
		if chunks.is_empty() || chunks.last().unwrap().pos < chunk.pos {
			chunks.push(chunk);
			return;
		};

		// Find where this log chunk needs to be inserted.
		let index = chunks
			.iter()
			.position(|existing| existing.pos > chunk.pos)
			.unwrap_or(chunks.len() - 1);
		chunks.insert(index, chunk);

		// Check if we need to truncate the next chunk.
		let next_pos = chunk_position + chunk_size;
		let next_chunk = &mut chunks[index + 1];
		if next_pos > next_chunk.pos {
			let new_length =
				next_chunk.bytes.len() - (next_pos - next_chunk.pos).to_usize().unwrap();
			if new_length > 0 {
				next_chunk.bytes.truncate(new_length);
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
				chunks.last().unwrap().bytes.len(),
				&chunks,
			);
			inner.scroll_up(height + 2, &chunks);
			scroll.replace(inner);
			return Ok(());
		};

		// Attempt to scroll up by one, and pull in new data if necessary.
		if scroll.scroll_up(2, &self.inner.chunks.lock().await) == 0 {
			let pos = self.inner.chunks.lock().await.first().unwrap().pos;
			if pos == 0 {
				return Ok(());
			}
			let len = 3 * self.rect().area().to_i64().unwrap() / 2;
			let arg = tg::build::GetLogArg {
				pos: Some(pos),
				len: Some(len),
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
					e.pos <= scroll_pos && (e.pos + e.bytes.len().to_u64().unwrap()) >= scroll_pos
				})
				.unwrap();
			let byte = (scroll_pos - chunks[chunk].pos).to_usize().unwrap();
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
			let byte = chunks.last().unwrap().bytes.len();
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
	fn new(width: usize, chunk: usize, byte: usize, chunks: &[tg::build::LogChunk]) -> Self {
		let offset = chunks[chunk].pos.to_usize().unwrap() + byte;
		let length = chunks
			.last()
			.map(|chunk| chunk.pos.to_usize().unwrap() + chunk.bytes.len())
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
		forward: bool,                  // Advance forward if true, backward if false.
		chunks: &[tg::build::LogChunk], // The chunks to use as a corpus.
		buffer: &mut Vec<u8>,           // The output buffer to write to.
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
					if new_pos < chunks[self.chunk].pos.to_usize().unwrap()
						|| new_pos
							>= chunks[self.chunk].pos.to_usize().unwrap()
								+ chunks[self.chunk].bytes.len()
					{
						self.chunk = chunks
							.iter()
							.enumerate()
							.find_map(|(idx, chunk)| {
								(new_pos >= chunk.pos.to_usize().unwrap()
									&& new_pos < chunk.pos.to_usize().unwrap() + chunk.bytes.len())
								.then_some(idx)
							})
							.unwrap_or(chunks.len() - 1);
					}
					self.byte = new_pos - chunks[self.chunk].pos.to_usize().unwrap();
					break;
				},
				Ok(None) => {
					if forward {
						self.byte = chunks[self.chunk].bytes.len();
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
				let bytes = &chunks[new_chunk].bytes[old_byte..new_byte];
				buffer.extend_from_slice(bytes);
			} else {
				let bytes = &chunks[old_chunk].bytes[old_byte..];
				buffer.extend_from_slice(bytes);
				let bytes = &chunks[new_chunk].bytes[..new_byte];
				buffer.extend_from_slice(bytes);
			}
		} else {
			let mid = buffer.len();
			if new_chunk == old_chunk {
				let bytes = &chunks[new_chunk].bytes[new_byte..old_byte];
				buffer.extend_from_slice(bytes);
			} else {
				let bytes = &chunks[new_chunk].bytes[new_byte..];
				buffer.extend_from_slice(bytes);
				let bytes = &chunks[old_chunk].bytes[..old_byte];
				buffer.extend_from_slice(bytes);
			}
			buffer.rotate_left(mid);
		}
		Some(true)
	}

	fn is_at_end(&self, chunks: &[tg::build::LogChunk]) -> bool {
		let chunk = &chunks[self.chunk];
		self.byte == chunk.bytes.len()
	}

	fn is_at_start(&self, chunks: &[tg::build::LogChunk]) -> bool {
		self.chunk == 0 && chunks[self.chunk].first_codepoint() == Some(self.byte)
	}

	/// Scroll down by `height` num lines, wrapping on grapheme clusters, and return the number of lines that were scrolled.
	fn scroll_down(&mut self, height: usize, chunks: &[tg::build::LogChunk]) -> usize {
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
	fn scroll_up(&mut self, height: usize, chunks: &[tg::build::LogChunk]) -> usize {
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

	fn read_lines(&self, count: usize, chunks: &[tg::build::LogChunk]) -> Vec<String> {
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
						if scroll.byte == chunks[scroll.chunk].bytes.len()
							&& scroll.chunk < chunks.len() - 1
						{
							scroll.chunk += 1;
						}
						let pos = chunks[scroll.chunk].pos.to_usize().unwrap() + scroll.byte;
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
		chunks: &'a [tg::build::LogChunk], // The chunks to use as a corpus.
	) -> Option<(Either<&'a str, String>, usize)> {
		let first_codepoint = chunks[self.chunk].first_codepoint()?;
		let last_codepoint = chunks[self.chunk].last_codepoint()?;

		// Special case: we're reading past the end of the buffer.
		if self.is_at_end(chunks) {
			let chunk = chunks.last().unwrap();
			let chunk_start = chunk.pos.to_usize().unwrap() + chunk.bytes.len();
			Some((Either::Left(""), chunk_start))
		} else if self.byte == last_codepoint {
			let first_byte = chunks[self.chunk].bytes[self.byte];
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
				if self.byte + n < chunks[self.chunk].bytes.len() {
					buf.push(chunks[self.chunk].bytes[self.byte + n]);
				} else {
					let byte = self.byte + n - chunks[self.chunk].bytes.len();
					buf.push(chunks[self.chunk + 1].bytes[byte]);
				}
			}
			let chunk_start = chunks[self.chunk].pos.to_usize().unwrap() + self.byte;
			let string = String::from_utf8(buf).ok()?;
			Some((Either::Right(string), chunk_start))
		} else {
			let bytes = &chunks[self.chunk].bytes[first_codepoint..last_codepoint];
			let utf8 = std::str::from_utf8(bytes).ok()?;
			let chunk_start = chunks[self.chunk].pos.to_usize().unwrap() + first_codepoint;
			Some((Either::Left(utf8), chunk_start))
		}
	}

	// Helper: get a utf8 string that ends at `end`.
	fn get_pre_context<'a>(
		&self,
		chunks: &'a [tg::build::LogChunk],
		end: usize,
	) -> Option<(&'a str, usize)> {
		let chunk = chunks[..=self.chunk]
			.iter()
			.rev()
			.find(|chunk| chunk.pos.to_usize().unwrap() < end)?;
		let end_byte = end - chunk.pos.to_usize().unwrap();
		for start_byte in 0..chunk.bytes.len() {
			let bytes = &chunk.bytes[start_byte..end_byte];
			if let Ok(string) = std::str::from_utf8(bytes) {
				return Some((string, chunk.pos.to_usize().unwrap() + start_byte));
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

impl ChunkExt for tg::build::LogChunk {
	fn first_codepoint(&self) -> Option<usize> {
		for (i, byte) in self.bytes.iter().enumerate() {
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
		for (i, byte) in self.bytes.iter().rev().enumerate() {
			if *byte & 0b1111_0000 == 0b1111_0000
				|| *byte & 0b1110_0000 == 0b1110_0000
				|| *byte & 0b1100_0000 == 0b1100_0000
				|| *byte & 0b1000_0000 == 0b0000_0000
			{
				return Some(self.bytes.len() - 1 - i);
			}
		}
		None
	}

	fn next_codepoint(&self, byte: usize) -> Option<usize> {
		for i in byte..self.bytes.len() {
			let byte = self.bytes[i];
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
			let byte = self.bytes[i];
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
			tg::build::LogChunk {
				pos: 0,
				bytes: b"11".to_vec().into(),
			},
			tg::build::LogChunk {
				pos: 2,
				bytes: b"\n\n22\n".to_vec().into(),
			},
			tg::build::LogChunk {
				pos: 7,
				bytes: b"3".to_vec().into(),
			},
			tg::build::LogChunk {
				pos: 8,
				bytes: b"344".to_vec().into(),
			},
		];
		let mut scroll = Scroll::new(
			2,
			chunks.len() - 1,
			chunks.last().unwrap().bytes.len(),
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

		// Log chunks from a real build.
		let chunks: &str = r#"
		[{"pos":0,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,97,32,66,83,68,45,99,111,109,112,97,116,105,98,108,101,32,105,110,115,116,97,108,108,46,46,46,32]},{"pos":41,"bytes":[47,46,116,97,110,103,114,97,109,47,97,114,116,105,102,97,99,116,115,47,100,105,114,95,48,49,113,119,104,120,104,122,51,99,50,52,52,114,57,53,103,56,102,114,114,102,116,52,122,98,115,97,120,113,97,53,102,106,112,104,100,119,57,57,110,112,50,50,112,50,49,119,112,113,107,50,48,48,47,98,105,110,47,105,110,115,116,97,108,108,32,45,99,10,99,104,101,99,107,105,110,103,32,119,104,101,116,104,101,114,32,98,117,105,108,100,32,101,110,118,105,114,111,110,109,101,110,116,32,105,115,32,115,97,110,101,46,46,46,32]},{"pos":181,"bytes":[121,101,115,10]},{"pos":185,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,97,32,114,97,99,101,45,102,114,101,101,32,109,107,100,105,114,32,45,112,46,46,46,32]},{"pos":222,"bytes":[47,46,116,97,110,103,114,97,109,47,97,114,116,105,102,97,99,116,115,47,100,105,114,95,48,49,53,120,56,106,53,53,115,121,109,55,107,56,52,122,112,57,114,109,114,53,106,120,57,48,56,50,107,97,115,107,48,122,116,121,106,107,100,54,98,49,52,113,112,112,55,102,120,49,99,118,101,48,47,98,117,105,108,100,45,97,117,120,47,105,110,115,116,97,108,108,45,115,104,32,45,99,32,45,100,10]},{"pos":328,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,103,97,119,107,46,46,46,32,110,111,10,99,104,101,99,107,105,110,103,32,102,111,114,32,109,97,119,107,46,46,46,32,110,111,10,99,104,101,99,107,105,110,103,32,102,111,114,32,110,97,119,107,46,46,46,32,110,111,10,99,104,101,99,107,105,110,103,32,102,111,114,32,97,119,107,46,46,46,32,97,119,107,10,99,104,101,99,107,105,110,103,32,119,104,101,116,104,101,114,32,109,97,107,101,32,115,101,116,115,32,36,40,77,65,75,69,41,46,46,46,32]},{"pos":462,"bytes":[110,111,10]},{"pos":465,"bytes":[99,104,101,99,107,105,110,103,32,119,104,101,116,104,101,114,32,109,97,107,101,32,115,117,112,112,111,114,116,115,32,110,101,115,116,101,100,32,118,97,114,105,97,98,108,101,115,46,46,46,32]},{"pos":516,"bytes":[110,111,10]},{"pos":519,"bytes":[99,104,101,99,107,105,110,103,32,119,104,101,116,104,101,114,32,109,97,107,101,32,115,117,112,112,111,114,116,115,32,116,104,101,32,105,110,99,108,117,100,101,32,100,105,114,101,99,116,105,118,101,46,46,46,32]},{"pos":575,"bytes":[110,111,10]},{"pos":578,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,103,99,99,46,46,46,32]},{"pos":598,"bytes":[103,99,99,10]},{"pos":602,"bytes":[99,104,101,99,107,105,110,103,32,119,104,101,116,104,101,114,32,116,104,101,32,67,32,99,111,109,112,105,108,101,114,32,119,111,114,107,115,46,46,46,32]},{"pos":643,"bytes":[121,101,115,10,99,104,101,99,107,105,110,103,32,102,111,114,32,67,32,99,111,109,112,105,108,101,114,32,100,101,102,97,117,108,116,32,111,117,116,112,117,116,32,102,105,108,101,32,110,97,109,101,46,46,46,32,97,46,111,117,116,10]},{"pos":705,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,115,117,102,102,105,120,32,111,102,32,101,120,101,99,117,116,97,98,108,101,115,46,46,46,32]},{"pos":743,"bytes":[10]},{"pos":744,"bytes":[99,104,101,99,107,105,110,103,32,119,104,101,116,104,101,114,32,119,101,32,97,114,101,32,99,114,111,115,115,32,99,111,109,112,105,108,105,110,103,46,46,46,32]},{"pos":787,"bytes":[110,111,10]},{"pos":790,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,115,117,102,102,105,120,32,111,102,32,111,98,106,101,99,116,32,102,105,108,101,115,46,46,46,32]},{"pos":829,"bytes":[111,10,99,104,101,99,107,105,110,103,32,119,104,101,116,104,101,114,32,116,104,101,32,99,111,109,112,105,108,101,114,32,115,117,112,112,111,114,116,115,32,71,78,85,32,67,46,46,46,32]},{"pos":879,"bytes":[121,101,115,10]},{"pos":883,"bytes":[99,104,101,99,107,105,110,103,32,119,104,101,116,104,101,114,32,103,99,99,32,97,99,99,101,112,116,115,32,45,103,46,46,46,32]},{"pos":918,"bytes":[121,101,115,10]},{"pos":922,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,103,99,99,32,111,112,116,105,111,110,32,116,111,32,101,110,97,98,108,101,32,67,49,49,32,102,101,97,116,117,114,101,115,46,46,46,32]},{"pos":972,"bytes":[110,111,110,101,32,110,101,101,100,101,100,10]},{"pos":984,"bytes":[99,104,101,99,107,105,110,103,32,119,104,101,116,104,101,114,32,103,99,99,32,117,110,100,101,114,115,116,97,110,100,115,32,45,99,32,97,110,100,32,45,111,32,116,111,103,101,116,104,101,114,46,46,46,32]},{"pos":1039,"bytes":[121,101,115,10]},{"pos":1043,"bytes":[99,104,101,99,107,105,110,103,32,119,104,101,116,104,101,114,32,116,104,101,32,99,111,109,112,105,108,101,114,32,105,115,32,99,108,97,110,103,46,46,46,32]},{"pos":1085,"bytes":[110,111,10,99,104,101,99,107,105,110,103,32,102,111,114,32,99,111,109,112,105,108,101,114,32,111,112,116,105,111,110,32,110,101,101,100,101,100,32,119,104,101,110,32,99,104,101,99,107,105,110,103,32,102,111,114,32,100,101,99,108,97,114,97,116,105,111,110,115,46,46,46,32]},{"pos":1158,"bytes":[110,111,110,101,10,99,104,101,99,107,105,110,103,32,100,101,112,101,110,100,101,110,99,121,32,115,116,121,108,101,32,111,102,32,103,99,99,46,46,46,32,110,111,110,101,10,99,104,101,99,107,105,110,103,32,102,111,114,32,115,116,100,105,111,46,104,46,46,46,32]},{"pos":1228,"bytes":[121,101,115,10]},{"pos":1232,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,115,116,100,108,105,98,46,104,46,46,46,32]},{"pos":1257,"bytes":[121,101,115,10]},{"pos":1261,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,115,116,114,105,110,103,46,104,46,46,46,32]},{"pos":1286,"bytes":[121,101,115,10]},{"pos":1290,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,105,110,116,116,121,112,101,115,46,104,46,46,46,32]},{"pos":1317,"bytes":[121,101,115,10]},{"pos":1321,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,115,116,100,105,110,116,46,104,46,46,46,32]},{"pos":1346,"bytes":[121,101,115,10]},{"pos":1350,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,115,116,114,105,110,103,115,46,104,46,46,46,32]},{"pos":1376,"bytes":[121,101,115,10]},{"pos":1380,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,115,121,115,47,115,116,97,116,46,104,46,46,46,32]},{"pos":1407,"bytes":[121,101,115,10]},{"pos":1411,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,115,121,115,47,116,121,112,101,115,46,104,46,46,46,32]},{"pos":1439,"bytes":[121,101,115,10]},{"pos":1443,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,117,110,105,115,116,100,46,104,46,46,46,32]},{"pos":1468,"bytes":[121,101,115,10]},{"pos":1472,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,119,99,104,97,114,46,104,46,46,46,32]},{"pos":1496,"bytes":[121,101,115,10]},{"pos":1500,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,109,105,110,105,120,47,99,111,110,102,105,103,46,104,46,46,46,32]},{"pos":1531,"bytes":[110,111,10]},{"pos":1534,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,115,121,115,47,112,97,114,97,109,46,104,46,46,46,32]},{"pos":1562,"bytes":[121,101,115,10]},{"pos":1566,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,115,116,100,98,111,111,108,46,104,46,46,46,32]},{"pos":1592,"bytes":[121,101,115,10]},{"pos":1596,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,118,102,111,114,107,46,104,46,46,46,32]},{"pos":1620,"bytes":[110,111,10]},{"pos":1623,"bytes":[99,104,101,99,107,105,110,103,32,119,104,101,116,104,101,114,32,105,116,32,105,115,32,115,97,102,101,32,116,111,32,100,101,102,105,110,101,32,95,95,69,88,84,69,78,83,73,79,78,83,95,95,46,46,46,32]},{"pos":1679,"bytes":[121,101,115,10,99,104,101,99,107,105,110,103,32,119,104,101,116,104,101,114,32,95,88,79,80,69,78,95,83,79,85,82,67,69,32,115,104,111,117,108,100,32,98,101,32,100,101,102,105,110,101,100,46,46,46,32]},{"pos":1735,"bytes":[110,111,10]},{"pos":1738,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,103,99,99,46,46,46,32,40,99,97,99,104,101,100,41,32,103,99,99,10]},{"pos":1771,"bytes":[99,104,101,99,107,105,110,103,32,119,104,101,116,104,101,114,32,116,104,101,32,99,111,109,112,105,108,101,114,32,115,117,112,112,111,114,116,115,32,71,78,85,32,67,46,46,46,32,40,99,97,99,104,101,100,41,32]},{"pos":1828,"bytes":[121,101,115,10,99,104,101,99,107,105,110,103,32,119,104,101,116,104,101,114,32,103,99,99,32,97,99,99,101,112,116,115,32,45,103,46,46,46,32,40,99,97,99,104,101,100,41,32,121,101,115,10,99,104,101,99,107,105,110,103,32,102,111,114,32,103,99,99,32,111,112,116,105,111,110,32,116,111,32,101,110,97,98,108,101,32,67,49,49,32,102,101,97,116,117,114,101,115,46,46,46,32,40,99,97,99,104,101,100,41,32,110,111,110,101,32,110,101,101,100,101,100,10,99,104,101,99,107,105,110,103,32,119,104,101,116,104,101,114,32,103,99,99,32,117,110,100,101,114,115,116,97,110,100,115,32,45,99,32,97,110,100,32,45,111,32,116,111,103,101,116,104,101,114,46,46,46,32,40,99,97,99,104,101,100,41,32,121,101,115,10,99,104,101,99,107,105,110,103,32,119,104,101,116,104,101,114,32,116,104,101,32,99,111,109,112,105,108,101,114,32,105,115,32,99,108,97,110,103,46,46,46,32,40,99,97,99,104,101,100,41,32,110,111,10,99,104,101,99,107,105,110,103,32,102,111,114,32,99,111,109,112,105,108,101,114,32,111,112,116,105,111,110,32,110,101,101,100,101,100,32,119,104,101,110,32,99,104,101,99,107,105,110,103,32,102,111,114,32,100,101,99,108,97,114,97,116,105,111,110,115,46,46,46,32,40,99,97,99,104,101,100,41,32,110,111,110,101,10,99,104,101,99,107,105,110,103,32,100,101,112,101,110,100,101,110,99,121,32,115,116,121,108,101,32,111,102,32,103,99,99,46,46,46,32,40,99,97,99,104,101,100,41,32,110,111,110,101,10,99,104,101,99,107,105,110,103,32,102,111,114,32,103,43,43,46,46,46,32]},{"pos":2227,"bytes":[103,43,43,10]},{"pos":2231,"bytes":[99,104,101,99,107,105,110,103,32,119,104,101,116,104,101,114,32,116,104,101,32,99,111,109,112,105,108,101,114,32,115,117,112,112,111,114,116,115,32,71,78,85,32,67,43,43,46,46,46,32]},{"pos":2281,"bytes":[121,101,115,10]},{"pos":2285,"bytes":[99,104,101,99,107,105,110,103,32,119,104,101,116,104,101,114,32,103,43,43,32,97,99,99,101,112,116,115,32,45,103,46,46,46,32]},{"pos":2320,"bytes":[121,101,115,10]},{"pos":2324,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,103,43,43,32,111,112,116,105,111,110,32,116,111,32,101,110,97,98,108,101,32,67,43,43,49,49,32,102,101,97,116,117,114,101,115,46,46,46,32]},{"pos":2376,"bytes":[110,111,110,101,32,110,101,101,100,101,100,10]},{"pos":2388,"bytes":[99,104,101,99,107,105,110,103,32,100,101,112,101,110,100,101,110,99,121,32,115,116,121,108,101,32,111,102,32,103,43,43,46,46,46,32,110,111,110,101,10]},{"pos":2429,"bytes":[99,104,101,99,107,105,110,103,32,98,117,105,108,100,32,115,121,115,116,101,109,32,116,121,112,101,46,46,46,32]},{"pos":2459,"bytes":[120,56,54,95,54,52,45,112,99,45,108,105,110,117,120,45,103,110,117,10]},{"pos":2479,"bytes":[99,104,101,99,107,105,110,103,32,104,111,115,116,32,115,121,115,116,101,109,32,116,121,112,101,46,46,46,32,120,56,54,95,54,52,45,112,99,45,108,105,110,117,120,45,103,110,117,10,99,104,101,99,107,105,110,103,32,104,111,119,32,116,111,32,114,117,110,32,116,104,101,32,67,32,112,114,101,112,114,111,99,101,115,115,111,114,46,46,46,32]},{"pos":2570,"bytes":[103,99,99,32,45,69,10]},{"pos":2577,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,103,114,101,112,32,116,104,97,116,32,104,97,110,100,108,101,115,32,108,111,110,103,32,108,105,110,101,115,32,97,110,100,32,45,101,46,46,46,32]},{"pos":2629,"bytes":[47,46,116,97,110,103,114,97,109,47,97,114,116,105,102,97,99,116,115,47,100,105,114,95,48,49,113,119,104,120,104,122,51,99,50,52,52,114,57,53,103,56,102,114,114,102,116,52,122,98,115,97,120,113,97,53,102,106,112,104,100,119,57,57,110,112,50,50,112,50,49,119,112,113,107,50,48,48,47,98,105,110,47,103,114,101,112,10]},{"pos":2717,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,101,103,114,101,112,46,46,46,32,47,46,116,97,110,103,114,97,109,47,97,114,116,105,102,97,99,116,115,47,100,105,114,95,48,49,113,119,104,120,104,122,51,99,50,52,52,114,57,53,103,56,102,114,114,102,116,52,122,98,115,97,120,113,97,53,102,106,112,104,100,119,57,57,110,112,50,50,112,50,49,119,112,113,107,50,48,48,47,98,105,110,47,103,114,101,112,32,45,69,10,99,104,101,99,107,105,110,103,32,102,111,114,32,77,105,110,105,120,32,65,109,115,116,101,114,100,97,109,32,99,111,109,112,105,108,101,114,46,46,46,32]},{"pos":2871,"bytes":[110,111,10]},{"pos":2874,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,97,114,46,46,46,32,97,114,10,99,104,101,99,107,105,110,103,32,102,111,114,32,114,97,110,108,105,98,46,46,46,32,114,97,110,108,105,98,10,99,104,101,99,107,105,110,103,32,102,111,114,32,103,99,99,32,111,112,116,105,111,110,32,116,111,32,101,110,97,98,108,101,32,108,97,114,103,101,32,102,105,108,101,32,115,117,112,112,111,114,116,46,46,46,32]},{"pos":2982,"bytes":[110,111,110,101,32,110,101,101,100,101,100,10]},{"pos":2994,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,115,105,122,101,95,116,46,46,46,32]},{"pos":3017,"bytes":[121,101,115,10,99,104,101,99,107,105,110,103,32,102,111,114,32,119,111,114,107,105,110,103,32,97,108,108,111,99,97,46,104,46,46,46,32]},{"pos":3054,"bytes":[121,101,115,10]},{"pos":3058,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,97,108,108,111,99,97,46,46,46,32,121,101,115,10,99,104,101,99,107,105,110,103,32,102,111,114,32,101,97,99,99,101,115,115,46,46,46,32]},{"pos":3109,"bytes":[121,101,115,10]},{"pos":3113,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,103,99,99,32,111,112,116,105,111,110,115,32,110,101,101,100,101,100,32,116,111,32,100,101,116,101,99,116,32,97,108,108,32,117,110,100,101,99,108,97,114,101,100,32,102,117,110,99,116,105,111,110,115,46,46,46,32]},{"pos":3183,"bytes":[110,111,110,101,32,110,101,101,100,101,100,10]},{"pos":3195,"bytes":[99,104,101,99,107,105,110,103,32,104,111,115,116,32,67,80,85,32,97,110,100,32,67,32,65,66,73,46,46,46,32]},{"pos":3226,"bytes":[120,56,54,95,54,52,10]},{"pos":3233,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,67,32,99,111,109,112,105,108,101,114,32,111,112,116,105,111,110,32,116,111,32,97,108,108,111,119,32,119,97,114,110,105,110,103,115,46,46,46,32]},{"pos":3285,"bytes":[45,87,110,111,45,101,114,114,111,114,10]},{"pos":3296,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,97,108,108,111,99,97,32,97,115,32,97,32,99,111,109,112,105,108,101,114,32,98,117,105,108,116,45,105,110,46,46,46,32]},{"pos":3342,"bytes":[121,101,115,10]},{"pos":3346,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,103,101,116,108,111,97,100,97,118,103,46,46,46,32]},{"pos":3373,"bytes":[121,101,115,10]},{"pos":3377,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,115,121,115,47,108,111,97,100,97,118,103,46,104,46,46,46,32]},{"pos":3407,"bytes":[110,111,10]},{"pos":3410,"bytes":[99,104,101,99,107,105,110,103,32,119,104,101,116,104,101,114,32,103,101,116,108,111,97,100,97,118,103,32,105,115,32,100,101,99,108,97,114,101,100,46,46,46,32]},{"pos":3453,"bytes":[121,101,115,10]},{"pos":3457,"bytes":[99,104,101,99,107,105,110,103,32,105,102,32,115,121,115,116,101,109,32,108,105,98,99,32,104,97,115,32,119,111,114,107,105,110,103,32,71,78,85,32,103,108,111,98,46,46,46,32]},{"pos":3505,"bytes":[110,111,10]},{"pos":3508,"bytes":[99,104,101,99,107,105,110,103,32,119,104,101,116,104,101,114,32,115,121,115,116,101,109,32,117,115,101,115,32,77,83,68,79,83,45,115,116,121,108,101,32,112,97,116,104,115,46,46,46,32]},{"pos":3558,"bytes":[110,111,10]},{"pos":3561,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,98,111,111,108,44,32,116,114,117,101,44,32,102,97,108,115,101,46,46,46,32]},{"pos":3595,"bytes":[110,111,10]},{"pos":3598,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,114,97,110,108,105,98,46,46,46,32,40,99,97,99,104,101,100,41,32,114,97,110,108,105,98,10,99,104,101,99,107,105,110,103,32,104,111,119,32,116,111,32,114,117,110,32,116,104,101,32,67,32,112,114,101,112,114,111,99,101,115,115,111,114,46,46,46,32,103,99,99,32,45,69,10]},{"pos":3686,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,97,114,46,46,46,32]},{"pos":3705,"bytes":[97,114,10,99,104,101,99,107,105,110,103,32,102,111,114,32,112,101,114,108,46,46,46,32,112,101,114,108,10,99,104,101,99,107,105,110,103,32,119,104,101,116,104,101,114,32,98,121,116,101,32,111,114,100,101,114,105,110,103,32,105,115,32,98,105,103,101,110,100,105,97,110,46,46,46,32]},{"pos":3781,"bytes":[110,111,10,99,104,101,99,107,105,110,103,32,102,111,114,32,97,32,115,101,100,32,116,104,97,116,32,100,111,101,115,32,110,111,116,32,116,114,117,110,99,97,116,101,32,111,117,116,112,117,116,46,46,46,32]},{"pos":3836,"bytes":[47,46,116,97,110,103,114,97,109,47,97,114,116,105,102,97,99,116,115,47,100,105,114,95,48,49,113,119,104,120,104,122,51,99,50,52,52,114,57,53,103,56,102,114,114,102,116,52,122,98,115,97,120,113,97,53,102,106,112,104,100,119,57,57,110,112,50,50,112,50,49,119,112,113,107,50,48,48,47,98,105,110,47,115,101,100,10]},{"pos":3923,"bytes":[99,104,101,99,107,105,110,103,32,119,104,101,116,104,101,114,32,78,76,83,32,105,115,32,114,101,113,117,101,115,116,101,100,46,46,46,32,121,101,115,10]},{"pos":3964,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,109,115,103,102,109,116,46,46,46,32]},{"pos":3987,"bytes":[110,111,10,99,104,101,99,107,105,110,103,32,102,111,114,32,103,109,115,103,102,109,116,46,46,46,32,58,10]},{"pos":4016,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,120,103,101,116,116,101,120,116,46,46,46,32]},{"pos":4041,"bytes":[110,111,10]},{"pos":4044,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,109,115,103,109,101,114,103,101,46,46,46,32]},{"pos":4069,"bytes":[110,111,10,99,104,101,99,107,105,110,103,32,102,111,114,32,108,100,32,117,115,101,100,32,98,121,32,103,99,99,46,46,46,32]},{"pos":4103,"bytes":[47,46,116,97,110,103,114,97,109,47,97,114,116,105,102,97,99,116,115,47,100,105,114,95,48,49,98,50,110,50,99,118,49,118,113,48,112,116,54,120,104,119,115,51,107,51,106,98,55,99,100,98,54,116,114,116,48,50,114,56,107,121,99,119,103,120,119,103,121,55,98,112,102,118,110,102,101,48,47,108,100,10]},{"pos":4185,"bytes":[99,104,101,99,107,105,110,103,32,105,102,32,116,104,101,32,108,105,110,107,101,114,32,40,47,46,116,97,110,103,114,97,109,47,97,114,116,105,102,97,99,116,115,47,100,105,114,95,48,49,98,50,110,50,99,118,49,118,113,48,112,116,54,120,104,119,115,51,107,51,106,98,55,99,100,98,54,116,114,116,48,50,114,56,107,121,99,119,103,120,119,103,121,55,98,112,102,118,110,102,101,48,47,108,100,41,32,105,115,32,71,78,85,32,108,100,46,46,46,32]},{"pos":4305,"bytes":[121,101,115,10]},{"pos":4309,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,115,104,97,114,101,100,32,108,105,98,114,97,114,121,32,114,117,110,32,112,97,116,104,32,111,114,105,103,105,110,46,46,46,32]},{"pos":4356,"bytes":[100,111,110,101,10]},{"pos":4361,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,67,70,80,114,101,102,101,114,101,110,99,101,115,67,111,112,121,65,112,112,86,97,108,117,101,46,46,46,32]},{"pos":4403,"bytes":[110,111,10]},{"pos":4406,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,67,70,76,111,99,97,108,101,67,111,112,121,67,117,114,114,101,110,116,46,46,46,32]},{"pos":4442,"bytes":[110,111,10]},{"pos":4445,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,71,78,85,32,103,101,116,116,101,120,116,32,105,110,32,108,105,98,99,46,46,46,32]},{"pos":4481,"bytes":[110,111,10]},{"pos":4484,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,105,99,111,110,118,46,46,46,32]},{"pos":4506,"bytes":[121,101,115,10,99,104,101,99,107,105,110,103,32,102,111,114,32,119,111,114,107,105,110,103,32,105,99,111,110,118,46,46,46,32]},{"pos":4540,"bytes":[121,101,115,10]},{"pos":4544,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,71,78,85,32,103,101,116,116,101,120,116,32,105,110,32,108,105,98,105,110,116,108,46,46,46,32]},{"pos":4583,"bytes":[110,111,10]},{"pos":4586,"bytes":[99,104,101,99,107,105,110,103,32,119,104,101,116,104,101,114,32,116,111,32,117,115,101,32,78,76,83,46,46,46,32,110,111,10,99,104,101,99,107,105,110,103,32,102,111,114,32,108,105,98,114,97,114,121,32,99,111,110,116,97,105,110,105,110,103,32,115,116,114,101,114,114,111,114,46,46,46,32]},{"pos":4664,"bytes":[110,111,110,101,32,114,101,113,117,105,114,101,100,10]},{"pos":4678,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,108,105,98,114,97,114,121,32,99,111,110,116,97,105,110,105,110,103,32,103,101,116,112,119,110,97,109,46,46,46,32]},{"pos":4722,"bytes":[110,111,110,101,32,114,101,113,117,105,114,101,100,10]},{"pos":4736,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,100,105,114,101,110,116,46,104,32,116,104,97,116,32,100,101,102,105,110,101,115,32,68,73,82,46,46,46,32]},{"pos":4778,"bytes":[121,101,115,10]},{"pos":4782,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,108,105,98,114,97,114,121,32,99,111,110,116,97,105,110,105,110,103,32,111,112,101,110,100,105,114,46,46,46,32]},{"pos":4825,"bytes":[110,111,110,101,32,114,101,113,117,105,114,101,100,10]},{"pos":4839,"bytes":[99,104,101,99,107,105,110,103,32,119,104,101,116,104,101,114,32,115,116,97,116,32,102,105,108,101,45,109,111,100,101,32,109,97,99,114,111,115,32,97,114,101,32,98,114,111,107,101,110,46,46,46,32]},{"pos":4892,"bytes":[110,111,10]},{"pos":4895,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,115,116,100,108,105,98,46,104,46,46,46,32,40,99,97,99,104,101,100,41,32,121,101,115,10,99,104,101,99,107,105,110,103,32,102,111,114,32,115,116,114,105,110,103,46,104,46,46,46,32,40,99,97,99,104,101,100,41,32,121,101,115,10,99,104,101,99,107,105,110,103,32,102,111,114,32,115,116,114,105,110,103,115,46,104,46,46,46,32,40,99,97,99,104,101,100,41,32,121,101,115,10,99,104,101,99,107,105,110,103,32,102,111,114,32,108,111,99,97,108,101,46,104,46,46,46,32]},{"pos":5035,"bytes":[121,101,115,10]},{"pos":5039,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,117,110,105,115,116,100,46,104,46,46,46,32,40,99,97,99,104,101,100,41,32,121,101,115,10,99,104,101,99,107,105,110,103,32,102,111,114,32,108,105,109,105,116,115,46,104,46,46,46,32]},{"pos":5102,"bytes":[121,101,115,10]},{"pos":5106,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,109,101,109,111,114,121,46,104,46,46,46,32]},{"pos":5131,"bytes":[121,101,115,10]},{"pos":5135,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,115,121,115,47,112,97,114,97,109,46,104,46,46,46,32,40,99,97,99,104,101,100,41,32,121,101,115,10,99,104,101,99,107,105,110,103,32,102,111,114,32,115,121,115,47,114,101,115,111,117,114,99,101,46,104,46,46,46,32]},{"pos":5207,"bytes":[121,101,115,10]},{"pos":5211,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,115,121,115,47,116,105,109,101,98,46,104,46,46,46,32]},{"pos":5239,"bytes":[121,101,115,10]},{"pos":5243,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,115,121,115,47,116,105,109,101,46,104,46,46,46,32]},{"pos":5270,"bytes":[121,101,115,10]},{"pos":5274,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,115,121,115,47,115,101,108,101,99,116,46,104,46,46,46,32]},{"pos":5303,"bytes":[121,101,115,10]},{"pos":5307,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,115,121,115,47,102,105,108,101,46,104,46,46,46,32]},{"pos":5334,"bytes":[121,101,115,10]},{"pos":5338,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,102,99,110,116,108,46,104,46,46,46,32]},{"pos":5362,"bytes":[121,101,115,10]},{"pos":5366,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,115,112,97,119,110,46,104,46,46,46,32]},{"pos":5390,"bytes":[121,101,115,10]},{"pos":5394,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,97,110,32,65,78,83,73,32,67,45,99,111,110,102,111,114,109,105,110,103,32,99,111,110,115,116,46,46,46,32]},{"pos":5437,"bytes":[121,101,115,10,99,104,101,99,107,105,110,103,32,102,111,114,32,117,105,100,95,116,32,105,110,32,115,121,115,47,116,121,112,101,115,46,104,46,46,46,32]},{"pos":5478,"bytes":[121,101,115,10]},{"pos":5482,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,112,105,100,95,116,46,46,46,32]},{"pos":5504,"bytes":[121,101,115,10]},{"pos":5508,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,111,102,102,95,116,46,46,46,32]},{"pos":5530,"bytes":[121,101,115,10]},{"pos":5534,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,115,105,122,101,95,116,46,46,46,32,40,99,97,99,104,101,100,41,32,121,101,115,10,99,104,101,99,107,105,110,103,32,102,111,114,32,115,115,105,122,101,95,116,46,46,46,32]},{"pos":5594,"bytes":[121,101,115,10]},{"pos":5598,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,117,110,115,105,103,110,101,100,32,108,111,110,103,32,108,111,110,103,32,105,110,116,46,46,46,32]},{"pos":5637,"bytes":[121,101,115,10]},{"pos":5641,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,108,111,110,103,32,108,111,110,103,32,105,110,116,46,46,46,32]},{"pos":5671,"bytes":[121,101,115,10]},{"pos":5675,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,105,110,116,109,97,120,95,116,46,46,46,32]},{"pos":5700,"bytes":[121,101,115,10]},{"pos":5704,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,117,105,110,116,109,97,120,95,116,46,46,46,32]},{"pos":5730,"bytes":[121,101,115,10]},{"pos":5734,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,115,105,103,95,97,116,111,109,105,99,95,116,46,46,46,32]},{"pos":5763,"bytes":[121,101,115,10]},{"pos":5767,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,110,97,110,111,115,101,99,111,110,100,115,32,102,105,101,108,100,32,111,102,32,115,116,114,117,99,116,32,115,116,97,116,46,46,46,32]},{"pos":5816,"bytes":[115,116,95,109,116,105,109,46,116,118,95,110,115,101,99,10]},{"pos":5832,"bytes":[99,104,101,99,107,105,110,103,32,119,104,101,116,104,101,114,32,116,111,32,117,115,101,32,104,105,103,104,32,114,101,115,111,108,117,116,105,111,110,32,102,105,108,101,32,116,105,109,101,115,116,97,109,112,115,46,46,46,32]},{"pos":5891,"bytes":[121,101,115,10]},{"pos":5895,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,108,105,98,114,97,114,121,32,99,111,110,116,97,105,110,105,110,103,32,99,108,111,99,107,95,103,101,116,116,105,109,101,46,46,46,32]},{"pos":5944,"bytes":[110,111,110,101,32,114,101,113,117,105,114,101,100,10]},{"pos":5958,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,115,116,97,110,100,97,114,100,32,103,101,116,116,105,109,101,111,102,100,97,121,46,46,46,32]},{"pos":5996,"bytes":[121,101,115,10]},{"pos":6000,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,115,116,114,116,111,108,108,46,46,46,32]},{"pos":6024,"bytes":[121,101,115,10]},{"pos":6028,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,115,116,114,100,117,112,46,46,46,32]},{"pos":6051,"bytes":[121,101,115,10]},{"pos":6055,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,115,116,114,110,100,117,112,46,46,46,32]},{"pos":6079,"bytes":[121,101,115,10]},{"pos":6083,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,115,116,112,99,112,121,46,46,46,32]},{"pos":6106,"bytes":[121,101,115,10]},{"pos":6110,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,109,101,109,114,99,104,114,46,46,46,32]},{"pos":6134,"bytes":[121,101,115,10]},{"pos":6138,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,109,101,109,112,99,112,121,46,46,46,32]},{"pos":6162,"bytes":[121,101,115,10]},{"pos":6166,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,117,109,97,115,107,46,46,46,32]},{"pos":6188,"bytes":[121,101,115,10]},{"pos":6192,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,109,107,115,116,101,109,112,46,46,46,32]},{"pos":6216,"bytes":[121,101,115,10]},{"pos":6220,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,109,107,116,101,109,112,46,46,46,32]},{"pos":6243,"bytes":[121,101,115,10]},{"pos":6247,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,102,100,111,112,101,110,46,46,46,32]},{"pos":6270,"bytes":[121,101,115,10]},{"pos":6274,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,100,117,112,46,46,46,32]},{"pos":6294,"bytes":[121,101,115,10]},{"pos":6298,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,100,117,112,50,46,46,46,32]},{"pos":6319,"bytes":[121,101,115,10]},{"pos":6323,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,103,101,116,99,119,100,46,46,46,32]},{"pos":6346,"bytes":[121,101,115,10]},{"pos":6350,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,114,101,97,108,112,97,116,104,46,46,46,32]},{"pos":6375,"bytes":[121,101,115,10]},{"pos":6379,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,115,105,103,115,101,116,109,97,115,107,46,46,46,32]},{"pos":6406,"bytes":[110,111,10]},{"pos":6409,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,115,105,103,97,99,116,105,111,110,46,46,46,32]},{"pos":6435,"bytes":[121,101,115,10]},{"pos":6439,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,103,101,116,103,114,111,117,112,115,46,46,46,32]},{"pos":6465,"bytes":[121,101,115,10]},{"pos":6469,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,115,101,116,101,117,105,100,46,46,46,32]},{"pos":6493,"bytes":[121,101,115,10]},{"pos":6497,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,115,101,116,101,103,105,100,46,46,46,32]},{"pos":6521,"bytes":[121,101,115,10]},{"pos":6525,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,115,101,116,108,105,110,101,98,117,102,46,46,46,32]},{"pos":6552,"bytes":[121,101,115,10]},{"pos":6556,"bytes":[99,104,101,99,107,105,110,103,32,102,111,114,32,115,101,116,114,101,117,105,100,46,46,46,32]}]
		"#;
		let chunks: Vec<tg::build::LogChunk> = serde_json::from_str(chunks).unwrap();
		let height = 2;
		let width = 20;

		let mut iter = Scroll::new(
			width,
			chunks.len() - 1,
			chunks.last().unwrap().bytes.len(),
			&chunks,
		);
		iter.scroll_up(height, &chunks);
		let lines = iter.read_lines(height, &chunks);
		assert_eq!(&lines[0], "checking for setreui");
		assert_eq!(&lines[1], "d... ");
	}

	#[allow(clippy::too_many_lines)]
	#[test]
	fn tailing() {
		let chunks = vec![
			tg::build::LogChunk {
				pos: 0,
				bytes: b"\"log line 0\"\n".to_vec().into(),
			},
			tg::build::LogChunk {
				pos: 13,
				bytes: b"\"log line 1\"\n".to_vec().into(),
			},
			tg::build::LogChunk {
				pos: 26,
				bytes: b"\"log line 2\"\n".to_vec().into(),
			},
			tg::build::LogChunk {
				pos: 39,
				bytes: b"\"log line 3\"\n".to_vec().into(),
			},
			tg::build::LogChunk {
				pos: 52,
				bytes: b"\"log line 4\"\n".to_vec().into(),
			},
			tg::build::LogChunk {
				pos: 65,
				bytes: b"\"log line 5\"\n".to_vec().into(),
			},
			tg::build::LogChunk {
				pos: 78,
				bytes: b"\"log line 6\"\n".to_vec().into(),
			},
			tg::build::LogChunk {
				pos: 91,
				bytes: b"\"log line 7\"\n".to_vec().into(),
			},
			tg::build::LogChunk {
				pos: 104,
				bytes: b"\"log line 8\"\n".to_vec().into(),
			},
			tg::build::LogChunk {
				pos: 117,
				bytes: b"\"log line 9\"\n".to_vec().into(),
			},
			tg::build::LogChunk {
				pos: 130,
				bytes: b"\"log line 10\"\n".to_vec().into(),
			},
			tg::build::LogChunk {
				pos: 144,
				bytes: b"\"log line 11\"\n".to_vec().into(),
			},
			tg::build::LogChunk {
				pos: 158,
				bytes: b"\"log line 12\"\n".to_vec().into(),
			},
			tg::build::LogChunk {
				pos: 172,
				bytes: b"\"log line 13\"\n".to_vec().into(),
			},
			tg::build::LogChunk {
				pos: 186,
				bytes: b"\"log line 14\"\n".to_vec().into(),
			},
			tg::build::LogChunk {
				pos: 200,
				bytes: b"\"log line 15\"\n".to_vec().into(),
			},
			tg::build::LogChunk {
				pos: 214,
				bytes: b"\"log line 16\"\n".to_vec().into(),
			},
			tg::build::LogChunk {
				pos: 228,
				bytes: b"\"log line 17\"\n".to_vec().into(),
			},
			tg::build::LogChunk {
				pos: 242,
				bytes: b"\"log line 18\"\n".to_vec().into(),
			},
			tg::build::LogChunk {
				pos: 256,
				bytes: b"\"log line 19\"\n".to_vec().into(),
			},
			tg::build::LogChunk {
				pos: 270,
				bytes: b"\"log line 20\"\n".to_vec().into(),
			},
			tg::build::LogChunk {
				pos: 284,
				bytes: b"\"log line 21\"\n".to_vec().into(),
			},
			tg::build::LogChunk {
				pos: 298,
				bytes: b"\"log line 22\"\n".to_vec().into(),
			},
			tg::build::LogChunk {
				pos: 312,
				bytes: b"\"log line 23\"\n".to_vec().into(),
			},
		];
		let height = 40;
		let width = 189;
		let chunk = chunks.len() - 1;
		let byte = chunks.last().unwrap().bytes.len() - 1;
		let mut scroll = Scroll::new(width, chunk, byte, &chunks);
		scroll.scroll_up(height, &chunks);
		let lines = scroll.read_lines(height, &chunks);
		assert_eq!(lines.len(), chunks.len());
	}

	#[test]
	fn simple_tailing() {
		let chunks = vec![
			tg::build::LogChunk {
				pos: 0,
				bytes: b"\"0\"\n".to_vec().into(),
			},
			tg::build::LogChunk {
				pos: 4,
				bytes: b"\"1\"\n".to_vec().into(),
			},
			tg::build::LogChunk {
				pos: 8,
				bytes: b"\"2\"\n".to_vec().into(),
			},
			tg::build::LogChunk {
				pos: 12,
				bytes: b"\"3\"\n".to_vec().into(),
			},
		];
		let mut scroll = Scroll::new(
			10,
			chunks.len() - 1,
			chunks.last().unwrap().bytes.len(),
			&chunks,
		);
		scroll.scroll_up(3, &chunks);
		assert_eq!(scroll.chunk, 2);
		assert_eq!(scroll.byte, 0);
		let lines = scroll.read_lines(3, &chunks);
		assert_eq!(&lines[0], "\"2\"");
		assert_eq!(&lines[1], "\"3\"");
	}

	#[test]
	fn scroll_up() {
		use tg::build::LogChunk;
		let chunks = [
			LogChunk {
				pos: 0,
				bytes: b"\"0\"\n".to_vec().into(),
			},
			LogChunk {
				pos: 4,
				bytes: b"\"1\"\n".to_vec().into(),
			},
			LogChunk {
				pos: 8,
				bytes: b"\"2\"\n".to_vec().into(),
			},
			LogChunk {
				pos: 12,
				bytes: b"\"3\"\n".to_vec().into(),
			},
			LogChunk {
				pos: 16,
				bytes: b"\"4\"\n".to_vec().into(),
			},
			LogChunk {
				pos: 20,
				bytes: b"\"5\"\n".to_vec().into(),
			},
			LogChunk {
				pos: 24,
				bytes: b"\"6\"\n".to_vec().into(),
			},
			LogChunk {
				pos: 28,
				bytes: b"\"7\"\n".to_vec().into(),
			},
			LogChunk {
				pos: 32,
				bytes: b"\"8\"\n".to_vec().into(),
			},
			LogChunk {
				pos: 36,
				bytes: b"\"9\"\n".to_vec().into(),
			},
			LogChunk {
				pos: 40,
				bytes: b"\"10\"\n".to_vec().into(),
			},
			LogChunk {
				pos: 45,
				bytes: b"\"11\"\n".to_vec().into(),
			},
			LogChunk {
				pos: 50,
				bytes: b"\"12\"\n".to_vec().into(),
			},
			LogChunk {
				pos: 55,
				bytes: b"\"13\"\n".to_vec().into(),
			},
			LogChunk {
				pos: 60,
				bytes: b"\"14\"\n".to_vec().into(),
			},
			LogChunk {
				pos: 65,
				bytes: b"\"15\"\n".to_vec().into(),
			},
			LogChunk {
				pos: 70,
				bytes: b"\"16\"\n".to_vec().into(),
			},
			LogChunk {
				pos: 75,
				bytes: b"\"17\"\n".to_vec().into(),
			},
			LogChunk {
				pos: 80,
				bytes: b"\"18\"\n".to_vec().into(),
			},
			LogChunk {
				pos: 85,
				bytes: b"\"19\"\n".to_vec().into(),
			},
			LogChunk {
				pos: 90,
				bytes: b"\"20\"\n".to_vec().into(),
			},
		];
		let mut inner = Scroll::new(
			40,
			chunks.len() - 1,
			chunks.last().unwrap().bytes.len(),
			&chunks,
		);
		let height = 6;
		inner.scroll_up(height + 2, &chunks);
	}
}
