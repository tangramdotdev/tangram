use crossterm as ct;
use futures::{stream::FusedStream, StreamExt, TryStreamExt};
use num::ToPrimitive;
use ratatui as tui;
use std::{
	collections::VecDeque,
	sync::{Arc, Weak},
};
use tangram_client as tg;
use tangram_error::{Result, WrapErr};
use tg::package::Ext;
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
	entries: tokio::sync::Mutex<Vec<LogEntry>>,

	// The bounding box of the log view.
	rect: tokio::sync::watch::Sender<Rect>,

	// Channel used to send UI events.
	sender: tokio::sync::mpsc::UnboundedSender<LogUpdate>,

	// The lines of text that will be displayed.
	lines: std::sync::Mutex<VecDeque<String>>,

	// The log's task
	task: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,

	// The client.
	tg: Box<dyn tg::Handle>,
}

// Represents the current state of log's scroll, pointing to a newline character within a log entry.
#[derive(Clone, Copy, Debug)]
struct Scroll {
	entry: usize,
	newline: Option<usize>,
}

#[derive(Debug)]
struct LogEntry {
	entry: tg::log::Entry,
	newlines: Vec<usize>,
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
		let log = Log::new(tg.as_ref(), build, rect);
		let root = TreeItem::new(tg.as_ref(), build, None, 0, true);
		root.expand();
		let tree = Tree::new(root, rect);
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
	let metadata = package.metadata(tg).await?;

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

impl From<tg::log::Entry> for LogEntry {
	fn from(entry: tg::log::Entry) -> Self {
		let newlines = entry
			.bytes
			.iter()
			.enumerate()
			.filter_map(|(pos, byte)| (*byte == b'\n').then_some(pos))
			.collect::<Vec<_>>();
		Self { entry, newlines }
	}
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
	fn new(tg: &dyn tg::Handle, build: &tg::Build, rect: Rect) -> Self {
		let build = build.clone();
		let entries = tokio::sync::Mutex::new(Vec::new());
		let tg = tg.clone_box();

		let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
		let (rect, mut rect_watch) = tokio::sync::watch::channel(rect);
		let lines = std::sync::Mutex::new(VecDeque::new());

		let log = Log {
			inner: Arc::new(LogInner {
				build,
				entries,
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
			let mut scroll = None;
			async move {
				let area = log.rect().area().to_i64().unwrap();
				let stream = log
					.inner
					.build
					.log(log.inner.tg.as_ref(), None, Some(-3 * area / 2))
					.await
					.expect("Failed to get log stream.");

				let mut stream = stream.fuse();
				loop {
					tokio::select! {
						Some(entry) = stream.next(), if !stream.is_terminated() => {
							let Ok(entry) = entry else {
								return;
							};
							log.add_entry(entry.into()).await;
						},
						result = receiver.recv() => match result.unwrap() {
							LogUpdate::Down => {
								let result = log.down_impl(&mut scroll).await;
								if result.is_err() {
									return;
								}
							}
							LogUpdate::Up => {
								let result = log.up_impl(&mut scroll).await;
								if result.is_err() {
									return;
								}
							}
						},
						_ = rect_watch.changed() => (),
					};
					log.update_lines(scroll).await;
				}
			}
		});
		log.inner.task.lock().unwrap().replace(task);
		log
	}

	// Log a new entry from the stream.
	async fn add_entry(&self, entry: LogEntry) {
		// Get some metadata about this log entry.
		let entry_position = entry.entry.pos;
		let entry_size = entry.entry.bytes.len().to_u64().unwrap();

		let mut entries = self.inner.entries.lock().await;

		// Short circuit for the common case that we're appending to the log.
		if entries.is_empty() || entries.last().unwrap().entry.pos < entry.entry.pos {
			entries.push(entry);
			return;
		};

		// Find where this log entry needs to be inserted.
		let index = entries
			.iter()
			.position(|existing| existing.entry.pos > entry.entry.pos)
			.unwrap();
		entries.insert(index, entry);

		// Check if we need to truncate the next entry.
		let next_pos = entry_position + entry_size;
		let next_entry = &mut entries[index + 1];
		if next_pos > next_entry.entry.pos {
			let new_length = next_entry.entry.bytes.len()
				- (next_pos - next_entry.entry.pos).to_usize().unwrap();
			next_entry.entry.bytes.truncate(new_length);
			next_entry.newlines = next_entry
				.entry
				.bytes
				.iter()
				.enumerate()
				.filter_map(|(pos, byte)| (*byte == b'\n').then_some(pos))
				.collect::<Vec<_>>();
		}
	}

	// Handle a scroll up event.
	async fn up_impl(&self, scroll: &mut Option<Scroll>) -> Result<()> {
		let Some(scroll) = scroll.as_mut() else {
			let entries = self.inner.entries.lock().await;
			let lines = self.inner.lines.lock().unwrap();
			if lines.is_empty() {
				return Ok(());
			}
			*scroll = Some(get_nth_newline_from_end(
				&entries,
				self.rect().height.to_usize().unwrap() + 1,
			));
			return Ok(());
		};

		// If we've scrolled to the beginning of the stream, pull in some new results and find the first newline.
		if scroll.entry == 0 && scroll.newline.is_none() {
			let pos = self.inner.entries.lock().await[scroll.entry].entry.pos;
			if pos == 0 {
				// Nothing to do.
				return Ok(());
			}
			let len = 3 * self.rect().area().to_i64().unwrap() / 2;
			let stream = self
				.inner
				.build
				.log(self.inner.tg.as_ref(), Some(pos), Some(len))
				.await?;
			let entries = stream
				.try_collect::<Vec<_>>()
				.await?
				.into_iter()
				.map(LogEntry::from)
				.collect::<Vec<_>>();

			// Update the scroll. Since this is prepended to the entries, the entry index is stable.
			*scroll = get_nth_newline_from_end(&entries, 1);
			for entry in entries {
				self.add_entry(entry).await;
			}
			return Ok(());
		}

		if scroll.newline == Some(0) {
			if scroll.entry == 0 {
				scroll.newline = None;
			} else {
				let entries = self.inner.entries.lock().await;
				*scroll = get_nth_newline_from_end(&entries[0..scroll.entry], 1);
			}
		}
		Ok(())
	}

	// Handle a scroll down event.
	async fn down_impl(&self, scroll: &mut Option<Scroll>) -> Result<()> {
		// We're tailing, no work to do.
		let Some(scroll_state) = *scroll else {
			return Ok(());
		};

		let entries = self.inner.entries.lock().await;
		let height = self.rect().height.to_usize().unwrap();

		// Check if we need to start tailing.
		let bottom = get_nth_newline_from_start(&entries[scroll_state.entry..], height + 1);
		if bottom.newline.is_none() {
			*scroll = None;
		} else {
			// Otherwise, scroll by one line.
			let mut top = get_nth_newline_from_start(&entries[scroll_state.entry..], 1);
			top.entry += scroll_state.entry + 1;
			*scroll = Some(top);
		}
		Ok(())
	}

	// Update the rendered lines.
	async fn update_lines(&self, scroll: Option<Scroll>) {
		let entries = self.inner.entries.lock().await;
		if entries.is_empty() {
			self.inner.lines.lock().unwrap().clear();
			return;
		}
		let height = self.rect().height.to_usize().unwrap();
		let scroll = scroll.unwrap_or_else(|| get_nth_newline_from_end(&entries, height));
		let lines = read_lines(&entries, height, scroll.entry, scroll.newline);
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

// Read through entries starting from the start and return the indices of the entry containing the nth newline character, and the index of that character in the newline table.
// Preconditions: entries must be non empty and n must be non zero.
fn get_nth_newline_from_start(entries: &[LogEntry], n: usize) -> Scroll {
	let mut entry_index = 0;
	let mut count = 0;
	loop {
		let entry = &entries[entry_index];
		if entry_index == entries.len() - 1 && entry.newlines.is_empty() {
			return Scroll {
				entry: entry_index,
				newline: None,
			};
		}
		count += entry.newlines.len();
		if count >= n || entry_index == entries.len() - 1 {
			break;
		}
		entry_index += 1;
	}
	if count < n {
		return Scroll {
			entry: entry_index,
			newline: None,
		};
	}
	let newline_index = (entries[entry_index].newlines.len() - 1) - (count - n);
	Scroll {
		entry: entry_index,
		newline: Some(newline_index),
	}
}

// Read through entries starting from the end and return the indices of the entry containing the nth newline character, and the index of that character in the newline table.
// Preconditions: entries must be non empty and n must be nonzero.
fn get_nth_newline_from_end(entries: &[LogEntry], n: usize) -> Scroll {
	// Get the first entry with a newline.
	let mut entry_index = entries.len() - 1;
	let mut count = 0;
	loop {
		let entry = &entries[entry_index];
		if entry_index == 0 && entry.newlines.is_empty() {
			return Scroll {
				entry: entry_index,
				newline: None,
			};
		}
		count += entry.newlines.len();
		if count >= n || entry_index == 0 {
			break;
		}
		entry_index -= 1;
	}
	if count < n {
		return Scroll {
			entry: 0,
			newline: None,
		};
	}
	let newline_index = count - n;
	Scroll {
		entry: entry_index,
		newline: Some(newline_index),
	}
}

// Read `count` many lines of text from `entries` starting at a given entry and new line offset.
fn read_lines(
	entries: &[LogEntry],
	count: usize,
	mut entry_index: usize,
	mut line_index: Option<usize>,
) -> VecDeque<String> {
	// Read <count> number of lines starting at <entry_index> and <line_index>.
	let mut lines = VecDeque::new();

	// Edge case: handling the first line of the first entry.
	if line_index.is_none() && !entries[entry_index].newlines.is_empty() {
		let entry = &entries[entry_index];
		let end = entry.newlines.first().unwrap();
		let mut line = String::new();
		append_log_to_string(&mut line, &entry.entry.bytes[0..*end]);
		lines.push_back(line);
		line_index = Some(0);
	}

	// Read lines until the buffer is filled.
	'outer: while lines.len() < count {
		let mut next_line = String::new();
		'inner: loop {
			let entry = &entries[entry_index];
			if let Some(current_line_index) = line_index {
				let start = entry.newlines[current_line_index] + 1;
				if let Some(end) = entry.newlines.get(current_line_index + 1) {
					append_log_to_string(&mut next_line, &entry.entry.bytes[start..*end]);
					line_index = Some(current_line_index + 1);
					break 'inner;
				}
				append_log_to_string(&mut next_line, &entry.entry.bytes[start..]);
				entry_index += 1;
				if entry_index == entries.len() {
					lines.push_back(next_line);
					break 'outer;
				}
				let entry = &entries[entry_index];
				if let Some(first_line) = entry.newlines.first() {
					append_log_to_string(&mut next_line, &entry.entry.bytes[0..*first_line]);
					line_index = Some(0);
					break 'inner;
				}
				line_index = None;
			} else {
				append_log_to_string(&mut next_line, &entry.entry.bytes);
				entry_index += 1;
				if entry_index == entries.len() {
					lines.push_back(next_line);
					break 'outer;
				}
				let entry = &entries[entry_index];
				if let Some(first_line) = entry.newlines.first() {
					append_log_to_string(&mut next_line, &entry.entry.bytes[0..*first_line]);
					line_index = Some(0);
					break 'inner;
				}
				line_index = None;
			}
		}
		lines.push_back(next_line);
	}
	lines
}

// Append a byte string to a utf8 string, converting non-ascii text to the unknown char.
fn append_log_to_string(string: &mut String, bytes: &[u8]) {
	let chars = bytes.iter().map(|byte| {
		let byte = *byte;
		if byte.is_ascii_control() {
			// Display a spaces for a tab character.
			if byte as char == '\t' {
				' '
			} else if byte as char == '\n' {
				byte as char
			} else {
				'�'
			}
		} else if byte.is_ascii() {
			byte as char
		} else {
			'�'
		}
	});
	string.extend(chars);
}

#[cfg(test)]
mod tests {
	use crate::tui::{get_nth_newline_from_end, get_nth_newline_from_start, read_lines, Scroll};
	use tangram_client as tg;

	#[test]
	fn read_overlapping_log_entries() {
		let entries = [
			tg::log::Entry {
				pos: 0,
				bytes: b"Line 1\nLine".to_vec().into(),
			},
			tg::log::Entry {
				pos: 0,
				bytes: b" ".to_vec().into(),
			},
			tg::log::Entry {
				pos: 0,
				bytes: b"2\nLine 3\n".to_vec().into(),
			},
			tg::log::Entry {
				pos: 0,
				bytes: b"Line 4\n".to_vec().into(),
			},
		];
		let entries = entries
			.into_iter()
			.map(super::LogEntry::from)
			.collect::<Vec<_>>();
		let lines = read_lines(&entries, 2, 0, Some(0));
		assert_eq!(lines.len(), 2);
		assert_eq!(&lines[0], "Line 2");
		assert_eq!(&lines[1], "Line 3");
	}

	#[test]
	fn get_nth_newline() {
		let entries: Vec<super::LogEntry> = vec![
			tg::log::Entry {
				pos: 0,
				bytes: b";;;;; Begin ;;;;;\n".to_vec().into(),
			}
			.into(),
			tg::log::Entry {
				pos: 0,
				bytes: b"Line 1\nLine 2\n".to_vec().into(),
			}
			.into(),
			tg::log::Entry {
				pos: 0,
				bytes: b"Line 3\n".to_vec().into(),
			}
			.into(),
			tg::log::Entry {
				pos: 0,
				bytes: b"Line 4\n".to_vec().into(),
			}
			.into(),
		];
		let entries = entries
			.into_iter()
			.map(super::LogEntry::from)
			.collect::<Vec<_>>();
		let Scroll { entry, newline } = get_nth_newline_from_start(&entries, 3);
		assert_eq!(entry, 1);
		assert_eq!(newline, Some(1));

		let Scroll { entry, newline } = get_nth_newline_from_end(&entries, 3);
		assert_eq!(entry, 1);
		assert_eq!(newline, Some(1));

		let Scroll { entry, newline } = get_nth_newline_from_end(&entries, 1);
		assert_eq!(entry, 3);
		assert_eq!(newline, Some(0));

		let Scroll { entry, newline } = get_nth_newline_from_end(&entries, 2);
		assert_eq!(entry, 2);
		assert_eq!(newline, Some(0));

		let Scroll { entry, newline } = get_nth_newline_from_start(&entries, 1);
		assert_eq!(entry, 0);
		assert_eq!(newline, Some(0));
	}

	#[test]
	fn read_full_log_from_end() {
		let entries: Vec<super::LogEntry> = vec![
			tg::log::Entry {
				pos: 0,
				bytes: b";;;;; Begin ;;;;;\n".to_vec().into(),
			}
			.into(),
			tg::log::Entry {
				pos: 0,
				bytes: b"Line 1\n".to_vec().into(),
			}
			.into(),
			tg::log::Entry {
				pos: 0,
				bytes: b"Line 2\n".to_vec().into(),
			}
			.into(),
			tg::log::Entry {
				pos: 0,
				bytes: b"Line 3\n".to_vec().into(),
			}
			.into(),
		];
		let height = 20;
		let scroll = get_nth_newline_from_end(&entries, height);
		let lines = read_lines(&entries, height, scroll.entry, scroll.newline);
		assert_eq!(&lines[0], ";;;;; Begin ;;;;;");
		assert_eq!(&lines[1], "Line 1");
		assert_eq!(&lines[2], "Line 2");
		assert_eq!(&lines[3], "Line 3");
		assert_eq!(&lines[4], "");
	}
}
