use crossterm as ct;
use either::Either;
use futures::{StreamExt, TryStreamExt};
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
	Empty,
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
	build: tg::Build,

	// A buffer of log chunks.
	chunks: tokio::sync::Mutex<Vec<tg::build::log::Chunk>>,

	// Whether we've reached eof or not.
	eof: std::sync::atomic::AtomicBool,

	// Channel used to send UI events.
	event_sender: tokio::sync::mpsc::UnboundedSender<LogEvent>,

	// The lines of text that will be displayed.
	lines: std::sync::Mutex<Vec<String>>,

	// The log streaming task.
	log_task: std::sync::Mutex<Option<tokio::task::JoinHandle<Result<()>>>>,

	// A watch to be notified when new logs are received from the log task.
	log_watch: tokio::sync::Mutex<Option<tokio::sync::watch::Receiver<()>>>,

	// The event handler task.
	event_task: std::sync::Mutex<Option<tokio::task::JoinHandle<Result<()>>>>,

	// The bounding box of the log view.
	rect: tokio::sync::watch::Sender<Rect>,

	// The current state of the log's scrolling position.
	scroll: tokio::sync::Mutex<Option<Scroll>>,

	// The handle.
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

enum LogEvent {
	ScrollUp,
	ScrollDown,
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
			indicator: TreeItemIndicator::Empty,
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
				let arg = tg::build::children::GetArg {
					position: Some(0),
					..Default::default()
				};
				let Ok(mut children) = item.inner.build.children(item.inner.tg.as_ref(), arg).await
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
			TreeItemIndicator::Empty => " ".red(),
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
		if let Some(task) = self.event_task.lock().unwrap().take() {
			task.abort();
		}
		if let Some(task) = self.log_task.lock().unwrap().take() {
			task.abort();
		}
	}
}

impl Log {
	#[allow(clippy::too_many_lines)]
	fn new(tg: &dyn tg::Handle, build: &tg::Build, rect: Rect) -> Self {
		let tg = tg.clone_box();
		let build = build.clone();
		let chunks = tokio::sync::Mutex::new(Vec::new());
		let (event_sender, mut event_receiver) = tokio::sync::mpsc::unbounded_channel();
		let lines = std::sync::Mutex::new(Vec::new());
		let (rect, mut rect_receiver) = tokio::sync::watch::channel(rect);

		let log = Log {
			inner: Arc::new(LogInner {
				build,
				chunks,
				eof: std::sync::atomic::AtomicBool::new(false),
				event_sender,
				event_task: std::sync::Mutex::new(None),
				lines,
				log_task: std::sync::Mutex::new(None),
				log_watch: tokio::sync::Mutex::new(None),
				rect,
				scroll: tokio::sync::Mutex::new(None),
				tg,
			}),
		};

		// Create the event handler task.
		let event_task = tokio::task::spawn({
			let log = log.clone();
			async move {
				log.update_log_stream(false).await?;
				log.update_log_stream(true).await?;
				loop {
					let log_receiver = log.inner.log_watch.lock().await.clone();
					let log_receiver = async move {
						if let Some(mut log_receiver) = log_receiver {
							log_receiver.changed().await.ok()
						} else {
							std::future::pending().await
						}
					};
					tokio::select! {
						event = event_receiver.recv() => match event.unwrap() {
							LogEvent::ScrollDown => {
								log.down_impl().await.ok();
							}
							LogEvent::ScrollUp => {
								log.up_impl().await.ok();
							}
						},
						_ = log_receiver => (),
						_ = rect_receiver.changed() => {
							let mut scroll = log.inner.scroll.lock().await;
							if let Some(scroll) = scroll.as_mut() {
								let rect = log.rect();
								scroll.width = rect.width.to_usize().unwrap();
							}
						},
					}
					log.update_lines().await?;
				}
			}
		});
		log.inner.event_task.lock().unwrap().replace(event_task);
		log
	}

	fn is_complete(&self) -> bool {
		self.inner.eof.load(std::sync::atomic::Ordering::SeqCst)
	}

	// Handle a scroll up event.
	async fn up_impl(&self) -> Result<()> {
		let height = if self.inner.scroll.lock().await.is_none() {
			let chunks = self.inner.chunks.lock().await;
			if chunks.is_empty() {
				return Ok(());
			}
			let rect = self.rect();
			let width = rect.width.to_usize().unwrap();

			let inner = Scroll::new(
				width,
				chunks.len() - 1,
				chunks.last().unwrap().bytes.len(),
				&chunks,
			);
			self.inner.scroll.lock().await.replace(inner);
			rect.height.to_usize().unwrap() + 3 // height + 1 + 2
		} else {
			2
		};

		loop {
			let chunks = self.inner.chunks.lock().await;
			let mut scroll = self.inner.scroll.lock().await;
			match scroll.as_mut().unwrap().scroll_up(height, &chunks) {
				Ok(_) => {
					break;
				},
				Err(ScrollError::PrependChunks) => {
					drop(scroll);
					drop(chunks);
					self.update_log_stream(false).await?;
				},
				Err(ScrollError::AppendChunks) => {
					drop(scroll);
					drop(chunks);
					self.update_log_stream(true).await?;
				},
				Err(ScrollError::InvalidUtf) => {},
			}
		}
		Ok(())
	}

	// Handle a scroll down event.
	async fn down_impl(&self) -> Result<()> {
		let height = self.rect().height.to_usize().unwrap();
		loop {
			let Some(mut scroll) = self.inner.scroll.lock().await.clone() else {
				break;
			};

			let chunks = self.inner.chunks.lock().await;

			// Attempt to scroll down by `height + 1` many lines non-destructively.
			match scroll.scroll_down(height + 1, &chunks) {
				// If we weren't able to scroll down all the way and we're at eof, there's no work to do.
				Ok(count) if count < height && self.is_complete() => {
					break;
				},

				// If we weren't able to scroll down all the way we need to start tailing.
				Ok(count) if count < height => {
					drop(chunks);
					self.update_log_stream(true).await?;
					self.inner.scroll.lock().await.take();
				},

				// Otherwise destructively scroll down by 1 line.
				Ok(_) => {
					self.inner
						.scroll
						.lock()
						.await
						.as_mut()
						.unwrap()
						.scroll_down(1, &chunks)
						.unwrap();
					break;
				},
				Err(ScrollError::PrependChunks) => {
					drop(chunks);
					self.update_log_stream(false).await?;
				},
				Err(ScrollError::AppendChunks) => {
					drop(chunks);
					self.update_log_stream(true).await?;
					self.inner.scroll.lock().await.take();
				},
				Err(ScrollError::InvalidUtf) => {},
			}
		}

		Ok(())
	}

	// Update the rendered lines.
	async fn update_lines(&self) -> Result<()> {
		loop {
			let scroll = self.inner.scroll.lock().await.clone();
			let chunks = self.inner.chunks.lock().await;
			if chunks.is_empty() {
				self.inner.lines.lock().unwrap().clear();
				return Ok(());
			}

			// Get enough lines to fill the rectangle
			let rect = self.rect();
			let height = rect.height.to_usize().unwrap();
			let width = rect.width.to_usize().unwrap();
			let scroll = scroll.unwrap_or_else(|| {
				let chunk = chunks.len() - 1;
				let byte = chunks.last().unwrap().bytes.len();
				let mut scroll = Scroll::new(width, chunk, byte, &chunks);
				scroll.scroll_up(height + 1, &chunks).unwrap();
				scroll
			});

			// Update the list of lines and break out if successful.
			match scroll.read_lines(height + 1, &chunks) {
				Ok(lines) => {
					*self.inner.lines.lock().unwrap() = lines;
					break;
				},
				Err(ScrollError::PrependChunks) => {
					drop(chunks);
					self.update_log_stream(false).await?;
				},
				Err(ScrollError::AppendChunks) => {
					drop(chunks);
					self.update_log_stream(true).await?;
				},
				Err(ScrollError::InvalidUtf) => (),
			}
		}

		Ok(())
	}

	// Update the log stream. If prepend is Some, the tailing stream is destroyed and bytes are appended to the the front.
	async fn update_log_stream(&self, append: bool) -> Result<()> {
		// If we're appending and the task already exists, just wait for more data to be available.
		if append && self.inner.log_watch.lock().await.is_some() {
			let mut watch = self.inner.log_watch.lock().await.clone().unwrap();
			watch.changed().await.ok();
			return Ok(());
		}

		// Compute position and length.
		let area = self.rect().area().to_i64().unwrap();
		let mut chunks = self.inner.chunks.lock().await;
		let (position, length) = if append {
			let position = chunks
				.last()
				.map(|chunk| chunk.position + chunk.bytes.len().to_u64().unwrap());
			let length = Some(3 * area / 2);
			(position, length)
		} else {
			let position = chunks.first().map(|chunk| chunk.position);
			let length = Some(-3 * area / 2);
			(position, length)
		};

		// Create the stream.
		let mut stream = self
			.inner
			.build
			.log(
				self.inner.tg.as_ref(),
				tg::build::log::GetArg {
					length,
					position,
					..Default::default()
				},
			)
			.await?;

		// Spawn the log task if necessary.
		if append && chunks.last().map_or(true, |chunk| !chunk.bytes.is_empty()) {
			drop(chunks);
			let log = self.clone();
			let (tx, rx) = tokio::sync::watch::channel(());
			let task = tokio::spawn(async move {
				while let Some(chunk) = stream.try_next().await? {
					let mut chunks = log.inner.chunks.lock().await;
					if chunk.bytes.is_empty() {
						log.inner
							.eof
							.store(true, std::sync::atomic::Ordering::SeqCst);
						break;
					}
					chunks.push(chunk);
					drop(chunks);
					tx.send(()).ok();
				}
				log.inner.log_watch.lock().await.take();
				Ok::<_, tangram_error::Error>(())
			});
			self.inner.log_task.lock().unwrap().replace(task);
			self.inner.log_watch.lock().await.replace(rx);
		} else {
			if let Some(task) = self.inner.log_task.lock().unwrap().take() {
				task.abort();
			}

			// Drain the stream and prepend the chunks.
			let new_chunks = stream.try_collect::<Vec<_>>().await?;
			let mid = chunks.len();
			chunks.extend_from_slice(&new_chunks);
			chunks.rotate_left(mid);
			let mut scroll = self.inner.scroll.lock().await;
			if let Some(scroll) = scroll.as_mut() {
				scroll.chunk += mid;
			}
		}
		Ok(())
	}

	/// Get the bounding box of the log widget.
	fn rect(&self) -> tui::layout::Rect {
		*self.inner.rect.borrow()
	}

	/// Issue a scroll up event.
	fn up(&self) {
		self.inner.event_sender.send(LogEvent::ScrollUp).ok();
	}

	/// Issue a scroll down event.
	fn down(&self) {
		self.inner.event_sender.send(LogEvent::ScrollDown).ok();
	}

	/// Issue a resize event.
	fn resize(&self, rect: Rect) {
		self.inner.rect.send(rect).ok();
	}

	/// Render the log.
	fn render(&self, rect: tui::layout::Rect, buf: &mut tui::buffer::Buffer) {
		let lines = self.inner.lines.lock().unwrap();
		for (y, line) in (0..rect.height).zip(lines.iter()) {
			buf.set_line(rect.x, rect.y + y, &tui::text::Line::raw(line), rect.width);
		}
	}
}

#[derive(Debug)]
enum ScrollError {
	InvalidUtf,
	PrependChunks,
	AppendChunks,
}

impl Scroll {
	fn new(width: usize, chunk: usize, byte: usize, chunks: &[tg::build::log::Chunk]) -> Self {
		let offset = chunks[chunk].position.to_usize().unwrap() + byte;
		let length = chunks
			.last()
			.map(|chunk| chunk.position.to_usize().unwrap() + chunk.bytes.len())
			.unwrap();
		let cursor = unicode_segmentation::GraphemeCursor::new(offset, length, true);
		Self {
			width,
			chunk,
			byte,
			cursor,
		}
	}

	/// Increment the scroll position by one UTF8 grapheme cluster and add the intermediate results to end of the buffer. Returns Some(true) if successful, Some(false) if additional pre-context is required, or `None` if we receive invalid UTF-8.
	#[allow(clippy::too_many_lines)]
	fn advance(
		&mut self,
		forward: bool,                    // Advance forward if true, backward if false.
		chunks: &[tg::build::log::Chunk], // The chunks to use as a corpus.
		buffer: &mut Vec<u8>,             // The output buffer to write to.
	) -> std::result::Result<(), ScrollError> {
		let (old_chunk, old_byte) = (self.chunk, self.byte);
		loop {
			// Handle boundary conditions.
			if self.is_at_end(chunks) && forward {
				break;
			}

			// Get the current chunk and utf8 string at a current position.
			let (utf8_str, chunk_start) =
				self.get_utf8_str(chunks).ok_or(ScrollError::InvalidUtf)?;

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
					if new_pos < chunks[self.chunk].position.to_usize().unwrap()
						|| new_pos
							>= chunks[self.chunk].position.to_usize().unwrap()
								+ chunks[self.chunk].bytes.len()
					{
						self.chunk = chunks
							.iter()
							.enumerate()
							.find_map(|(idx, chunk)| {
								(new_pos >= chunk.position.to_usize().unwrap()
									&& new_pos
										< chunk.position.to_usize().unwrap() + chunk.bytes.len())
								.then_some(idx)
							})
							.unwrap_or(chunks.len() - 1);
					}
					self.byte = new_pos - chunks[self.chunk].position.to_usize().unwrap();
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
						if self.chunk == chunks.len() - 1 {
							return Err(ScrollError::AppendChunks);
						}
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
							return Err(ScrollError::PrependChunks);
						}
						self.chunk -= 1;
						self.byte = chunks[self.chunk].last_codepoint().unwrap();
					} else {
						self.byte = chunks[self.chunk].prev_codepoint(self.byte).unwrap();
					}
				},
				Err(unicode_segmentation::GraphemeIncomplete::PreContext(end)) => {
					let Some((string, start)) = self.get_pre_context(chunks, end) else {
						return Err(ScrollError::PrependChunks);
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
		Ok(())
	}

	fn is_at_end(&self, chunks: &[tg::build::log::Chunk]) -> bool {
		let chunk = &chunks[self.chunk];
		self.byte == chunk.bytes.len()
	}

	fn is_at_start(&self, chunks: &[tg::build::log::Chunk]) -> bool {
		self.chunk == 0 && chunks[self.chunk].first_codepoint() == Some(self.byte)
	}

	/// Scroll down by `height` num lines, wrapping on grapheme clusters, and return the number of lines that were scrolled.
	fn scroll_down(
		&mut self,
		height: usize,
		chunks: &[tg::build::log::Chunk],
	) -> std::result::Result<usize, ScrollError> {
		let mut buffer = Vec::with_capacity(3 * self.width / 2);

		// Advance the cursor by `height` number of lines, accounting for word wrap.
		let mut count = 0;
		let mut last_width = 0;
		while count < height {
			if self.is_at_end(chunks) {
				return Ok(count);
			}

			// Advance the cursor by width, or until we hit a newline.
			buffer.clear();
			let mut width = 0;
			let skip = loop {
				self.advance(true, chunks, &mut buffer)?;
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
		Ok(height)
	}

	/// Scroll up by height num lines, wrapping on grapheme clusters, returning the number of lines scrolled.
	fn scroll_up(
		&mut self,
		height: usize,
		chunks: &[tg::build::log::Chunk],
	) -> std::result::Result<usize, ScrollError> {
		let mut buffer = Vec::with_capacity(3 * self.width / 2);
		let mut last_line_wrapped = false;
		for count in 0..height {
			buffer.clear();
			let mut width = 0;
			last_line_wrapped = loop {
				self.advance(false, chunks, &mut buffer)?;
				if buffer.starts_with(b"\n") || buffer.starts_with(b"\r\n") {
					break false;
				}
				width += 1;
				if width == self.width {
					break true;
				}
			};
			if self.is_at_start(chunks) {
				return Ok(count);
			}
		}

		// Make sure to advance by one grapheme cluster if the start of the last line was a newline character.
		// TODO: remove this allocation
		if !last_line_wrapped {
			let mut buffer = Vec::with_capacity(4);
			self.advance(true, chunks, &mut buffer).ok();
		}

		Ok(height)
	}

	fn read_lines(
		&self,
		count: usize,
		chunks: &[tg::build::log::Chunk],
	) -> std::result::Result<Vec<String>, ScrollError> {
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
				match scroll.advance(true, chunks, &mut buffer) {
					Ok(()) => (),
					Err(ScrollError::InvalidUtf) => {
						// Handle invalid utf8.
						buffer.extend_from_slice("�".as_bytes());
						if let Some(next_codepoint) =
							chunks[scroll.chunk].next_codepoint(scroll.byte)
						{
							scroll.byte = next_codepoint;
						} else {
							scroll.byte += 1;
							if scroll.byte == chunks[scroll.chunk].bytes.len()
								&& scroll.chunk < chunks.len() - 1
							{
								scroll.chunk += 1;
							}
							let pos =
								chunks[scroll.chunk].position.to_usize().unwrap() + scroll.byte;
							scroll.cursor.set_cursor(pos);
						}
					},
					Err(e) => return Err(e),
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
		Ok(lines)
	}

	// Get the UTF8-validated string that contains the current scroll position.
	fn get_utf8_str<'a>(
		&self,
		chunks: &'a [tg::build::log::Chunk], // The chunks to use as a corpus.
	) -> Option<(Either<&'a str, String>, usize)> {
		// Special case: we're reading past the end of the buffer.
		if self.is_at_end(chunks) {
			let chunk = chunks.last().unwrap();
			let chunk_start = chunk.position.to_usize().unwrap() + chunk.bytes.len();
			return Some((Either::Left(""), chunk_start));
		}

		let first_codepoint = chunks[self.chunk].first_codepoint()?;
		let last_codepoint = chunks[self.chunk].last_codepoint()?;
		if self.byte == last_codepoint {
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
			let chunk_start = chunks[self.chunk].position.to_usize().unwrap() + self.byte;
			let string = String::from_utf8(buf).ok()?;
			Some((Either::Right(string), chunk_start))
		} else {
			let bytes = &chunks[self.chunk].bytes[first_codepoint..last_codepoint];
			let utf8 = std::str::from_utf8(bytes).ok()?;
			let chunk_start = chunks[self.chunk].position.to_usize().unwrap() + first_codepoint;
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
			.find(|chunk| chunk.position.to_usize().unwrap() < end)?;
		let end_byte = end - chunk.position.to_usize().unwrap();
		for start_byte in 0..chunk.bytes.len() {
			let bytes = &chunk.bytes[start_byte..end_byte];
			if let Ok(string) = std::str::from_utf8(bytes) {
				return Some((string, chunk.position.to_usize().unwrap() + start_byte));
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
			tg::build::log::Chunk {
				position: 0,
				bytes: b"11".to_vec().into(),
			},
			tg::build::log::Chunk {
				position: 2,
				bytes: b"\n\n22\n".to_vec().into(),
			},
			tg::build::log::Chunk {
				position: 7,
				bytes: b"3".to_vec().into(),
			},
			tg::build::log::Chunk {
				position: 8,
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
		scroll.scroll_up(2, &chunks).unwrap();
		assert_eq!(scroll.chunk, 2);
		assert_eq!(scroll.byte, 0);

		let lines = scroll.read_lines(4, &chunks).unwrap();
		assert_eq!(lines.len(), 2);
		assert_eq!(&lines[0], "33");
		assert_eq!(&lines[1], "44");

		// Empty lines.
		scroll.scroll_up(5, &chunks).unwrap();
		assert_eq!(scroll.chunk, 0);
		assert_eq!(scroll.byte, 0);
		assert_eq!(scroll.cursor.cur_cursor(), 0);

		let lines = scroll.read_lines(5, &chunks).unwrap();
		assert_eq!(lines.len(), 5);
		assert_eq!(&lines[0], "11");
		assert_eq!(&lines[1], "");
		assert_eq!(&lines[2], "22");
		assert_eq!(&lines[3], "33");
		assert_eq!(&lines[4], "44");

		// Scrolling down.
		scroll.scroll_down(2, &chunks).unwrap();
		let lines = scroll.read_lines(5, &chunks).unwrap();
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
				position: 0,
				bytes: b"\"log line 0\"\n".to_vec().into(),
			},
			tg::build::log::Chunk {
				position: 13,
				bytes: b"\"log line 1\"\n".to_vec().into(),
			},
			tg::build::log::Chunk {
				position: 26,
				bytes: b"\"log line 2\"\n".to_vec().into(),
			},
			tg::build::log::Chunk {
				position: 39,
				bytes: b"\"log line 3\"\n".to_vec().into(),
			},
			tg::build::log::Chunk {
				position: 52,
				bytes: b"\"log line 4\"\n".to_vec().into(),
			},
			tg::build::log::Chunk {
				position: 65,
				bytes: b"\"log line 5\"\n".to_vec().into(),
			},
			tg::build::log::Chunk {
				position: 78,
				bytes: b"\"log line 6\"\n".to_vec().into(),
			},
			tg::build::log::Chunk {
				position: 91,
				bytes: b"\"log line 7\"\n".to_vec().into(),
			},
			tg::build::log::Chunk {
				position: 104,
				bytes: b"\"log line 8\"\n".to_vec().into(),
			},
			tg::build::log::Chunk {
				position: 117,
				bytes: b"\"log line 9\"\n".to_vec().into(),
			},
			tg::build::log::Chunk {
				position: 130,
				bytes: b"\"log line 10\"\n".to_vec().into(),
			},
			tg::build::log::Chunk {
				position: 144,
				bytes: b"\"log line 11\"\n".to_vec().into(),
			},
			tg::build::log::Chunk {
				position: 158,
				bytes: b"\"log line 12\"\n".to_vec().into(),
			},
			tg::build::log::Chunk {
				position: 172,
				bytes: b"\"log line 13\"\n".to_vec().into(),
			},
			tg::build::log::Chunk {
				position: 186,
				bytes: b"\"log line 14\"\n".to_vec().into(),
			},
			tg::build::log::Chunk {
				position: 200,
				bytes: b"\"log line 15\"\n".to_vec().into(),
			},
			tg::build::log::Chunk {
				position: 214,
				bytes: b"\"log line 16\"\n".to_vec().into(),
			},
			tg::build::log::Chunk {
				position: 228,
				bytes: b"\"log line 17\"\n".to_vec().into(),
			},
			tg::build::log::Chunk {
				position: 242,
				bytes: b"\"log line 18\"\n".to_vec().into(),
			},
			tg::build::log::Chunk {
				position: 256,
				bytes: b"\"log line 19\"\n".to_vec().into(),
			},
			tg::build::log::Chunk {
				position: 270,
				bytes: b"\"log line 20\"\n".to_vec().into(),
			},
			tg::build::log::Chunk {
				position: 284,
				bytes: b"\"log line 21\"\n".to_vec().into(),
			},
			tg::build::log::Chunk {
				position: 298,
				bytes: b"\"log line 22\"\n".to_vec().into(),
			},
			tg::build::log::Chunk {
				position: 312,
				bytes: b"\"log line 23\"\n".to_vec().into(),
			},
		];
		let height = 40;
		let width = 189;
		let chunk = chunks.len() - 1;
		let byte = chunks.last().unwrap().bytes.len() - 1;
		let mut scroll = Scroll::new(width, chunk, byte, &chunks);
		scroll.scroll_up(height, &chunks).unwrap();
		let lines = scroll.read_lines(height, &chunks).unwrap();
		assert_eq!(lines.len(), chunks.len());
	}

	#[test]
	fn simple_tailing() {
		let chunks = vec![
			tg::build::log::Chunk {
				position: 0,
				bytes: b"\"0\"\n".to_vec().into(),
			},
			tg::build::log::Chunk {
				position: 4,
				bytes: b"\"1\"\n".to_vec().into(),
			},
			tg::build::log::Chunk {
				position: 8,
				bytes: b"\"2\"\n".to_vec().into(),
			},
			tg::build::log::Chunk {
				position: 12,
				bytes: b"\"3\"\n".to_vec().into(),
			},
		];
		let mut scroll = Scroll::new(
			10,
			chunks.len() - 1,
			chunks.last().unwrap().bytes.len(),
			&chunks,
		);
		scroll.scroll_up(3, &chunks).unwrap();
		assert_eq!(scroll.chunk, 2);
		assert_eq!(scroll.byte, 0);
		let lines = scroll.read_lines(3, &chunks).unwrap();
		assert_eq!(&lines[0], "\"2\"");
		assert_eq!(&lines[1], "\"3\"");
	}

	#[allow(clippy::too_many_lines)]
	#[test]
	fn scroll_up() {
		let chunks = [
			tg::build::log::Chunk {
				position: 0,
				bytes: b"\"0\"\n".to_vec().into(),
			},
			tg::build::log::Chunk {
				position: 4,
				bytes: b"\"1\"\n".to_vec().into(),
			},
			tg::build::log::Chunk {
				position: 8,
				bytes: b"\"2\"\n".to_vec().into(),
			},
			tg::build::log::Chunk {
				position: 12,
				bytes: b"\"3\"\n".to_vec().into(),
			},
			tg::build::log::Chunk {
				position: 16,
				bytes: b"\"4\"\n".to_vec().into(),
			},
			tg::build::log::Chunk {
				position: 20,
				bytes: b"\"5\"\n".to_vec().into(),
			},
			tg::build::log::Chunk {
				position: 24,
				bytes: b"\"6\"\n".to_vec().into(),
			},
			tg::build::log::Chunk {
				position: 28,
				bytes: b"\"7\"\n".to_vec().into(),
			},
			tg::build::log::Chunk {
				position: 32,
				bytes: b"\"8\"\n".to_vec().into(),
			},
			tg::build::log::Chunk {
				position: 36,
				bytes: b"\"9\"\n".to_vec().into(),
			},
			tg::build::log::Chunk {
				position: 40,
				bytes: b"\"10\"\n".to_vec().into(),
			},
			tg::build::log::Chunk {
				position: 45,
				bytes: b"\"11\"\n".to_vec().into(),
			},
			tg::build::log::Chunk {
				position: 50,
				bytes: b"\"12\"\n".to_vec().into(),
			},
			tg::build::log::Chunk {
				position: 55,
				bytes: b"\"13\"\n".to_vec().into(),
			},
			tg::build::log::Chunk {
				position: 60,
				bytes: b"\"14\"\n".to_vec().into(),
			},
			tg::build::log::Chunk {
				position: 65,
				bytes: b"\"15\"\n".to_vec().into(),
			},
			tg::build::log::Chunk {
				position: 70,
				bytes: b"\"16\"\n".to_vec().into(),
			},
			tg::build::log::Chunk {
				position: 75,
				bytes: b"\"17\"\n".to_vec().into(),
			},
			tg::build::log::Chunk {
				position: 80,
				bytes: b"\"18\"\n".to_vec().into(),
			},
			tg::build::log::Chunk {
				position: 85,
				bytes: b"\"19\"\n".to_vec().into(),
			},
			tg::build::log::Chunk {
				position: 90,
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
		inner.scroll_up(height + 2, &chunks).ok();
	}

	#[test]
	fn incomplete() {
		let chunks = [
			tg::build::log::Chunk {
				position: 114,
				bytes: b"\"doing stuff 6...\"\n".to_vec().into(),
			},
			tg::build::log::Chunk {
				position: 133,
				bytes: b"\"doing stuff 7...\"\n".to_vec().into(),
			},
			tg::build::log::Chunk {
				position: 152,
				bytes: b"\"doing stuff 8...\"\n".to_vec().into(),
			},
			tg::build::log::Chunk {
				position: 171,
				bytes: b"\"doing stuff 9...\"\n".to_vec().into(),
			},
			tg::build::log::Chunk {
				position: 190,
				bytes: b"\"doing stuff 10...\"\n".to_vec().into(),
			},
		];
		let chunk = chunks.len() - 1;
		let width = 80;
		let height = 26;
		let byte = chunks.last().unwrap().bytes.len();
		let mut scroll = Scroll::new(width, chunk, byte, &chunks);
		scroll.scroll_up(height + 1, &chunks).ok();

		let lines = scroll.read_lines(height, &chunks);
		assert!(lines.is_err());
	}
}
