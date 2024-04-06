use crossterm as ct;
use futures::{StreamExt as _, TryStreamExt as _};
use num::ToPrimitive;
use ratatui as tui;
use std::{
	collections::VecDeque,
	io::SeekFrom,
	sync::{
		atomic::{AtomicBool, AtomicU64, Ordering},
		Arc, Weak,
	},
};
use tangram_client as tg;
use tui::{layout::Rect, style::Stylize, widgets::Widget};

mod scroll;

pub struct Tui {
	stop: tokio::sync::watch::Sender<bool>,
	task: Option<tokio::task::JoinHandle<std::io::Result<Terminal>>>,
}

type Backend = tui::backend::CrosstermBackend<std::fs::File>;

type Terminal = tui::Terminal<Backend>;

struct App {
	client: tg::Client,
	direction: tui::layout::Direction,
	layout: tui::layout::Layout,
	log: Log,
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
	client: tg::Client,
	index: usize,
	indicator_task: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
	parent: Option<Weak<TreeItemInner>>,
	state: std::sync::Mutex<TreeItemState>,
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
	Created,
	Queued,
	Started,
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

	// The client.
	client: tg::Client,

	// Whether we've reached eof or not.
	eof: AtomicBool,

	// Channel used to send UI events.
	event_sender: tokio::sync::mpsc::UnboundedSender<LogEvent>,

	// The lines of text that will be displayed.
	lines: std::sync::Mutex<Vec<String>>,

	// The log streaming task.
	log_task: std::sync::Mutex<Option<tokio::task::JoinHandle<tg::Result<()>>>>,

	// A watch to be notified when new logs are received from the log task.
	log_watch: tokio::sync::Mutex<Option<tokio::sync::watch::Receiver<()>>>,

	// The maximum position of the log we've seen so far.
	max_position: AtomicU64,

	// The event handler task.
	event_task: std::sync::Mutex<Option<tokio::task::JoinHandle<tg::Result<()>>>>,

	// The bounding box of the log view.
	rect: tokio::sync::watch::Sender<Rect>,

	// The current state of the log's scrolling position.
	scroll: tokio::sync::Mutex<Option<scroll::Scroll>>,
}

enum LogEvent {
	ScrollUp,
	ScrollDown,
}

const SPINNER: [char; 10] = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];

impl Tui {
	pub async fn start(client: &tg::Client, build: &tg::Build) -> tg::Result<Self> {
		// Create the terminal.
		let tty = tokio::fs::OpenOptions::new()
			.read(true)
			.write(true)
			.open("/dev/tty")
			.await
			.map_err(|source| tg::error!(!source, "failed to open /dev/tty"))?;
		let tty = tty.into_std().await;
		let backend = Backend::new(tty);
		let mut terminal = Terminal::new(backend)
			.map_err(|source| tg::error!(!source, "failed to create the terminal backend"))?;
		ct::terminal::enable_raw_mode()
			.map_err(|source| tg::error!(!source, "failed to enable the terminal's raw mode"))?;
		ct::execute!(
			terminal.backend_mut(),
			ct::event::EnableMouseCapture,
			ct::terminal::EnterAlternateScreen,
		)
		.map_err(|source| tg::error!(!source, "failed to set up the terminal"))?;

		// Create the stop flag.
		let (stop, _) = tokio::sync::watch::channel(false);

		// Spawn the task.
		let task = tokio::task::spawn_blocking({
			let client = client.clone();
			let build = build.clone();
			let stop = stop.subscribe();
			move || {
				// Create the app.
				let rect = terminal.get_frame().size();
				let mut app = App::new(&client, &build, rect);

				// Run the event loop.
				while !*stop.borrow() {
					// Wait for and handle an event.
					if ct::event::poll(std::time::Duration::from_millis(16))? {
						let event = ct::event::read()?;

						// Quit the TUI if requested.
						if let ct::event::Event::Key(event) = event {
							if event.code == ct::event::KeyCode::Char('q')
								|| (event.code == ct::event::KeyCode::Char('c')
									&& event.modifiers == ct::event::KeyModifiers::CONTROL)
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
			stop,
			task: Some(task),
		})
	}

	pub fn stop(&self) {
		self.stop.send_replace(true);
	}

	pub async fn join(mut self) -> tg::Result<()> {
		// Get the task.
		let Some(task) = self.task.take() else {
			return Ok(());
		};

		// Join the task and get the terminal.
		let mut terminal = task
			.await
			.unwrap()
			.map_err(|source| tg::error!(!source, "the task did not succeed"))?;

		// Reset the terminal.
		terminal
			.clear()
			.map_err(|source| tg::error!(!source, "failed to clear the terminal"))?;
		ct::execute!(
			terminal.backend_mut(),
			ct::event::DisableMouseCapture,
			ct::terminal::LeaveAlternateScreen
		)
		.map_err(|source| tg::error!(!source, "failed to reset the terminal"))?;
		ct::terminal::disable_raw_mode()
			.map_err(|source| tg::error!(!source, "failed to disable the terminal's raw mode"))?;

		Ok(())
	}
}

impl App {
	fn new(client: &tg::Client, build: &tg::Build, rect: tui::layout::Rect) -> Self {
		let client = client.clone();
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
		let log = Log::new(&client, build, layouts[2]);
		let root = TreeItem::new(&client, build, None, 0, true);
		root.expand();
		let tree = Tree::new(root, layouts[0]);
		Self {
			client,
			direction,
			layout,
			log,
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
		if new_selected_index == previous_selected_index {
			return;
		}

		let height = self.tree.rect.height.to_usize().unwrap();
		if new_selected_index < self.tree.scroll {
			self.tree.scroll -= 1;
		} else if new_selected_index >= self.tree.scroll + height {
			self.tree.scroll += 1;
		}
		let new_selected_item = expanded_items[new_selected_index].clone();
		self.tree.selected.inner.state.lock().unwrap().selected = false;
		new_selected_item.inner.state.lock().unwrap().selected = true;
		self.log.stop();
		self.log = Log::new(
			&self.client,
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
				self.log.stop();
				self.log = Log::new(&self.client, &parent.inner.build, self.log.rect());
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
		let client = self.client.clone();
		tokio::spawn(async move { build.cancel(&client).await.ok() });
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
		client: &tg::Client,
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
			client: client.clone(),
			index,
			indicator_task: std::sync::Mutex::new(None),
			parent,
			state: std::sync::Mutex::new(state),
			title_task: std::sync::Mutex::new(None),
		});

		let item = Self { inner };

		let task = tokio::task::spawn({
			let item = item.clone();
			async move {
				let arg = tg::build::status::GetArg::default();
				let Ok(mut stream) = item.inner.build.status(&item.inner.client, arg).await else {
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
						tg::build::Status::Created => {
							item.inner.state.lock().unwrap().indicator = TreeItemIndicator::Created;
						},
						tg::build::Status::Queued => {
							item.inner.state.lock().unwrap().indicator = TreeItemIndicator::Queued;
						},
						tg::build::Status::Started => {
							item.inner.state.lock().unwrap().indicator = TreeItemIndicator::Started;
						},
						tg::build::Status::Finished => {
							break;
						},
					}
				}
				let Ok(outcome) = item.inner.build.outcome(&item.inner.client).await else {
					item.inner.state.lock().unwrap().indicator = TreeItemIndicator::Errored;
					return;
				};
				let indicator = match outcome {
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
				let title = title(&item.inner.client, &item.inner.build)
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
					position: Some(SeekFrom::Start(0)),
					..Default::default()
				};
				let Ok(mut children) = item.inner.build.children(&item.inner.client, arg).await
				else {
					return;
				};
				while let Some(Ok(child)) = children.next().await {
					let client = item.inner.client.clone();
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
					let child = TreeItem::new(&client, &child, parent, index, selected);
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
		let children_task = self
			.inner
			.children_task
			.lock()
			.unwrap()
			.replace(children_task);
		if let Some(task) = children_task {
			task.abort();
		}
	}

	fn collapse(&self) {
		self.inner.state.lock().unwrap().expanded = false;
		if let Some(children_task) = self.inner.children_task.lock().unwrap().take() {
			children_task.abort();
		}

		let children = self
			.inner
			.state
			.lock()
			.unwrap()
			.children
			.take()
			.into_iter()
			.flatten();
		for child in children {
			child.stop();
			child.collapse();
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
			TreeItemIndicator::Created | TreeItemIndicator::Queued => "⟳".yellow(),
			TreeItemIndicator::Started => {
				let now = std::time::SystemTime::now()
					.duration_since(std::time::UNIX_EPOCH)
					.unwrap()
					.as_millis();
				let position = (now / (1000 / 10)) % 10;
				let position = position.to_usize().unwrap();
				SPINNER[position].to_string().blue()
			},
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

	fn stop(&self) {
		if let Some(task) = self.inner.children_task.lock().unwrap().take() {
			task.abort();
		}
		if let Some(task) = self.inner.indicator_task.lock().unwrap().take() {
			task.abort();
		}
		if let Some(task) = self.inner.title_task.lock().unwrap().take() {
			task.abort();
		}
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

async fn title(client: &tg::Client, build: &tg::Build) -> tg::Result<Option<String>> {
	// Get the target.
	let target = build.target(client).await?;

	// Get the package.
	let Some(package) = target.package(client).await? else {
		return Ok(None);
	};

	// Get the metadata.
	let metadata = tg::package::get_metadata(client, &package).await.ok();

	let mut title = String::new();
	if let Some(metadata) = metadata {
		title.push_str(metadata.name.as_deref().unwrap_or("<unknown>"));
		if let Some(version) = &metadata.version {
			title.push_str(&format!("@{version}"));
		}
	}
	let name = target.name(client).await?;
	title.push_str(name.as_deref().unwrap_or("<unknown>"));
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
	fn new(client: &tg::Client, build: &tg::Build, rect: Rect) -> Self {
		let client = client.clone();
		let build = build.clone();
		let chunks = tokio::sync::Mutex::new(Vec::new());
		let (event_sender, mut event_receiver) = tokio::sync::mpsc::unbounded_channel();
		let lines = std::sync::Mutex::new(Vec::new());
		let (rect, mut rect_receiver) = tokio::sync::watch::channel(rect);

		let log = Log {
			inner: Arc::new(LogInner {
				build,
				chunks,
				client,
				eof: AtomicBool::new(false),
				event_sender,
				event_task: std::sync::Mutex::new(None),
				lines,
				log_task: std::sync::Mutex::new(None),
				log_watch: tokio::sync::Mutex::new(None),
				max_position: AtomicU64::new(0),
				rect,
				scroll: tokio::sync::Mutex::new(None),
			}),
		};

		// Create the event handler task.
		let event_task = tokio::task::spawn({
			let log = log.clone();
			async move {
				log.init().await?;
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
								scroll.rect = log.rect();
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

	async fn init(&self) -> tg::Result<()> {
		let client = &self.inner.client;
		let position = Some(std::io::SeekFrom::End(0));

		// Get at least one chunk.
		let chunk = self
			.inner
			.build
			.log(
				client,
				tg::build::log::GetArg {
					position,
					..Default::default()
				},
			)
			.await?
			.try_next()
			.await?
			.ok_or_else(|| tg::error!("failed to get a log chunk"))?;
		let max_position = chunk.position + chunk.bytes.len().to_u64().unwrap();
		self.inner
			.max_position
			.store(max_position, Ordering::Relaxed);

		// Seed the front of the log.
		self.update_log_stream(false).await?;

		// Start tailing if necessary.
		self.update_log_stream(true).await?;
		Ok(())
	}

	fn stop(&self) {
		if let Some(task) = self.inner.log_task.lock().unwrap().take() {
			task.abort();
		}
		if let Some(task) = self.inner.event_task.lock().unwrap().take() {
			task.abort();
		}
	}

	fn is_complete(&self) -> bool {
		self.inner.eof.load(Ordering::SeqCst)
	}

	// Handle a scroll up event.
	async fn up_impl(&self) -> tg::Result<()> {
		// Create the scroll state if necessary.
		if self.inner.scroll.lock().await.is_none() {
			loop {
				let chunks = self.inner.chunks.lock().await;
				match scroll::Scroll::new(self.rect(), &chunks) {
					Ok(inner) => {
						self.inner.scroll.lock().await.replace(inner);
						break;
					},
					Err(error) => {
						drop(chunks);
						self.update_log_stream(matches!(error, scroll::Error::Append))
							.await?;
					},
				}
			}
		};

		// Attempt to scroll up by 1 line.
		loop {
			let mut scroll = self.inner.scroll.lock().await;
			let scroll = scroll.as_mut().unwrap();
			let chunks = self.inner.chunks.lock().await;
			match scroll.scroll_up(1, &chunks) {
				Ok(_) => return Ok(()),
				// If we need to append or prepend, update the log stream and try again.
				Err(error) => {
					drop(chunks);
					self.update_log_stream(matches!(error, scroll::Error::Append))
						.await?;
				},
			}
		}
	}

	// Handle a scroll down event.
	async fn down_impl(&self) -> tg::Result<()> {
		loop {
			let mut scroll = self.inner.scroll.lock().await;
			let Some(scroll_) = scroll.as_mut() else {
				return Ok(());
			};
			let chunks = self.inner.chunks.lock().await;
			match scroll_.scroll_down(1, &chunks) {
				// If the scroll succeeded but we didn't scroll any lines and the build is not yet complete, we need to start tailing.
				Ok(count) if count != 1 && !self.is_complete() => {
					drop(chunks);
					scroll.take();
					self.update_log_stream(true).await?;
				},
				Ok(_) => return Ok(()),
				Err(error) => {
					drop(chunks);
					self.update_log_stream(matches!(error, scroll::Error::Append))
						.await?;
				},
			}
		}
	}

	// Update the rendered lines.
	async fn update_lines(&self) -> tg::Result<()> {
		loop {
			let chunks = self.inner.chunks.lock().await;
			if chunks.is_empty() {
				self.inner.lines.lock().unwrap().clear();
				return Ok(());
			}
			let mut scroll = self.inner.scroll.lock().await;
			let result = scroll.as_mut().map_or_else(
				|| {
					let mut scroll = scroll::Scroll::new(self.rect(), &chunks)?;
					scroll.read_lines(&chunks)
				},
				|scroll| scroll.read_lines(&chunks),
			);

			// Update the list of lines and break out if successful.
			match result {
				Ok(lines) => {
					*self.inner.lines.lock().unwrap() = lines;
					break;
				},
				Err(error) => {
					drop(chunks);
					self.update_log_stream(matches!(error, scroll::Error::Append))
						.await?;
				},
			}
		}

		Ok(())
	}

	// Update the log stream. If prepend is Some, the tailing stream is destroyed and bytes are appended to the the front.
	async fn update_log_stream(&self, append: bool) -> tg::Result<()> {
		// If we're appending and the task already exists, just wait for more data to be available.
		if append && self.inner.log_watch.lock().await.is_some() {
			let mut watch = self.inner.log_watch.lock().await.clone().unwrap();
			watch.changed().await.ok();
			return Ok(());
		}
		// Otherwise abort any existing log task.
		if let Some(task) = self.inner.log_task.lock().unwrap().take() {
			task.abort();
		}

		// Compute position and length.
		let area = self.rect().area().to_i64().unwrap();
		let mut chunks = self.inner.chunks.lock().await;
		let max_position = self.inner.max_position.load(Ordering::Relaxed);
		let (position, length) = if append {
			let last_position = chunks
				.last()
				.map(|chunk| chunk.position + chunk.bytes.len().to_u64().unwrap());
			match last_position {
				Some(position) if position == max_position => {
					(Some(SeekFrom::Start(position)), None)
				},
				Some(position) => (Some(SeekFrom::Start(position)), Some(3 * area / 2)),
				None => (None, None),
			}
		} else {
			let position = chunks.first().map(|chunk| chunk.position);

			let length = (3 * area / 2).to_u64().unwrap();
			match position {
				Some(position) if position >= length => {
					(Some(SeekFrom::Start(0)), Some(position.to_i64().unwrap()))
				},
				Some(position) => (
					Some(SeekFrom::Start(position)),
					Some(-length.to_i64().unwrap()),
				),
				None => (None, Some(-length.to_i64().unwrap())),
			}
		};

		// Create the stream.
		let mut stream = self
			.inner
			.build
			.log(
				&self.inner.client,
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
						log.inner.eof.store(true, Ordering::SeqCst);
						break;
					}
					let max_position = chunk.position + chunk.bytes.len().to_u64().unwrap();
					log.inner
						.max_position
						.fetch_max(max_position, Ordering::AcqRel);
					chunks.push(chunk);
					drop(chunks);
					tx.send(()).ok();
				}
				log.inner.log_watch.lock().await.take();
				Ok::<_, tg::Error>(())
			});
			self.inner.log_task.lock().unwrap().replace(task);
			self.inner.log_watch.lock().await.replace(rx);
		} else {
			// Drain the stream and prepend the chunks.
			let new_chunks = stream.try_collect::<Vec<_>>().await?;
			let mid = chunks.len();
			chunks.extend_from_slice(&new_chunks);
			chunks.rotate_left(mid);
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
