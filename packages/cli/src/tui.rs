use bytes::Bytes;
use crossterm as ct;
use futures::{stream::FusedStream, StreamExt};
use num::ToPrimitive;
use ratatui as tui;
use std::{
	borrow::BorrowMut,
	collections::VecDeque,
	sync::{
		atomic::{AtomicBool, AtomicUsize},
		Arc, Weak,
	},
};
use tangram_client as tg;
use tangram_error::{Result, WrapErr};
use tg::package::Ext;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tui::{style::Stylize, widgets::Widget};
use unicode_width::UnicodeWidthChar;

pub struct Tui {
	#[allow(dead_code)]
	options: Options,
	stop: Arc<AtomicBool>,
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
	build: tg::Build,
	lines: std::sync::Mutex<Vec<String>>,
	lines_bytes: std::sync::Mutex<Vec<u64>>,
	lines_file_offset_start: std::sync::Mutex<u64>,
	rect: tokio::sync::watch::Sender<tui::layout::Rect>,
	sender: tokio::sync::mpsc::UnboundedSender<LogUpdate>,
	task: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
	tg: Box<dyn tg::Handle>,
}

enum LogUpdate {
	Up,
	Down,
}

static SPINNER_POSITION: AtomicUsize = AtomicUsize::new(0);

const SPINNER: [char; 10] = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];

const SPINNER_FRAMES_PER_UPDATE: usize = 4;

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
		let stop = Arc::new(AtomicBool::new(false));

		// Spawn the task.
		let task = tokio::task::spawn_blocking({
			let tg = tg.clone_box();
			let build = build.clone();
			let stop = stop.clone();
			move || {
				// Create the app.
				let rect = terminal.get_frame().size();
				let mut app = App::new(tg.as_ref(), &build, rect);

				// Run the event loop.
				while !stop.load(std::sync::atomic::Ordering::SeqCst) {
					SPINNER_POSITION.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

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
		// Set the stop flag.
		let ordering = std::sync::atomic::Ordering::SeqCst;
		self.stop.store(true, ordering);
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
				let state = SPINNER_POSITION.load(std::sync::atomic::Ordering::SeqCst);
				let state = (state / SPINNER_FRAMES_PER_UPDATE) % SPINNER.len();
				SPINNER[state].to_string().blue()
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

impl Log {
	fn new(tg: &dyn tg::Handle, build: &tg::Build, rect: tui::layout::Rect) -> Self {
		let tg = tg.clone_box();
		let lines = std::sync::Mutex::new(Vec::new());
		let lines_bytes = std::sync::Mutex::new(Vec::new());
		let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
		let (rect_sender, mut rect_receiver) = tokio::sync::watch::channel(rect);
		let inner = Arc::new(LogInner {
			build: build.clone(),
			lines,
			lines_bytes,
			lines_file_offset_start: std::sync::Mutex::new(0),
			rect: rect_sender,
			sender,
			task: std::sync::Mutex::new(None),
			tg: tg.clone_box(),
		});
		let log = Self { inner };
		let task = tokio::task::spawn({
			let log = log.clone();
			async move {
				let mut file = tokio::fs::File::from_std(tempfile::tempfile().unwrap());
				let mut scroll: Option<u64> = None;
				let Ok(stream) = log.inner.build.log(log.inner.tg.as_ref()).await else {
					return;
				};
				let mut stream = stream.fuse();
				loop {
					tokio::select! {
						Some(bytes) = stream.next(), if !stream.is_terminated() => {
							let Ok(bytes) = bytes else {
								return;
							};
							let result = log.bytes_impl(&mut file, &mut scroll, &bytes).await;
							if result.is_err() {
								return;
							}
						},
						result = receiver.recv() => match result.unwrap() {
							LogUpdate::Down => {
								let result = log.down_impl(&mut file, &mut scroll).await;
								if result.is_err() {
									return;
								}
							}
							LogUpdate::Up => {
								let result = log.up_impl(&mut file, &mut scroll).await;
								if result.is_err() {
									return;
								}
							}
						},
						result = rect_receiver.changed() => {
							result.unwrap();
							let rect = *rect_receiver.borrow();
							let result = log.rect_impl(&mut file, &mut scroll, rect).await;
							if result.is_err() {
								return;
							}
						},
					};
				}
			}
		});
		log.inner.task.lock().unwrap().replace(task);
		log
	}

	async fn bytes_impl(
		&self,
		file: &mut tokio::fs::File,
		scroll: &mut Option<u64>,
		bytes: &Bytes,
	) -> Result<()> {
		file.seek(std::io::SeekFrom::End(0))
			.await
			.wrap_err("Failed to seek the log file.")?;
		file.write_all(bytes)
			.await
			.wrap_err("Failed to write to the log file.")?;

		if scroll.is_some() {
			// We are not tailing, so no need to update the lines.
			return Ok(());
		}

		let max_width = self.inner.rect.borrow().width.to_usize().unwrap();
		let mut lines = self.inner.lines.lock().unwrap();
		let mut line = String::new();
		let mut current_width = 0;
		let mut lines_file_offset_start = self.inner.lines_file_offset_start.lock().unwrap();
		let mut lines_bytes = self.inner.lines_bytes.lock().unwrap();
		let mut current_line_bytes = 0;

		for byte in bytes {
			let char = byte_to_char(*byte);
			if char == '\n' {
				if lines.len() == self.inner.rect.borrow().height.to_usize().unwrap() {
					lines.remove(0);
					let bytes_removed = lines_bytes.remove(0);
					*lines_file_offset_start += bytes_removed;
				}
				lines.push(line);
				current_line_bytes += 1;
				lines_bytes.push(current_line_bytes);
				line = String::new();
				current_width = 0;
				current_line_bytes = 0;
			} else {
				if current_width + char.width().unwrap_or(0) > max_width {
					if lines.len() == self.inner.rect.borrow().height.to_usize().unwrap() {
						lines.remove(0);
						let bytes_removed = lines_bytes.remove(0);
						*lines_file_offset_start += bytes_removed;
					}
					lines.push(line);
					lines_bytes.push(current_line_bytes);
					line = String::new();
					current_width = 0;
					current_line_bytes = 0;
				}
				current_width += char.width().unwrap_or(0);
				current_line_bytes += 1;
				line.push(char);
			}
		}

		if !line.is_empty() {
			lines.remove(0);
			let bytes_removed = lines_bytes.remove(0);
			*lines_file_offset_start += bytes_removed;
			lines.push(line);
			lines_bytes.push(current_line_bytes);
		}

		Ok(())
	}

	async fn rect_impl(
		&self,
		file: &mut tokio::fs::File,
		scroll: &mut Option<u64>,
		rect: tui::layout::Rect,
	) -> Result<()> {
		// All the lines need to be recomputed.
		{
			let mut lines = self.inner.lines.lock().unwrap();
			let len = lines.len();
			lines.drain(0..len);

			let mut lines_bytes = self.inner.lines_bytes.lock().unwrap();
			let len = lines_bytes.len();
			lines_bytes.drain(0..len);
		}

		// Starting at the lines file offset, append lines.
		let seek = *self.inner.lines_file_offset_start.lock().unwrap();
		let mut buf = [0u8; 1];
		let mut current_line_width = 0;
		let mut current_line_bytes = 0;
		let max_width = rect.width.to_usize().unwrap();
		let file_bytes = file.metadata().await.unwrap().len();
		let mut num_lines = 0;

		let mut line = String::new();
		for seek in seek..file_bytes {
			if num_lines == rect.height.to_usize().unwrap() {
				break;
			}
			file.seek(std::io::SeekFrom::Start(seek)).await.unwrap();
			file.read_exact(buf.as_mut())
				.await
				.wrap_err("Failed to read from the log file.")?;
			let char = byte_to_char(buf[0]);
			if char == '\n' {
				current_line_bytes += 1;
				let mut lines = self.inner.lines.lock().unwrap();
				lines.push(line);
				let mut lines_bytes = self.inner.lines_bytes.lock().unwrap();
				lines_bytes.push(current_line_bytes);
				line = String::new();
				num_lines += 1;
				current_line_bytes = 0;
				current_line_width = 0;
				continue;
			} else if current_line_width + char.width().unwrap_or(0) > max_width {
				let mut lines = self.inner.lines.lock().unwrap();
				lines.push(line);
				let mut lines_bytes = self.inner.lines_bytes.lock().unwrap();
				lines_bytes.push(current_line_bytes);
				line = String::new();
				num_lines += 1;
				current_line_bytes = 0;
				current_line_width = 0;
			}
			current_line_bytes += 1;
			line.push(char);
			current_line_width += char.width().unwrap_or(0);
		}

		// If there are still not enough lines, prepend lines until we fill the height or we reach the start of the buffer.
		while *self.inner.lines_file_offset_start.lock().unwrap() > 0
			&& self.inner.lines.lock().unwrap().len() < rect.height.to_usize().unwrap()
		{
			self.prepend_line(file, scroll).await?;
		}

		// If there are still not enough lines, set scroll to None so we tail.
		if self.inner.lines.lock().unwrap().len() < rect.height.to_usize().unwrap() {
			*scroll.borrow_mut() = None;
		}

		// If there are enough lines but we aren't at the bottom, set scroll so we don't tail.
		let lines_file_offset_end = *self.inner.lines_file_offset_start.lock().unwrap()
			+ self.inner.lines_bytes.lock().unwrap().iter().sum::<u64>();
		if self.inner.lines.lock().unwrap().len() == rect.height.to_usize().unwrap()
			&& lines_file_offset_end < file_bytes
		{
			*scroll.borrow_mut() = Some(*self.inner.lines_file_offset_start.lock().unwrap());
		}

		Ok(())
	}

	async fn down_impl(&self, file: &mut tokio::fs::File, scroll: &mut Option<u64>) -> Result<()> {
		// If lines is equal to height and scroll is none, we are already at the bottom.
		if scroll.is_none()
			&& self.inner.lines.lock().unwrap().len().to_u16().unwrap()
				== self.inner.rect.borrow().height
		{
			return Ok(());
		}

		// If there aren't enough lines to fill the view, we can't scroll down.
		if self.inner.lines.lock().unwrap().len().to_u16().unwrap()
			< self.inner.rect.borrow().height
		{
			return Ok(());
		}

		let mut buf = [0; 1];
		let mut line = String::new();
		let seek = *self.inner.lines_file_offset_start.lock().unwrap()
			+ self.inner.lines_bytes.lock().unwrap().iter().sum::<u64>();
		let max_width = self.inner.rect.borrow().width.to_usize().unwrap();
		let mut current_line_width = 0;
		let mut current_line_bytes = 0;
		let file_bytes = file.metadata().await.unwrap().len();

		for seek in seek..file_bytes {
			// Read the value at the seek position.
			file.seek(std::io::SeekFrom::Start(seek))
				.await
				.wrap_err("Failed to seek.")?;
			file.read_exact(&mut buf)
				.await
				.wrap_err("Failed to read the bytes")?;
			let char = byte_to_char(buf[0]);
			if char == '\n' {
				current_line_bytes += 1;
				break;
			}
			if current_line_width + char.width().unwrap_or(0) > max_width {
				break;
			}
			line.push(char);
			current_line_width += char.width().unwrap_or(0);
			current_line_bytes += 1;
		}

		let mut lines_file_offset_start = self.inner.lines_file_offset_start.lock().unwrap();

		if current_line_bytes > 0 {
			// Update the lines.
			let mut lines = self.inner.lines.lock().unwrap();
			lines.remove(0);
			lines.push(line);

			// Update the lines_bytes.
			let mut lines_bytes = self.inner.lines_bytes.lock().unwrap();
			let bytes_removed = lines_bytes.remove(0);
			lines_bytes.push(current_line_bytes);
			*lines_file_offset_start += bytes_removed;
		}

		// Update the scroll.
		if seek == file_bytes {
			// We are at the bottom.
			*scroll.borrow_mut() = None;
		} else {
			*scroll.borrow_mut() = Some(*lines_file_offset_start);
		}

		Ok(())
	}

	async fn up_impl(&self, file: &mut tokio::fs::File, scroll: &mut Option<u64>) -> Result<()> {
		if scroll.map_or(false, |scroll| scroll == 0) {
			return Ok(());
		}

		if *self.inner.lines_file_offset_start.lock().unwrap() == 0 {
			return Ok(());
		}

		self.prepend_line(file, scroll).await?;

		let mut lines = self.inner.lines.lock().unwrap();
		let mut lines_bytes = self.inner.lines_bytes.lock().unwrap();
		lines.pop();
		lines_bytes.pop();

		// Update the scroll.
		let lines_file_offset_start = self.inner.lines_file_offset_start.lock().unwrap();
		*scroll.borrow_mut() = Some(*lines_file_offset_start);

		Ok(())
	}

	async fn prepend_line(
		&self,
		file: &mut tokio::fs::File,
		_scroll: &mut Option<u64>,
	) -> Result<()> {
		let lines_file_offset_start = *self.inner.lines_file_offset_start.lock().unwrap();
		let max_width = self.inner.rect.borrow().width.to_usize().unwrap();
		let mut buf = [0u8; 1];
		let mut line = VecDeque::new();
		let mut current_line_bytes = 0;
		let mut current_line_width = 0;
		for seek in (0..lines_file_offset_start).rev() {
			file.seek(std::io::SeekFrom::Start(seek)).await.unwrap();
			file.read_exact(buf.as_mut())
				.await
				.wrap_err("Failed to read from the log file.")?;
			let char = byte_to_char(buf[0]);
			if char == '\n' && !line.is_empty() {
				break;
			}
			if current_line_width + char.width().unwrap_or(0) > max_width {
				break;
			}
			current_line_bytes += 1;
			line.push_front(char);
			current_line_width += char.width().unwrap_or(0);
		}
		let mut lines = self.inner.lines.lock().unwrap();
		let mut lines_file_offset_start = self.inner.lines_file_offset_start.lock().unwrap();
		let mut lines_bytes = self.inner.lines_bytes.lock().unwrap();
		let line = line.into_iter().collect::<String>();
		lines.insert(0, line);
		lines_bytes.insert(0, current_line_bytes);
		*lines_file_offset_start -= current_line_bytes;

		Ok(())
	}

	fn rect(&self) -> tui::layout::Rect {
		*self.inner.rect.borrow()
	}

	fn resize(&mut self, rect: tui::layout::Rect) {
		self.inner.rect.send(rect).unwrap();
	}

	fn down(&mut self) {
		self.inner.sender.send(LogUpdate::Down).unwrap();
	}

	fn up(&mut self) {
		self.inner.sender.send(LogUpdate::Up).unwrap();
	}

	fn render(&self, rect: tui::layout::Rect, buf: &mut tui::buffer::Buffer) {
		let lines = self.inner.lines.lock().unwrap();
		for (y, line) in lines.iter().enumerate() {
			buf.set_line(
				rect.x,
				rect.y + y.to_u16().unwrap(),
				&tui::text::Line::raw(line),
				rect.width,
			);
		}
	}
}

impl Drop for LogInner {
	fn drop(&mut self) {
		if let Some(task) = self.task.lock().unwrap().take() {
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

fn byte_to_char(byte: u8) -> char {
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
}
