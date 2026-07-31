use {
	self::{data::Data, help::Help, log::Log, tree::Tree},
	anstream::println,
	crossterm::{self as ct, event::KeyModifiers},
	futures::{FutureExt as _, TryFutureExt as _, TryStreamExt as _, future},
	num::ToPrimitive as _,
	ratatui::{self as tui, prelude::*},
	std::{
		io::{IsTerminal as _, Write as _},
		os::fd::AsRawFd,
		pin::pin,
		time::Duration,
	},
	tangram_client::prelude::*,
	tangram_futures::task::Stopper,
	unicode_segmentation::UnicodeSegmentation as _,
	unicode_width::UnicodeWidthStr as _,
};

mod data;
mod help;
mod log;
mod tree;
mod util;

pub type UpdateReceiver = std::sync::mpsc::Receiver<Box<dyn FnOnce(&mut Viewer)>>;

pub type UpdateSender = std::sync::mpsc::Sender<Box<dyn FnOnce(&mut Viewer)>>;

pub struct Viewer {
	data: Data,
	exit: tokio::sync::oneshot::Receiver<()>,
	exited: bool,
	focus: Focus,
	help: Help,
	interrupt: bool,
	log: Option<Log>,
	quit: bool,
	split: Split,
	tree: Tree,
	tree_finished: bool,
	update_receiver: UpdateReceiver,
	_update_sender: UpdateSender,
}

#[derive(Clone, Debug, derive_more::TryUnwrap)]
pub enum Item {
	Group(tg::Group),
	Organization(tg::Organization),
	Process(tg::Process),
	Sandbox(tg::Sandbox),
	Tag(tg::Tag),
	User(tg::User),
	Value(tg::Value),
}

#[derive(Clone, Debug)]
pub struct Options {
	pub attached: bool,
	pub collapse_process_children: bool,
	pub depth: Option<u32>,
	pub expand_groups: bool,
	pub expand_metadata: bool,
	pub expand_objects: bool,
	pub expand_organizations: bool,
	pub expand_processes: bool,
	pub expand_sandboxes: bool,
	pub expand_tags: bool,
	pub expand_users: bool,
	pub expand_values: bool,
	pub show_process_commands: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Focus {
	Help,
	Tree,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Split {
	Horizontal,
	Vertical,
}

struct CursorGuard(std::fs::File);

struct TerminalSession {
	alternate_screen: bool,
	mouse_capture: bool,
	raw_mode: bool,
	terminal: tui::Terminal<tui::backend::CrosstermBackend<std::fs::File>>,
}

#[must_use]
pub fn clip(string: &str, max_width: usize) -> &str {
	let mut current_width = 0;
	for (index, grapheme) in string.grapheme_indices(true) {
		let grapheme_width = grapheme.width();
		if current_width + grapheme_width > max_width {
			return &string[..index];
		}
		current_width += grapheme_width;
	}

	string
}

impl Viewer {
	#[must_use]
	pub fn new(
		client: &tg::Client,
		root: tg::Referent<Item>,
		exit: tokio::sync::oneshot::Receiver<()>,
		options: Options,
	) -> Self {
		// Create the update channel.
		let (update_sender, update_receiver) = std::sync::mpsc::channel();

		// Create the views.
		let data = Data::new();
		let tree = Tree::new(
			client,
			root,
			options,
			data.update_sender(),
			update_sender.clone(),
		);
		let tree_finished = tree.is_finished();
		Self {
			data,
			exit,
			exited: false,
			focus: Focus::Tree,
			help: Help,
			interrupt: false,
			log: None,
			quit: false,
			split: Split::Vertical,
			tree,
			tree_finished,
			update_receiver,
			_update_sender: update_sender,
		}
	}

	pub async fn run_fullscreen(
		&mut self,
		stopper: Stopper,
		alternate_screen: bool,
	) -> tg::Result<()> {
		// Select the root.
		self.tree.ensure_root_selected();

		// Create the terminal.
		let Some(tty) = tangram_util::tty::open_controlling_tty() else {
			return self.run_inline(stopper, true).await;
		};
		let tty_fd = tty.as_raw_fd();
		if !tangram_util::tty::is_foreground_controlling_tty(tty_fd) {
			return self.run_inline(stopper, true).await;
		}

		// Set up the terminal session.
		let mut terminal = TerminalSession::new(tty, alternate_screen)?;

		// Create the event stream.
		let mut events = ct::event::EventStream::new();

		// Run the event loop.
		let mut dirty = true;
		let result = loop {
			// Stop when requested.
			if stopper.stopped() {
				break Ok(());
			}

			// Apply pending updates.
			dirty |= self.update();

			// Render a frame.
			if dirty || !self.tree.is_finished() {
				if let Err(error) = terminal
					.terminal
					.draw(|frame| self.render(frame.area(), frame.buffer_mut()))
				{
					break Err(tg::error!(!error, "failed to render the frame"));
				}
				dirty = false;
			}

			// Exit after rendering the final frame.
			if self.exited() {
				break Ok(());
			}

			// Wait for and handle an event.
			let sleep = tokio::time::sleep(Duration::from_millis(100));
			let event = events
				.try_next()
				.map_err(|error| tg::error!(!error, "failed to poll for an event"));
			match future::select(pin!(sleep), event).await {
				future::Either::Left(((), _)) => (),
				future::Either::Right((Err(error), _)) => {
					break Err(error);
				},
				future::Either::Right((Ok(None), _)) => {
					break Ok(());
				},
				future::Either::Right((Ok(Some(event)), _)) => {
					self.handle(&event);
					dirty = true;
				},
			}
		};

		// Restore the terminal before forwarding an interrupt to the process signal handler.
		drop(terminal);
		if self.interrupt {
			// SAFETY: Raising SIGINT forwards the terminal interrupt after the terminal state has been restored.
			unsafe {
				libc::raise(libc::SIGINT);
			}
		}

		result
	}

	pub async fn run_inline(&mut self, stopper: Stopper, print_final: bool) -> tg::Result<()> {
		let mut tty = if std::io::stderr().is_terminal() {
			tangram_util::tty::open_controlling_tty()
				.filter(|tty| tangram_util::tty::is_foreground_controlling_tty(tty.as_raw_fd()))
		} else {
			None
		};

		// Hide the cursor if necessary.
		let cursor_guard = if self.tree.has_process()
			&& let Some(tty) = tty.as_mut()
		{
			let guard = tty
				.try_clone()
				.map(CursorGuard)
				.map_err(|error| tg::error!(!error, "failed to clone the terminal"))?;
			ct::queue!(tty, ct::cursor::Hide)
				.map_err(|error| tg::error!(!error, "failed to write to the terminal"))?;
			tty.flush()
				.map_err(|error| tg::error!(!error, "failed to flush the terminal"))?;
			Some(guard)
		} else {
			None
		};

		// Run the render loop.
		loop {
			// Apply pending updates.
			self.update();

			// Finish the live display.
			if stopper.stopped() || self.tree.is_finished() {
				if self.tree.has_process()
					&& let Some(tty) = tty.as_mut()
				{
					ct::queue!(
						tty,
						ct::terminal::Clear(ct::terminal::ClearType::FromCursorDown),
						ct::cursor::Show,
					)
					.map_err(|error| tg::error!(!error, "failed to write to the terminal"))?;
					tty.flush()
						.map_err(|error| tg::error!(!error, "failed to flush the terminal"))?;
				}

				break;
			}

			// Render to the terminal or clear the display guards.
			if self.tree.has_process()
				&& let Some(tty) = tty.as_mut()
			{
				// Render the tree.
				let tree = self.tree.display().to_string();
				let tty_fd = tty.as_raw_fd();

				// Render within the terminal bounds.
				if let Some(size) = tangram_util::tty::get_tty_size(tty_fd) {
					let columns = size.cols.to_usize().unwrap();
					let rows = size.rows.to_usize().unwrap();

					// Fall back to row zero when the cursor position is unavailable.
					let row = tangram_util::tty::get_cursor_position(tty_fd)
						.map_or(0, |(_column, row)| row.to_usize().unwrap());

					// Clear the screen and save the cursor position.
					ct::queue!(
						tty,
						ct::terminal::Clear(ct::terminal::ClearType::FromCursorDown),
						ct::cursor::SavePosition,
					)
					.map_err(|error| tg::error!(!error, "failed to write to the terminal"))?;

					// Print the tree.
					let mut first = true;
					for line in tree.lines().take(rows.saturating_sub(row)) {
						if !first {
							writeln!(tty).map_err(|error| {
								tg::error!(!error, "failed to write to the terminal")
							})?;
						}
						first = false;
						let line = clip(line, columns);
						write!(tty, "{line}").map_err(|error| {
							tg::error!(!error, "failed to write to the terminal")
						})?;
					}

					// Restore the cursor position.
					ct::queue!(tty, ct::cursor::RestorePosition)
						.map_err(|error| tg::error!(!error, "failed to write to the terminal"))?;

					// Flush the terminal.
					tty.flush()
						.map_err(|error| tg::error!(!error, "failed to flush the terminal"))?;
				} else {
					self.tree.clear_guards();
				}
			} else {
				self.tree.clear_guards();
			}

			// Wait for the task to be stopped, a change, or a timeout.
			let mut stopper_wait = pin!(stopper.wait().fuse());
			let mut changed = pin!(self.tree.changed().fuse());
			let mut sleep = pin!(tokio::time::sleep(Duration::from_millis(100)).fuse());
			futures::select! {
				() = stopper_wait => (),
				() = changed => (),
				() = sleep => (),
			};
		}

		// Show the cursor before printing the final tree.
		drop(cursor_guard);

		// Render the tree one more time if necessary.
		if print_final {
			println!("{}", self.tree.display());
		}

		Ok(())
	}

	pub fn render(&mut self, area: Rect, buffer: &mut tui::buffer::Buffer) {
		if let Focus::Help = &self.focus {
			self.help.render(area, buffer);
			return;
		}

		// Compute the layout.
		let (tree_area, data_area, log_area) = self.layout(area);

		// Render the tree.
		let tree_area = render_block_and_get_area("Tree", false, tree_area, buffer);
		self.tree.render(tree_area, buffer);

		// Render the data.
		let data_area = render_block_and_get_area("Data", false, data_area, buffer);
		self.data.render(data_area, buffer);

		// Render the log.
		if let (Some(log), Some(log_area)) = (&self.log, log_area) {
			let log_area = render_block_and_get_area("Log", false, log_area, buffer);
			log.render(log_area, buffer);
		}
	}

	pub fn handle(&mut self, event: &ct::event::Event) {
		// Handle key events.
		if let ct::event::Event::Key(event) = event {
			match (event.code, event.modifiers) {
				(ct::event::KeyCode::Char('/'), ct::event::KeyModifiers::NONE) => {
					self.split = match self.split {
						Split::Horizontal => Split::Vertical,
						Split::Vertical => Split::Horizontal,
					}
				},
				(ct::event::KeyCode::Char('?'), ct::event::KeyModifiers::NONE) => {
					self.focus = match self.focus {
						Focus::Help => Focus::Tree,
						Focus::Tree => Focus::Help,
					};
					return;
				},
				(ct::event::KeyCode::Char('c'), ct::event::KeyModifiers::CONTROL) => {
					self.interrupt = true;
					return;
				},
				(ct::event::KeyCode::Char('q'), ct::event::KeyModifiers::NONE) => {
					self.quit = true;
					return;
				},
				(ct::event::KeyCode::Esc, ct::event::KeyModifiers::NONE) => {
					if self.focus == Focus::Help {
						self.focus = Focus::Tree;
					}
					return;
				},
				_ => (),
			}
		}

		// Handle mouse events.
		if let ct::event::Event::Mouse(event) = event {
			match event.kind {
				ct::event::MouseEventKind::ScrollDown => {
					if self.data.hit_test(event.column, event.row) {
						if event.modifiers.contains(KeyModifiers::SHIFT) {
							self.data.right();
						} else {
							self.data.down();
						}
					} else if let Some(log) = &self.log
						&& log.hit_test(event.column, event.row)
					{
						log.down();
					}
				},
				ct::event::MouseEventKind::ScrollLeft
					if self.data.hit_test(event.column, event.row) =>
				{
					self.data.left();
				},
				ct::event::MouseEventKind::ScrollRight
					if self.data.hit_test(event.column, event.row) =>
				{
					self.data.right();
				},
				ct::event::MouseEventKind::ScrollUp => {
					if self.data.hit_test(event.column, event.row) {
						if event.modifiers.contains(KeyModifiers::SHIFT) {
							self.data.left();
						} else {
							self.data.up();
						}
					} else if let Some(log) = &self.log
						&& log.hit_test(event.column, event.row)
					{
						log.up();
					}
				},
				_ => (),
			}
		}

		// Forward the event to the focused view.
		match &self.focus {
			Focus::Help => self.help.handle(event),
			Focus::Tree => self.tree.handle(event),
		}
	}

	pub fn update(&mut self) -> bool {
		let mut changed = self.tree.update();
		while let Ok(update) = self.update_receiver.try_recv() {
			update(self);
			changed = true;
		}
		changed |= self.data.update();
		changed |= self.log.as_ref().is_some_and(Log::take_dirty);
		let tree_finished = self.tree.is_finished();
		changed |= self.tree_finished != tree_finished;
		self.tree_finished = tree_finished;

		changed
	}

	fn exited(&mut self) -> bool {
		if !self.exited {
			self.exited = match self.exit.try_recv() {
				Err(tokio::sync::oneshot::error::TryRecvError::Closed) | Ok(()) => true,
				Err(tokio::sync::oneshot::error::TryRecvError::Empty) => false,
			};
		}
		self.interrupt || self.quit || (self.exited && self.tree.is_finished())
	}

	#[must_use]
	fn layout(&self, area: Rect) -> (Rect, Rect, Option<Rect>) {
		let (direction, log_direction) = match self.split {
			Split::Horizontal => (Direction::Vertical, Direction::Horizontal),
			Split::Vertical => (Direction::Horizontal, Direction::Vertical),
		};
		let areas = Layout::default()
			.direction(direction)
			.constraints([Constraint::Fill(1), Constraint::Fill(1)])
			.split(area);
		if self.log.is_some() {
			let tree_area = areas[0];
			let detail_areas = Layout::default()
				.direction(log_direction)
				.constraints([Constraint::Fill(1), Constraint::Fill(1)])
				.split(areas[1]);
			(tree_area, detail_areas[0], Some(detail_areas[1]))
		} else {
			(areas[0], areas[1], None)
		}
	}
}

impl TerminalSession {
	fn new(tty: std::fs::File, alternate_screen: bool) -> tg::Result<Self> {
		// Create the terminal.
		let backend = tui::backend::CrosstermBackend::new(tty);
		let terminal = tui::Terminal::new(backend)
			.map_err(|error| tg::error!(!error, "failed to create the terminal backend"))?;
		let mut session = Self {
			alternate_screen: false,
			mouse_capture: false,
			raw_mode: false,
			terminal,
		};

		// Enable raw mode.
		if !ct::terminal::is_raw_mode_enabled()
			.map_err(|error| tg::error!(!error, "failed to inspect raw mode"))?
		{
			ct::terminal::enable_raw_mode()
				.map_err(|error| tg::error!(!error, "failed to enable raw mode"))?;
			session.raw_mode = true;
		}

		// Enable mouse capture.
		session.mouse_capture = true;
		ct::execute!(
			session.terminal.backend_mut(),
			ct::event::EnableMouseCapture
		)
		.map_err(|error| tg::error!(!error, "failed to enable mouse capture"))?;

		// Enter the alternate screen.
		if alternate_screen {
			session.alternate_screen = true;
			ct::execute!(
				session.terminal.backend_mut(),
				ct::terminal::EnterAlternateScreen
			)
			.map_err(|error| tg::error!(!error, "failed to enter the alternate screen"))?;
		}

		Ok(session)
	}
}

impl Drop for CursorGuard {
	fn drop(&mut self) {
		ct::execute!(&mut self.0, ct::cursor::Show).ok();
	}
}

impl Drop for TerminalSession {
	fn drop(&mut self) {
		if self.mouse_capture {
			ct::execute!(self.terminal.backend_mut(), ct::event::DisableMouseCapture).ok();
		}
		if self.alternate_screen {
			ct::execute!(
				self.terminal.backend_mut(),
				ct::terminal::LeaveAlternateScreen
			)
			.ok();
		}
		if self.raw_mode {
			ct::terminal::disable_raw_mode().ok();
		}
	}
}

fn render_block_and_get_area(title: &str, focused: bool, area: Rect, buffer: &mut Buffer) -> Rect {
	let block = tui::widgets::Block::bordered()
		.title(title)
		.border_style(Style::default().fg(if focused { Color::Blue } else { Color::White }));
	block.render(area, buffer);
	Layout::default()
		.constraints([Constraint::Percentage(100)])
		.margin(1)
		.split(area)
		.first()
		.copied()
		.unwrap_or(area)
}
