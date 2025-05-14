use self::{data::Data, help::Help, log::Log, tree::Tree};
use crossterm as ct;
use futures::{TryFutureExt as _, TryStreamExt as _, future};
use num::ToPrimitive as _;
use ratatui::{self as tui, prelude::*};
use std::{
	io::{IsTerminal as _, Write as _},
	os::fd::AsRawFd,
	pin::pin,
	sync::Arc,
	time::Duration,
};
use tangram_client as tg;
use tangram_either::Either;
use tangram_futures::task::Stop;
use unicode_width::UnicodeWidthChar as _;

mod data;
mod help;
mod log;
mod tree;

pub struct Viewer<H> {
	data: Data,
	focus: Focus,
	help: Help,
	log: Option<Arc<Log<H>>>,
	split: Split,
	stopped: bool,
	tree: Tree<H>,
	update_receiver: UpdateReceiver<H>,
	_update_sender: UpdateSender<H>,
}

pub type UpdateSender<H> = std::sync::mpsc::Sender<Box<dyn FnOnce(&mut Viewer<H>)>>;

pub type UpdateReceiver<H> = std::sync::mpsc::Receiver<Box<dyn FnOnce(&mut Viewer<H>)>>;

#[derive(Clone, Debug, derive_more::TryUnwrap)]
pub enum Item {
	Process(tg::Process),
	Value(tg::Value),
}

#[derive(Clone, Debug)]
pub struct Options {
	pub condensed_processes: bool,
	pub expand_on_create: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Focus {
	Help,
	Tree,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Split {
	Horizontal,
	Vertical,
}

impl<H> Viewer<H>
where
	H: tg::Handle,
{
	pub fn handle(&mut self, event: &ct::event::Event) {
		if let ct::event::Event::Key(event) = event {
			match (event.code, event.modifiers) {
				(ct::event::KeyCode::Char('?'), ct::event::KeyModifiers::NONE) => {
					match self.focus {
						Focus::Help => {
							self.focus = Focus::Tree;
						},
						Focus::Tree => {
							self.focus = Focus::Help;
						},
					}
					return;
				},
				(ct::event::KeyCode::Esc, ct::event::KeyModifiers::NONE) => {
					if self.focus == Focus::Help {
						self.focus = Focus::Tree;
					}
					return;
				},
				(ct::event::KeyCode::Char('q'), ct::event::KeyModifiers::NONE) => {
					self.stopped = true;
					return;
				},
				(ct::event::KeyCode::Char('/'), ct::event::KeyModifiers::NONE) => {
					self.split = match self.split {
						Split::Horizontal => Split::Vertical,
						Split::Vertical => Split::Horizontal,
					}
				},
				_ => (),
			}
		}
		if let ct::event::Event::Mouse(event) = event {
			match event.kind {
				ct::event::MouseEventKind::ScrollUp => {
					if self.data.hit_test(event.column, event.row) {
						self.data.up();
					} else if let Some(log) = &self.log {
						if log.hit_test(event.column, event.row) {
							log.up();
						}
					}
				},
				ct::event::MouseEventKind::ScrollDown => {
					if self.data.hit_test(event.column, event.row) {
						self.data.down();
					} else if let Some(log) = &self.log {
						if log.hit_test(event.column, event.row) {
							log.down();
						}
					}
				},
				_ => (),
			}
		}
		match &self.focus {
			Focus::Help => self.help.handle(event),
			Focus::Tree => self.tree.handle(event),
		}
	}

	fn layout(&self, area: Rect) -> (Rect, Rect, Option<Rect>) {
		let (direction, log_direction) = match self.split {
			Split::Horizontal => (Direction::Vertical, Direction::Horizontal),
			Split::Vertical => (Direction::Horizontal, Direction::Vertical),
		};
		let rects = Layout::default()
			.direction(direction)
			.constraints([Constraint::Fill(1), Constraint::Fill(1)])
			.split(area);
		if self.log.is_some() {
			let tree = rects[0];
			let rects = Layout::default()
				.direction(log_direction)
				.constraints([Constraint::Fill(1), Constraint::Fill(1)])
				.split(rects[1]);
			(tree, rects[0], Some(rects[1]))
		} else {
			(rects[0], rects[1], None)
		}
	}

	pub fn new(
		handle: &H,
		root: Option<tg::Referent<Either<tg::Process, tg::Object>>>,
		item: Item,
		options: Options,
	) -> Self {
		let (update_sender, update_receiver) = std::sync::mpsc::channel();
		let data = Data::new();
		let tree = Tree::new(
			handle,
			item,
			root,
			options,
			data.update_sender(),
			update_sender.clone(),
		);
		Self {
			data,
			focus: Focus::Tree,
			help: Help,
			log: None,
			split: Split::Vertical,
			stopped: false,
			tree,
			update_receiver,
			_update_sender: update_sender,
		}
	}

	pub fn render(&mut self, rect: Rect, buffer: &mut tui::buffer::Buffer) {
		if let Focus::Help = &self.focus {
			self.help.render(rect, buffer);
			return;
		}

		// Get the layout.
		let (tree, data, log) = self.layout(rect);

		// Render the tree.
		let tree = render_block_and_get_area("Tree", false, tree, buffer);
		self.tree.render(tree, buffer);

		// Render the data.
		let data = render_block_and_get_area("Data", false, data, buffer);
		self.data.render(data, buffer);

		// Render the log if it exists.
		if let Some(log_) = &self.log {
			let log = render_block_and_get_area("Log", false, log.unwrap(), buffer);
			log_.render(log, buffer);
		}
	}

	pub async fn run_fullscreen(&mut self, stop: Stop) -> tg::Result<()> {
		// Create the terminal.
		let tty = std::fs::OpenOptions::new()
			.read(true)
			.write(true)
			.open("/dev/tty")
			.map_err(|source| tg::error!(!source, "failed to open /dev/tty"))?;
		let ttyfd = tty.as_raw_fd();

		// Set up the backend.
		let backend = tui::backend::CrosstermBackend::new(tty);
		let mut terminal = tui::Terminal::new(backend)
			.map_err(|source| tg::error!(!source, "failed to create the terminal backend"))?;

		// Enable mouse capture and enter an alternate screen.
		ct::execute!(
			terminal.backend_mut(),
			ct::event::EnableMouseCapture,
			ct::terminal::EnterAlternateScreen,
		)
		.map_err(|source| tg::error!(!source, "failed to enable mouse capture"))?;

		// Get the termios and enable raw mode.
		let termios = unsafe {
			// Get the original termios.
			let mut termios = std::mem::MaybeUninit::uninit();
			if libc::tcgetattr(ttyfd, termios.as_mut_ptr()) != 0 {
				return Err(tg::error!(
					source = std::io::Error::last_os_error(),
					"failed to get termios"
				));
			}
			let old_termios = termios.assume_init();

			// Enable raw mode.
			let mut new_termios = old_termios;
			new_termios.c_lflag &= !(libc::ECHO | libc::ICANON | libc::ISIG | libc::IEXTEN);
			new_termios.c_iflag &=
				!(libc::IXON | libc::ICRNL | libc::BRKINT | libc::INPCK | libc::ISTRIP);
			new_termios.c_oflag &= !(libc::OPOST);
			if libc::tcsetattr(ttyfd, libc::TCSAFLUSH, std::ptr::addr_of!(new_termios)) != 0 {
				return Err(tg::error!(
					source = std::io::Error::last_os_error(),
					"failed to set raw mode"
				));
			}

			// Return the original termios.
			old_termios
		};

		// Create the event stream.
		let mut events = ct::event::EventStream::new();

		// Run the event loop.
		let result = loop {
			if stop.stopped() || self.stopped {
				break Ok(());
			}

			// Update.
			self.update();

			// Try to render.
			if let Err(source) =
				terminal.draw(|frame| self.render(frame.area(), frame.buffer_mut()))
			{
				break Err(tg::error!(!source, "failed to render the frame"));
			}

			// Wait for and handle an event.
			let sleep = tokio::time::sleep(Duration::from_millis(100));
			let event = events
				.try_next()
				.map_err(|source| tg::error!(!source, "failed to poll for an event"));
			match future::select(pin!(sleep), event).await {
				future::Either::Right((Ok(Some(event)), _)) => {
					self.handle(&event);
				},
				future::Either::Right((Err(error), _)) => {
					break Err(error);
				},
				_ => (),
			}
		};

		// Restore the original termios, ignoring errors.
		unsafe {
			libc::tcsetattr(ttyfd, libc::TCSAFLUSH, std::ptr::addr_of!(termios));
		}

		// Disable mouse capture and leave the alternate screen, ignoring errors.
		ct::execute!(
			terminal.backend_mut(),
			ct::event::DisableMouseCapture,
			ct::terminal::LeaveAlternateScreen,
		)
		.ok();

		result
	}

	pub async fn run_inline(&mut self, stop: Stop) -> tg::Result<()> {
		let mut tty: Option<std::io::Stderr> = if std::io::stderr().is_terminal() {
			Some(std::io::stderr())
		} else {
			None
		};

		// Render the tree.
		let mut lines = None;
		loop {
			// Update.
			self.update();

			// If stdout is a terminal, then render the tree.
			if let Some(tty) = tty.as_mut() {
				// Get the size of the tty.
				let (columns, rows) = ct::terminal::size().map_or((64, 64), |(columns, rows)| {
					(columns.to_usize().unwrap(), rows.to_usize().unwrap())
				});

				// Clear.
				match lines {
					Some(lines) if lines > 0 => {
						ct::queue!(
							tty,
							ct::cursor::MoveToPreviousLine(lines),
							ct::terminal::Clear(ct::terminal::ClearType::FromCursorDown),
						)
						.unwrap();
					},
					_ => (),
				}

				// Print the tree.
				let tree = self.tree.display().to_string();
				let mut count = 0;
				for line in tree.lines().take(rows) {
					let line = clip(line, columns);
					writeln!(tty, "{line}")
						.map_err(|source| tg::error!(!source, "failed to print the tree"))?;
					count += 1;
				}
				tty.flush()
					.map_err(|source| tg::error!(!source, "failed to flush the terminal"))?;
				lines.replace(count);
			}

			// Sleep. If the task is stopped, then break.
			let sleep = tokio::time::sleep(Duration::from_millis(100));
			if let future::Either::Left(_) = future::select(pin!(stop.wait()), pin!(sleep)).await {
				break;
			}
		}

		// Handle any pending updates.
		self.update();

		// Clear.
		match (tty.as_mut(), lines) {
			(Some(tty), Some(lines)) if lines > 0 => {
				ct::queue!(
					tty,
					ct::cursor::MoveToPreviousLine(lines),
					ct::terminal::Clear(ct::terminal::ClearType::FromCursorDown),
				)
				.unwrap();
			},
			_ => (),
		}

		Ok(())
	}

	pub fn tree(&self) -> &Tree<H> {
		&self.tree
	}

	pub fn update(&mut self) {
		while let Ok(update) = self.update_receiver.try_recv() {
			update(self);
		}
		self.tree.update();
		self.data.update();
	}
}

fn render_block_and_get_area(title: &str, focused: bool, area: Rect, buf: &mut Buffer) -> Rect {
	let block = tui::widgets::Block::bordered()
		.title(title)
		.border_style(Style::default().fg(if focused { Color::Blue } else { Color::White }));
	block.render(area, buf);
	Layout::default()
		.constraints([Constraint::Percentage(100)])
		.margin(1)
		.split(area)
		.first()
		.copied()
		.unwrap_or(area)
}

pub(crate) fn clip(string: &str, mut width: usize) -> &str {
	let mut len = 0;
	let mut chars = string.chars();
	while width > 0 {
		let Some(char) = chars.next() else {
			break;
		};
		len += char.len_utf8();
		width = width.saturating_sub(char.width().unwrap_or(0));
	}
	&string[0..len]
}
