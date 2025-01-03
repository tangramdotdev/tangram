use self::{help::Help, tree::Tree};
use crossterm as ct;
use futures::{future, TryFutureExt as _, TryStreamExt as _};
use num::ToPrimitive as _;
use ratatui::{self as tui, prelude::*};
use std::{
	io::{IsTerminal as _, Write as _},
	pin::pin,
	time::Duration,
};
use tangram_client as tg;
use tangram_futures::task::Stop;

mod help;
mod tree;

pub struct Viewer<H> {
	focus: Focus,
	handle: H,
	help: Help,
	stopped: bool,
	tree: Tree<H>,
}

#[derive(Clone, Debug)]
pub enum Item {
	Build(tg::Build),
	Value(tg::Value),
}

#[derive(Clone, Debug)]
pub struct Options {
	pub collapse_finished_builds: bool,
	pub expand_on_create: bool,
	pub hide_build_targets: bool,
	pub max_depth: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Focus {
	Data,
	Help,
	Log,
	Tree,
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
						_ => {
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
				_ => (),
			}
		}
		match &self.focus {
			Focus::Help => self.help.handle(event),
			Focus::Tree => self.tree.handle(event),
			_ => (),
		}
	}

	pub fn new(handle: &H, item: Item, options: Options) -> Self {
		let tree = Tree::new(handle, item, options);
		Self {
			focus: Focus::Tree,
			handle: handle.clone(),
			help: Help,
			stopped: false,
			tree,
		}
	}

	pub fn render(&mut self, rect: Rect, buffer: &mut tui::buffer::Buffer) {
		if let Focus::Help = &self.focus {
			self.help.render(rect, buffer);
			return;
		}
		self.tree.render(rect, buffer);
	}

	pub async fn run_fullscreen(&mut self, stop: Stop) -> tg::Result<()> {
		// Create the terminal.
		let tty = std::fs::OpenOptions::new()
			.read(true)
			.write(true)
			.open("/dev/tty")
			.map_err(|source| tg::error!(!source, "failed to open /dev/tty"))?;
		let backend = tui::backend::CrosstermBackend::new(tty);
		let mut terminal = tui::Terminal::new(backend)
			.map_err(|source| tg::error!(!source, "failed to create the terminal backend"))?;

		// Set up the terminal.
		ct::terminal::enable_raw_mode()
			.map_err(|source| tg::error!(!source, "failed to enable the terminal's raw mode"))?;
		ct::execute!(
			terminal.backend_mut(),
			ct::event::EnableMouseCapture,
			ct::terminal::EnterAlternateScreen,
		)
		.map_err(|source| tg::error!(!source, "failed to set up the terminal"))?;

		// Create the event stream.
		let mut events = ct::event::EventStream::new();

		// Run the event loop.
		while !stop.stopped() && !self.stopped {
			// Update.
			self.update();

			// Render.
			terminal
				.draw(|frame| self.render(frame.area(), frame.buffer_mut()))
				.map_err(|source| tg::error!(!source, "failed to render the frame"))?;

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
					return Err(error);
				},
				_ => (),
			}
		}

		// Reset the terminal.
		ct::execute!(
			terminal.backend_mut(),
			ct::event::DisableMouseCapture,
			ct::terminal::LeaveAlternateScreen,
		)
		.map_err(|source| tg::error!(!source, "failed to reset the terminal"))?;
		ct::terminal::disable_raw_mode()
			.map_err(|source| tg::error!(!source, "failed to disable the terminal's raw mode"))?;

		Ok(())
	}

	pub async fn run_inline(&mut self, stop: Stop) -> tg::Result<()> {
		let mut tty: Option<std::io::Stderr> = if std::io::stderr().is_terminal() {
			Some(std::io::stderr())
		} else {
			None
		};

		// Render the tree until it is ready.
		let mut lines = None;
		loop {
			// Update.
			self.update();

			// If stdout is a terminal, then render the tree.
			if let Some(tty) = tty.as_mut() {
				// Clear.
				if let Some(lines) = lines {
					ct::queue!(
						tty,
						ct::cursor::MoveToPreviousLine(lines),
						ct::terminal::Clear(ct::terminal::ClearType::FromCursorDown),
					)
					.unwrap();
				}

				// Print the tree.
				let tree = self.tree.display().to_string();
				writeln!(tty, "{tree}")
					.map_err(|source| tg::error!(!source, "failed to print the tree"))?;
				tty.flush()
					.map_err(|source| tg::error!(!source, "failed to flush the terminal"))?;
				lines = Some(tree.lines().count().to_u16().unwrap());
			}

			// Sleep. If the tree becomes ready or the task is stopped, then break.
			let sleep = tokio::time::sleep(Duration::from_millis(100));
			let ready = self.tree.ready();

			match future::select(future::select(pin!(ready), pin!(stop.wait())), pin!(sleep)).await
			{
				future::Either::Left((future::Either::Left(_), _)) => {
					break;
				},
				future::Either::Left((future::Either::Right(_), _)) => {
					break;
				},
				_ => continue,
			}
		}

		// Handle any pending updates.
		self.update();

		// Clear.
		if let (Some(tty), Some(lines)) = (tty.as_mut(), lines) {
			ct::queue!(
				tty,
				ct::cursor::MoveToPreviousLine(lines),
				ct::terminal::Clear(ct::terminal::ClearType::FromCursorDown),
			)
			.unwrap();
		}

		// Print the tree.
		println!("{}", self.tree.display());

		Ok(())
	}

	pub fn update(&mut self) {
		self.tree.update();
	}
}
