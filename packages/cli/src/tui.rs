use self::app::App;
use crossterm as ct;
use ct::event;
use ratatui as tui;
use std::sync::Arc;
use tangram_client as tg;
use tg::Handle;

mod app;
mod commands;
mod data;
mod detail;
mod log;
mod tree;
mod util;

pub struct Tui<H>
where
	H: tg::Handle,
{
	app: Arc<App<H>>,
	task: Option<tokio::task::JoinHandle<tg::Result<()>>>,
}

// multiroot
#[derive(Clone)]
pub enum Kind {
	Root,
	Build(tg::Build),
	Value {
		name: Option<String>,
		value: tg::Value,
	},
	Package {
		dependency: tg::Dependency,
		artifact: Option<tg::Artifact>,
		lock: tg::Lock,
	},
}

impl<H> Tui<H>
where
	H: Handle,
{
	pub async fn start(handle: &H, root: Kind) -> tg::Result<Self>
	where
		H: tg::Handle,
	{
		// Create the terminal.
		let tty = std::fs::OpenOptions::new()
			.read(true)
			.write(true)
			.open("/dev/tty")
			.map_err(|source| tg::error!(!source, "failed to open /dev/tty"))?;
		let backend = tui::backend::CrosstermBackend::new(tty);
		let mut terminal = tui::Terminal::new(backend)
			.map_err(|source| tg::error!(!source, "failed to create the terminal backend"))?;

		// Create the app.
		let rect = terminal.get_frame().size();
		let app = app::App::new(handle, root, rect);

		// Spawn the task.
		let task = tokio::task::spawn_blocking({
			let app = app.clone();
			move || Self::task(&mut terminal, &app)
		});

		Ok(Self {
			app,
			task: Some(task),
		})
	}

	pub fn stop(&self) {
		self.app.stop();
	}

	pub async fn wait(mut self) -> tg::Result<()> {
		// Get the task.
		let Some(task) = self.task.take() else {
			return Ok(());
		};

		// Join the task.
		task.await.unwrap().ok();

		// Join the app.
		self.app.wait().await;

		Ok(())
	}

	fn task(
		terminal: &mut tui::Terminal<tui::backend::CrosstermBackend<std::fs::File>>,
		app: &App<H>,
	) -> tg::Result<()> {
		// Enable raw mode.
		ct::terminal::enable_raw_mode()
			.map_err(|source| tg::error!(!source, "failed to enable the terminal's raw mode"))?;

		// Enable mouse capture and the alternate screen.
		ct::execute!(
			terminal.backend_mut(),
			ct::event::EnableMouseCapture,
			ct::terminal::EnterAlternateScreen,
		)
		.map_err(|source| tg::error!(!source, "failed to set up the terminal"))?;

		// Run the event loop.
		while !app.stopped() {
			// Render.
			terminal
				.draw(|frame| app.render(frame.size(), frame.buffer_mut()))
				.ok();

			// Wait for and handle an event, swallowing any errors.
			let Ok(has_event) = event::poll(std::time::Duration::from_millis(10)) else {
				break;
			};
			if !has_event {
				continue;
			}
			let Ok(event) = event::read() else {
				break;
			};
			app.handle_event(&event);
		}

		// Reset the terminal.
		terminal.clear().ok();

		ct::execute!(
			terminal.backend_mut(),
			ct::event::DisableMouseCapture,
			ct::terminal::LeaveAlternateScreen
		)
		.ok();

		ct::terminal::disable_raw_mode()
			.map_err(|source| tg::error!(!source, "failed to disable the terminal's raw mode"))
			.ok();

		Ok(())
	}
}
