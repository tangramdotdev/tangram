use self::app::App;
use crate::Cli;
use crossterm as ct;
use ratatui as tui;
use std::{sync::Arc, time::Duration};
use tangram_client as tg;
use tangram_either::Either;

mod app;
mod commands;
mod data;
mod detail;
mod info;
mod log;
mod tree;
mod util;

/// View a build or value.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	/// If this flag is set, the package's lockfile will not be updated.
	#[arg(long)]
	pub locked: bool,

	/// The reference to the build or value to view.
	#[arg(index = 1, default_value = ".")]
	pub reference: tg::Reference,
}

pub struct Viewer<H>
where
	H: tg::Handle,
{
	app: Arc<App<H>>,
	task: Option<tokio::task::JoinHandle<tg::Result<()>>>,
}

impl Cli {
	pub async fn command_view(&self, args: Args) -> tg::Result<()> {
		let handle = self.handle().await?;

		// Get the reference.
		let referent = self.get_reference(&args.reference).await?;
		let item = match referent.item {
			Either::Left(build) => Either::Left(build),
			Either::Right(object) => {
				let object = if let Some(subpath) = &referent.subpath {
					let directory = object
						.try_unwrap_directory()
						.ok()
						.ok_or_else(|| tg::error!("expected a directory"))?;
					directory.get(&handle, subpath).await?.into()
				} else {
					object
				};
				Either::Right(object)
			},
		};

		// Get the node kind.
		let node_kind = match item {
			Either::Left(build) => self::tree::NodeKind::Build {
				build,
				remote: None,
			},
			Either::Right(object) => self::tree::NodeKind::Value {
				name: None,
				value: object.into(),
			},
		};

		// Start the viewer.
		let viewer = Viewer::start(&handle, node_kind).await?;

		// Wait for the viewer to finish.
		viewer.wait().await;

		Ok(())
	}
}

impl<H> Viewer<H>
where
	H: tg::Handle,
{
	pub async fn start(handle: &H, node_kind: self::tree::NodeKind) -> tg::Result<Self>
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
		let rect = terminal.get_frame().area();
		let app = app::App::new(handle, node_kind, rect);

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

	#[allow(dead_code)]
	pub fn stop(&self) {
		self.app.stop();
	}

	pub async fn wait(mut self) {
		// Get the task.
		let Some(task) = self.task.take() else {
			return;
		};

		// Join the task.
		task.await.unwrap().ok();

		// Join the app.
		self.app.wait().await;
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
				.draw(|frame| app.render(frame.area(), frame.buffer_mut()))
				.ok();

			// Wait for and handle an event, swallowing any errors.
			let Ok(has_event) = ct::event::poll(Duration::from_millis(10)) else {
				break;
			};
			if !has_event {
				continue;
			}
			let Ok(event) = ct::event::read() else {
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
