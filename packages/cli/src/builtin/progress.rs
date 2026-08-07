use {
	crate::viewer::clip,
	crossterm as ct,
	std::{
		io::Write as _,
		sync::{
			Arc,
			atomic::{AtomicU64, Ordering},
		},
		time::Duration,
	},
	tangram_client::prelude::*,
	tangram_futures::task::Task,
};

pub struct Progress {
	is_tty: bool,
	position: Arc<AtomicU64>,
	task: Task<()>,
}

impl Progress {
	pub fn new(title: &str, total: Option<u64>) -> tg::Result<Self> {
		let position = Arc::new(AtomicU64::new(0));
		Self::with_position(title, total, position)
	}

	pub fn with_position(
		title: &str,
		total: Option<u64>,
		position: Arc<AtomicU64>,
	) -> tg::Result<Self> {
		let is_tty = tangram_util::tty::is_foreground_controlling_tty(libc::STDERR_FILENO);
		write_indicator(&position, title, total, is_tty, false)?;
		let task = Task::spawn({
			let position = position.clone();
			let title = title.to_owned();
			move |_| async move {
				loop {
					tokio::time::sleep(Duration::from_secs(1)).await;
					if write_indicator(&position, &title, total, is_tty, true).is_err() {
						break;
					}
				}
			}
		});
		let progress = Self {
			is_tty,
			position,
			task,
		};

		Ok(progress)
	}

	#[must_use]
	pub fn position(&self) -> Arc<AtomicU64> {
		self.position.clone()
	}

	pub fn finish(mut self, message: &str) -> tg::Result<()> {
		self.task.abort();
		self.clear()?;
		write_message(message)
	}

	fn clear(&mut self) -> tg::Result<()> {
		if !self.is_tty {
			return Ok(());
		}
		self.is_tty = false;
		clear_indicator()
	}
}

impl Drop for Progress {
	fn drop(&mut self) {
		self.task.abort();
		self.clear().ok();
	}
}

pub fn write_message(message: &str) -> tg::Result<()> {
	let mut stderr = std::io::stderr().lock();
	writeln!(stderr, "{message}")
		.map_err(|error| tg::error!(!error, "failed to write a progress message"))?;
	stderr
		.flush()
		.map_err(|error| tg::error!(!error, "failed to flush a progress message"))?;

	Ok(())
}

fn clear_indicator() -> tg::Result<()> {
	let mut stderr = std::io::stderr().lock();
	ct::queue!(
		stderr,
		ct::cursor::MoveToPreviousLine(1),
		ct::terminal::Clear(ct::terminal::ClearType::FromCursorDown),
	)
	.map_err(|error| tg::error!(!error, "failed to clear a progress indicator"))?;
	stderr
		.flush()
		.map_err(|error| tg::error!(!error, "failed to flush a progress indicator"))?;

	Ok(())
}

fn write_indicator(
	position: &AtomicU64,
	title: &str,
	total: Option<u64>,
	is_tty: bool,
	redraw: bool,
) -> tg::Result<()> {
	let current = position.load(Ordering::Relaxed);
	let indicator = tg::progress::Indicator {
		current: Some(current),
		format: tg::progress::IndicatorFormat::Bytes,
		name: String::new(),
		title: title.to_owned(),
		total,
	};
	let line = format!("{title} {indicator}");
	let line = if is_tty {
		let columns = ct::terminal::size()
			.map(|(columns, _)| usize::from(columns))
			.ok()
			.filter(|&columns| columns > 0)
			.unwrap_or(64);
		clip(&line, columns)
	} else {
		&line
	};
	let mut stderr = std::io::stderr().lock();
	if is_tty && redraw {
		ct::queue!(
			stderr,
			ct::cursor::MoveToPreviousLine(1),
			ct::terminal::Clear(ct::terminal::ClearType::FromCursorDown),
		)
		.map_err(|error| tg::error!(!error, "failed to clear a progress indicator"))?;
	}
	writeln!(stderr, "{line}")
		.map_err(|error| tg::error!(!error, "failed to write a progress indicator"))?;
	stderr
		.flush()
		.map_err(|error| tg::error!(!error, "failed to flush a progress indicator"))?;

	Ok(())
}
