use futures::{future, TryStreamExt as _};
use num::ToPrimitive as _;
use ratatui::{self as tui, prelude::*};
use std::{
	io::SeekFrom,
	sync::{
		atomic::{AtomicBool, AtomicU64, Ordering},
		Arc,
	},
};
use tangram_client as tg;
mod scroll;

pub struct Log<H> {
	// The build.
	build: tg::Build,

	// A buffer of log chunks.
	chunks: tokio::sync::Mutex<Vec<tg::build::log::Chunk>>,

	// Whether the log has reached EOF.
	eof: AtomicBool,

	// Channel used to send UI events.
	event_sender: tokio::sync::mpsc::UnboundedSender<LogEvent>,

	// The event handler task.
	event_task: std::sync::Mutex<Option<tokio::task::JoinHandle<tg::Result<()>>>>,

	// The handle.
	handle: H,

	// The lines of text that will be displayed.
	lines: std::sync::Mutex<Vec<String>>,

	// The maximum position of the log seen so far.
	max_position: AtomicU64,

	// The bounding box of the log view.
	rect: tokio::sync::watch::Sender<Rect>,

	// The current state of the log's scrolling position.
	scroll: tokio::sync::Mutex<Option<scroll::Scroll>>,

	// The log streaming task.
	task: std::sync::Mutex<Option<tokio::task::JoinHandle<tg::Result<()>>>>,

	// A watch to be notified when new logs are received from the log task.
	watch: tokio::sync::Mutex<Option<tokio::sync::watch::Receiver<()>>>,
}

enum LogEvent {
	ScrollUp,
	ScrollDown,
}

impl<H> Log<H>
where
	H: tg::Handle,
{
	pub fn new(handle: &H, build: &tg::Build, rect: Rect) -> Arc<Self> {
		let handle = handle.clone();
		let build = build.clone();
		let chunks = tokio::sync::Mutex::new(Vec::new());
		let (event_sender, mut event_receiver) = tokio::sync::mpsc::unbounded_channel();
		let lines = std::sync::Mutex::new(Vec::new());
		let (rect, mut rect_receiver) = tokio::sync::watch::channel(rect);

		let log = Arc::new(Log {
			build,
			chunks,
			handle,
			eof: AtomicBool::new(false),
			event_sender,
			event_task: std::sync::Mutex::new(None),
			lines,
			task: std::sync::Mutex::new(None),
			watch: tokio::sync::Mutex::new(None),
			max_position: AtomicU64::new(0),
			rect,
			scroll: tokio::sync::Mutex::new(None),
		});

		// Create the event handler task.
		let event_task = tokio::task::spawn({
			let log = log.clone();
			async move {
				log.init().await?;
				loop {
					let log_receiver = log.watch.lock().await.clone();
					let log_receiver = async move {
						if let Some(mut log_receiver) = log_receiver {
							log_receiver.changed().await.ok()
						} else {
							future::pending().await
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
							let mut scroll = log.scroll.lock().await;
							if let Some(scroll) = scroll.as_mut() {
								scroll.rect = log.rect();
							}
						},
					}
					log.update_lines().await?;
				}
			}
		});
		log.event_task.lock().unwrap().replace(event_task);

		log
	}

	async fn init(self: &Arc<Self>) -> tg::Result<()> {
		let client = &self.handle;
		let position = Some(std::io::SeekFrom::End(0));
		let length = Some(0);
		let timeout = Some(std::time::Duration::from_millis(16));

		// Get at least one chunk.
		let chunk = self
			.build
			.log(
				client,
				tg::build::log::Arg {
					length,
					position,
					timeout,
					..Default::default()
				},
			)
			.await?
			.try_next()
			.await?;

		let max_position = chunk.map_or(0, |chunk| {
			chunk.position + chunk.bytes.len().to_u64().unwrap()
		});
		self.max_position.store(max_position, Ordering::Relaxed);

		// Seed the front of the log.
		if max_position > 0 {
			self.update_log_stream(false).await?;
		}

		// Start tailing if necessary.
		self.update_log_stream(true).await?;
		Ok(())
	}

	pub fn stop(&self) {
		if let Some(task) = self.task.lock().unwrap().take() {
			task.abort();
		}
		if let Some(task) = self.event_task.lock().unwrap().take() {
			task.abort();
		}
	}

	fn is_complete(&self) -> bool {
		self.eof.load(Ordering::SeqCst)
	}

	// Handle a scroll up event.
	async fn up_impl(self: &Arc<Self>) -> tg::Result<()> {
		// Create the scroll state if necessary.
		if self.scroll.lock().await.is_none() {
			loop {
				let chunks = self.chunks.lock().await;
				match scroll::Scroll::new(self.rect(), &chunks) {
					Ok(inner) => {
						self.scroll.lock().await.replace(inner);
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
			let mut scroll = self.scroll.lock().await;
			let scroll = scroll.as_mut().unwrap();
			let chunks = self.chunks.lock().await;
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
	async fn down_impl(self: &Arc<Self>) -> tg::Result<()> {
		loop {
			let mut scroll = self.scroll.lock().await;
			let Some(scroll_) = scroll.as_mut() else {
				return Ok(());
			};
			let chunks = self.chunks.lock().await;
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
	async fn update_lines(self: &Arc<Self>) -> tg::Result<()> {
		loop {
			let chunks = self.chunks.lock().await;
			if chunks.is_empty() {
				self.lines.lock().unwrap().clear();
				return Ok(());
			}
			let mut scroll = self.scroll.lock().await;
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
					*self.lines.lock().unwrap() = lines;
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
	async fn update_log_stream(self: &Arc<Self>, append: bool) -> tg::Result<()> {
		// If we're appending and the task already exists, just wait for more data to be available.
		if append && self.watch.lock().await.is_some() {
			let mut watch = self.watch.lock().await.clone().unwrap();
			watch.changed().await.ok();
			return Ok(());
		}

		// Otherwise, abort an existing log task.
		if let Some(task) = self.task.lock().unwrap().take() {
			task.abort();
		}

		// Compute the position and length.
		let area = self.rect().area().to_i64().unwrap();
		let mut chunks = self.chunks.lock().await;
		let max_position = self.max_position.load(Ordering::Relaxed);
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
			.build
			.log(
				&self.handle,
				tg::build::log::Arg {
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
					let mut chunks = log.chunks.lock().await;
					if chunk.bytes.is_empty() {
						log.eof.store(true, Ordering::SeqCst);
						break;
					}
					let max_position = chunk.position + chunk.bytes.len().to_u64().unwrap();
					log.max_position.fetch_max(max_position, Ordering::AcqRel);
					chunks.push(chunk);
					drop(chunks);
					tx.send(()).ok();
				}
				log.watch.lock().await.take();
				Ok::<_, tg::Error>(())
			});
			self.task.lock().unwrap().replace(task);
			self.watch.lock().await.replace(rx);
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
		*self.rect.borrow()
	}

	/// Send a scroll up event.
	pub fn up(&self) {
		self.event_sender.send(LogEvent::ScrollUp).ok();
	}

	/// Send a scroll down event.
	pub fn down(&self) {
		self.event_sender.send(LogEvent::ScrollDown).ok();
	}

	/// Send a resize event.
	pub fn resize(&self, rect: Rect) {
		self.rect.send(rect).ok();
	}

	pub fn hit_test(&self, x: u16, y: u16) -> bool {
		let position = tui::layout::Position::new(x, y);
		self.rect().contains(position)
	}

	/// Render the log.
	pub fn render(&self, area: tui::layout::Rect, buf: &mut tui::buffer::Buffer) {
		let lines = self.lines.lock().unwrap();
		for (y, line) in (0..area.height).zip(lines.iter()) {
			buf.set_line(area.x, area.y + y, &tui::text::Line::raw(line), area.width);
		}
	}
}

impl<H> Drop for Log<H> {
	fn drop(&mut self) {
		if let Some(task) = self.event_task.lock().unwrap().take() {
			task.abort();
		}
		if let Some(task) = self.task.lock().unwrap().take() {
			task.abort();
		}
	}
}
