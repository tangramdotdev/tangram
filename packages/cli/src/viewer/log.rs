#![allow(dead_code)]

use {
	futures::{FutureExt, TryStreamExt as _},
	num::ToPrimitive as _,
	ratatui::{self as tui, prelude::*},
	std::{
		io::SeekFrom,
		sync::{
			Arc, Mutex,
			atomic::{AtomicBool, AtomicU64, Ordering},
		},
	},
	tangram_client::prelude::*,
};

pub mod scroll;

pub struct Log<H> {
	// A buffer of log chunks.
	chunks: tokio::sync::Mutex<Vec<tg::process::log::get::Chunk>>,

	// Whether the log has reached EOF.
	eof: AtomicBool,

	// Channel used to send UI events.
	event_sender: tokio::sync::mpsc::UnboundedSender<LogEvent>,

	// The event handler task.
	event_task: Mutex<Option<tokio::task::JoinHandle<tg::Result<()>>>>,

	// The handle.
	handle: H,

	// The lines of text that will be displayed.
	lines: Mutex<Vec<String>>,

	// The maximum position of the log seen so far.
	max_position: AtomicU64,

	// The process.
	process: tg::Process,

	// The current state of the log's scrolling position.
	scroll: tokio::sync::Mutex<Option<scroll::Scroll>>,

	// The log streaming task.
	task: Mutex<Option<tokio::task::JoinHandle<tg::Result<()>>>>,

	// A watch to be notified when new logs are received from the log task.
	notify: tokio::sync::Notify,

	rect: std::sync::Mutex<Option<Rect>>,
}

enum LogEvent {
	ScrollUp,
	ScrollDown,
}

impl<H> Log<H>
where
	H: tg::Handle,
{
	pub fn _stop(&self) {
		if let Some(task) = self.task.lock().unwrap().take() {
			task.abort();
		}
		if let Some(task) = self.event_task.lock().unwrap().take() {
			task.abort();
		}
	}

	/// Send a scroll down event.
	pub fn down(&self) {
		self.event_sender.send(LogEvent::ScrollDown).ok();
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
				// If the scroll succeeded but we didn't scroll any lines and the process is not yet complete, we need to start tailing.
				Ok(count) if count != 1 && !self.is_complete() => {
					drop(chunks);
					scroll.take();
					self.tail_log().await?;
				},
				Ok(_) => {
					return Ok(());
				},
				Err(error) => {
					drop(chunks);
					self.try_update_chunks(error).await?;
				},
			}
		}
	}

	pub fn hit_test(&self, x: u16, y: u16) -> bool {
		let Some(rect) = *self.rect.lock().unwrap() else {
			return false;
		};
		let position = Position { x, y };
		rect.contains(position)
	}

	fn is_complete(&self) -> bool {
		self.eof.load(Ordering::SeqCst)
	}

	pub fn new(handle: &H, process: &tg::Process) -> Arc<Self> {
		let handle = handle.clone();
		let process = process.clone();
		let chunks = tokio::sync::Mutex::new(Vec::new());
		let (event_sender, mut event_receiver) = tokio::sync::mpsc::unbounded_channel();
		let lines = Mutex::new(Vec::new());

		let log = Arc::new(Log {
			process,
			chunks,
			handle,
			eof: AtomicBool::new(false),
			event_sender,
			event_task: Mutex::new(None),
			lines,
			task: Mutex::new(None),
			notify: tokio::sync::Notify::new(),
			max_position: AtomicU64::new(0),
			scroll: tokio::sync::Mutex::new(None),
			rect: std::sync::Mutex::new(None),
		});

		// Create the event handler task.
		let event_task = tokio::spawn({
			let log = log.clone();
			async move {
				log.tail_log().await?;
				loop {
					let notification = log.notify.notified();
					tokio::select! {
						event = event_receiver.recv() => match event.unwrap() {
							LogEvent::ScrollDown => {
								log.down_impl().await.ok();
							}
							LogEvent::ScrollUp => {
								log.up_impl().await.ok();
							}
						},
						_ = notification => (),
					}
					log.update_lines().await?;
				}
			}
		});
		log.event_task.lock().unwrap().replace(event_task);
		log
	}

	/// Render the log.
	pub fn render(&self, area: tui::layout::Rect, buf: &mut tui::buffer::Buffer) {
		self.rect.lock().unwrap().replace(area);
		let lines = self.lines.lock().unwrap();
		for (y, line) in (0..area.height).zip(lines.iter()) {
			buf.set_line(area.x, area.y + y, &tui::text::Line::raw(line), area.width);
		}
	}

	/// Send a scroll up event.
	pub fn up(&self) {
		self.event_sender.send(LogEvent::ScrollUp).ok();
	}

	// Handle a scroll up event.
	async fn up_impl(self: &Arc<Self>) -> tg::Result<()> {
		let Some(rect) = *self.rect.lock().unwrap() else {
			return Ok(());
		};

		// Create the scroll state if necessary.
		if self.scroll.lock().await.is_none() {
			loop {
				let chunks = self.chunks.lock().await;
				if chunks.is_empty() {
					return Ok(());
				}
				match scroll::Scroll::new(rect, &chunks) {
					Ok(inner) => {
						self.scroll.lock().await.replace(inner);
						break;
					},
					Err(error) => {
						drop(chunks);
						self.try_update_chunks(error).await?;
					},
				}
			}
		}

		// Attempt to scroll up by 1 line.
		loop {
			let mut scroll = self.scroll.lock().await;
			let scroll = scroll.as_mut().unwrap();
			let chunks = self.chunks.lock().await;
			match scroll.scroll_up(1, &chunks) {
				Ok(_) => {
					return Ok(());
				},
				Err(error) => {
					drop(chunks);
					self.try_update_chunks(error).await?;
				},
			}
		}
	}

	// Update the rendered lines.
	async fn update_lines(self: &Arc<Self>) -> tg::Result<()> {
		let Some(rect) = *self.rect.lock().unwrap() else {
			return Ok(());
		};
		loop {
			let chunks = self.chunks.lock().await;
			if chunks.is_empty() {
				self.lines.lock().unwrap().clear();
				return Ok(());
			}
			let mut scroll = self.scroll.lock().await;
			let result = scroll.as_mut().map_or_else(
				|| {
					let mut scroll = scroll::Scroll::new(rect, &chunks)?;
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
					self.try_update_chunks(error).await?;
				},
			}
		}

		Ok(())
	}

	async fn tail_log(self: &Arc<Self>) -> tg::Result<()> {
		// Cancel the task if it's running.
		if let Some(task) = self.task.lock().unwrap().take() {
			task.abort();
		}

		// Get the area, bailing if we haven't been set.
		let Some(area) = *self.rect.lock().unwrap() else {
			return Ok(());
		};
		let mut num_lines = area.height;
		let task = tokio::spawn({
			let log = self.clone();
			async move {
				// First figure out how many lines to walk back up.
				let arg = tg::process::log::get::Arg {
					position: Some(SeekFrom::End(0)),
					length: Some(-4096),
					..tg::process::log::get::Arg::default()
				};
				let stream = log
					.handle
					.get_process_log(log.process.id(), arg)
					.await
					.map_err(
						|source| tg::error!(!source, process = %log.process.id(), "failed to get the process log"),
					)?;
				let mut stream = std::pin::pin!(stream);
				let mut chunks = Vec::new();
				while let Some(chunk) = stream
					.try_next()
					.await
					.map_err(|source| tg::error!(!source, "failed to get the next chunk"))?
				{
					// Hold onto the chunk for now.
					chunks.push(chunk);
					let chunk = chunks.last().unwrap();

					// Count newlines walking backward through the chunk.
					for n in (0..chunk.bytes.len()).rev() {
						if chunk.bytes[n] == b'\n' {
							if num_lines == 0 {
								break;
							}
							num_lines -= 1;
						}
					}
				}

				// Get the max position of those chunks.
				let max_position = chunks
					.first()
					.map(|chunk| chunk.position + chunk.bytes.len().to_u64().unwrap() + 1)
					.unwrap_or_default();

				// Update chunks.
				let mut chunks_ = log.chunks.lock().await;
				chunks_.clear();
				while let Some(chunk) = chunks.pop() {
					chunks_.push(chunk);
				}
				log.max_position.store(max_position, Ordering::SeqCst);
				drop(chunks_);

				// Notify.
				log.notify.notify_waiters();

				let arg = tg::process::log::get::Arg {
					position: Some(SeekFrom::Start(max_position)),
					..tg::process::log::get::Arg::default()
				};
				let stream = log
					.handle
					.get_process_log(log.process.id(), arg)
					.await
					.map_err(
						|source| tg::error!(!source, process = %log.process.id(), "failed to get the process log"),
					)?;
				let mut stream = std::pin::pin!(stream);
				while let Some(chunk) = stream
					.try_next()
					.await
					.map_err(|source| tg::error!(!source, "failed to get the next chunk"))?
				{
					let max_position = chunk.position + chunk.bytes.len().to_u64().unwrap();

					// Add the chunk.
					let mut chunks = log.chunks.lock().await;
					chunks.push(chunk);

					// Update the max position.
					log.max_position.fetch_max(max_position, Ordering::AcqRel);
					drop(chunks);

					// Notify.
					log.notify.notify_waiters();
				}
				Ok::<_, tg::Error>(())
			}
		});

		self.task.lock().unwrap().replace(task);
		Ok(())
	}

	fn try_update_chunks(
		self: &Arc<Self>,
		error: scroll::Error,
	) -> impl Future<Output = tg::Result<()>> {
		match error {
			scroll::Error::Append => self.append().left_future(),
			scroll::Error::Prepend => self.prepend().right_future(),
		}
	}

	async fn append(self: &Arc<Self>) -> tg::Result<()> {
		// If we're appending and the log task already exists, just wait for more data to be available.
		if self.task.lock().unwrap().is_some() {
			self.notify.notified().await;
			return Ok(());
		}

		// Get the last chunk position.
		let position = self
			.chunks
			.lock()
			.await
			.last()
			.ok_or_else(|| tg::error!("expected a chunk"))
			.map(|chunk| chunk.position + chunk.bytes.len().to_u64().unwrap())?;

		// Create the stream.
		let arg = tg::process::log::get::Arg {
			position: Some(SeekFrom::Start(position)),
			..tg::process::log::get::Arg::default()
		};
		let stream = self
			.handle
			.get_process_log(self.process.id(), arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the process log stream"))?;
		let mut stream = std::pin::pin!(stream);
		let chunk = stream
			.try_next()
			.await
			.map_err(|source| tg::error!(!source, "failed to get the log chunk"))?
			.ok_or_else(|| tg::error!("expected a chunk"))?;
		let mut chunks = self.chunks.lock().await;
		let max_position = chunk.position + chunk.bytes.len().to_u64().unwrap();
		self.max_position.fetch_max(max_position, Ordering::AcqRel);
		chunks.push(chunk);
		Ok(())
	}

	async fn prepend(self: &Arc<Self>) -> tg::Result<()> {
		// Get the last chunk position.
		let Some(position) = self
			.chunks
			.lock()
			.await
			.first()
			.map(|chunk| chunk.position + chunk.bytes.len().to_u64().unwrap())
		else {
			return Ok(());
		};
		if position == 0 {
			return Err(tg::error!("start of stream"));
		}

		// Create the stream.
		let arg = tg::process::log::get::Arg {
			position: Some(SeekFrom::Start(position)),
			length: Some(-1),
			..tg::process::log::get::Arg::default()
		};
		let stream = self
			.handle
			.get_process_log(self.process.id(), arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the process log stream"))?;
		let mut stream = std::pin::pin!(stream);
		let chunk = stream
			.try_next()
			.await
			.map_err(|source| tg::error!(!source, "failed to get the log chunk"))?
			.ok_or_else(|| tg::error!("expected a chunk"))?;
		let mut chunks = self.chunks.lock().await;
		chunks.insert(0, chunk);
		Ok(())
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
