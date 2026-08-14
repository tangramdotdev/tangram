use {
	bytes::Bytes,
	futures::{StreamExt as _, TryStreamExt as _, stream::BoxStream},
	num::ToPrimitive as _,
	ratatui::{self as tui, prelude::*},
	scroll::Scroll,
	std::{
		pin::pin,
		sync::{
			Arc,
			atomic::{AtomicBool, Ordering},
		},
	},
	tangram_client::prelude::*,
	tokio::{sync::mpsc, task::JoinHandle},
};

mod scroll;

const PAGE_SIZE: u64 = 4096;

#[derive(Clone, Debug)]
pub(super) struct Chunk {
	bytes: Bytes,
	position: u64,
	stream: tg::process::stdio::Stream,
}

pub struct Log {
	state: Arc<State>,
	task: JoinHandle<()>,
}

struct State {
	client: tg::Client,
	dirty: AtomicBool,
	process: tg::Process,
	sender: mpsc::UnboundedSender<Event>,
	stream: tokio::sync::Mutex<StreamState>,
	streams: Vec<tg::process::stdio::Stream>,
	update_queued: AtomicBool,
	view: std::sync::Mutex<ViewState>,
}

#[derive(Default)]
struct ViewState {
	area: Option<Rect>,
	lines: Option<scroll::Lines>,
}

#[derive(derive_more::TryUnwrap, derive_more::Unwrap)]
#[try_unwrap(ref, ref_mut)]
#[unwrap(ref, ref_mut)]
enum StreamState {
	Scrolling(Scrolling),
	Tailing(Tailing),
}

#[derive(Default)]
struct Scrolling {
	chunks: Vec<Chunk>,
	eof: bool,
	forwards: bool,
	scroll: Option<Scroll>,
	stream: Option<BoxStream<'static, tg::Result<Chunk>>>,
}

struct Tailing {
	chunks: Vec<Chunk>,
	task: JoinHandle<()>,
}

#[derive(Debug)]
enum Event {
	Error(String),
	Resize(Rect),
	Scroll(isize),
	Update,
}

enum AfterPage {
	Resize(Rect),
	Scroll(isize),
	Update,
}

impl Log {
	#[must_use]
	pub fn new(
		client: &tg::Client,
		process: &tg::Process,
		streams: Vec<tg::process::stdio::Stream>,
	) -> Self {
		// Create the state.
		let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
		let stream = StreamState::Scrolling(Scrolling::default());
		let view = ViewState::default();
		let state = Arc::new(State {
			client: client.clone(),
			dirty: AtomicBool::new(true),
			process: process.clone(),
			sender,
			stream: tokio::sync::Mutex::new(stream),
			streams,
			update_queued: AtomicBool::new(false),
			view: std::sync::Mutex::new(view),
		});

		// Spawn the event task.
		let task = tokio::spawn({
			let state = Arc::downgrade(&state);
			async move {
				if let Some(state) = state.upgrade() {
					state.tail().await;
				}
				while let Some(event) = receiver.recv().await {
					let Some(state) = state.upgrade() else {
						break;
					};
					state.handle_event(event).await;
				}
			}
		});

		Self { state, task }
	}

	pub fn render(&self, area: Rect, buffer: &mut tui::buffer::Buffer) {
		let mut view = self.state.view.lock().unwrap();
		if view.area != Some(area) {
			self.state.sender.send(Event::Resize(area)).ok();
		}
		view.area.replace(area);
		let lines = view
			.lines
			.as_ref()
			.into_iter()
			.flat_map(|lines| lines.content.iter());
		for (row, line) in (0..area.height).zip(lines) {
			buffer.set_line(
				area.x,
				area.y + row,
				&tui::text::Line::raw(line),
				area.width,
			);
		}
	}

	pub fn down(&self) {
		self.state.sender.send(Event::Scroll(1)).ok();
	}

	pub fn up(&self) {
		self.state.sender.send(Event::Scroll(-1)).ok();
	}

	#[must_use]
	pub fn hit_test(&self, x: u16, y: u16) -> bool {
		let Some(rect) = self.state.view.lock().unwrap().area else {
			return false;
		};
		let position = Position { x, y };
		rect.contains(position)
	}

	pub fn take_dirty(&self) -> bool {
		self.state.dirty.swap(false, Ordering::AcqRel)
	}
}

impl State {
	async fn handle_event(self: &Arc<Self>, event: Event) {
		match event {
			Event::Error(error) => {
				self.set_error(error);
			},
			Event::Resize(area) => {
				self.resize(area).await;
			},
			Event::Scroll(height) => {
				self.scroll(height).await;
			},
			Event::Update => {
				self.update_queued.store(false, Ordering::Release);
				self.update_view().await;
			},
		}
	}

	fn set_error(&self, error: String) {
		let lines = scroll::Lines {
			content: vec![error],
			end: 0,
			start: 0,
		};
		self.view.lock().unwrap().lines.replace(lines);
		self.dirty.store(true, Ordering::Release);
	}

	async fn resize(&self, area: Rect) {
		let mut stream = self.stream.lock().await;
		let Ok(scrolling) = stream.try_unwrap_scrolling_mut() else {
			return;
		};
		if scrolling.chunks.is_empty() {
			return;
		}
		match Scroll::new(area, &scrolling.chunks) {
			Err(error) => {
				drop(stream);
				self.page(error, AfterPage::Resize(area)).await;
			},
			Ok(scroll) => {
				scrolling.scroll.replace(scroll);
				self.queue_update();
			},
		}
	}

	async fn scroll(self: &Arc<Self>, height: isize) {
		// Ignore zero-distance scrolling.
		if height == 0 {
			return;
		}

		// Ignore scrolling before layout.
		let Some(rect) = self.view.lock().unwrap().area else {
			return;
		};

		// Lock the stream state.
		let mut stream = self.stream.lock().await;

		// Stop tailing when scrolling up.
		if let Ok(tailing) = stream.try_unwrap_tailing_mut() {
			// Ignore downward scrolling while tailing.
			if height > 0 {
				return;
			}

			// Determine whether the buffered log starts at byte zero.
			let eof = tailing
				.chunks
				.first()
				.is_some_and(|chunk| chunk.position == 0);

			// Switch to scrolling.
			let scrolling = Scrolling {
				chunks: tailing.chunks.clone(),
				eof,
				forwards: false,
				scroll: None,
				stream: None,
			};
			*stream = StreamState::Scrolling(scrolling);
		}

		let scrolling = stream.unwrap_scrolling_mut();

		// Preserve EOF only while scrolling in the same direction.
		scrolling.eof = eof_for_direction(scrolling.eof, scrolling.forwards, height);

		// Record the intended direction.
		scrolling.forwards = height >= 0;

		// Page an empty buffer.
		if scrolling.chunks.is_empty() {
			let error = if scrolling.forwards {
				scroll::Error::Append
			} else {
				scroll::Error::Prepend
			};
			drop(stream);
			self.page(error, AfterPage::Scroll(height)).await;
			return;
		}

		// Create the scroll state, paging when the viewport exceeds the buffered chunks.
		if scrolling.scroll.is_none() {
			match Scroll::new(rect, &scrolling.chunks) {
				Ok(scroll) => {
					scrolling.scroll.replace(scroll);
				},
				Err(error) => {
					drop(stream);
					self.page(error, AfterPage::Scroll(height)).await;
					return;
				},
			}
		}

		// Scroll the buffered content.
		match scrolling
			.scroll
			.as_mut()
			.unwrap()
			.scroll(height, &scrolling.chunks)
		{
			// Begin tailing at the end of the buffered log.
			Ok(0) if scrolling.forwards && height > 0 && !scrolling.eof => {
				drop(stream);
				self.tail().await;
			},

			// Queue a successful update.
			Ok(_) => {
				self.queue_update();
			},

			// Load another page.
			Err(error) => {
				drop(stream);
				self.page(error, AfterPage::Scroll(height)).await;
			},
		}
	}

	async fn update_view(&self) {
		let Some(area) = self.view.lock().unwrap().area else {
			return;
		};
		let mut stream = self.stream.lock().await;
		match &mut *stream {
			StreamState::Scrolling(scrolling) => {
				if scrolling.scroll.is_none() {
					if scrolling.chunks.is_empty() {
						return;
					}
					match Scroll::new(area, &scrolling.chunks) {
						Err(error) => {
							drop(stream);
							self.page(error, AfterPage::Update).await;
							return;
						},
						Ok(scroll) => {
							scrolling.scroll.replace(scroll);
						},
					}
				}
				let scroll = scrolling.scroll.as_mut().unwrap();
				let lines = match scroll.read_lines(&scrolling.chunks) {
					Err(error) => {
						drop(stream);
						self.page(error, AfterPage::Update).await;
						return;
					},
					Ok(lines) => lines,
				};
				scrolling.chunks.retain(|chunk| {
					chunk.position < lines.end
						&& (chunk.position + chunk.bytes.len().to_u64().unwrap()) > lines.start
				});
				self.view.lock().unwrap().lines.replace(lines);
				self.dirty.store(true, Ordering::Release);
			},
			StreamState::Tailing(tailing) => {
				if tailing.chunks.is_empty() {
					return;
				}
				let Ok(mut scroll) = Scroll::new(area, &tailing.chunks) else {
					return;
				};
				let Ok(lines) = scroll.read_lines(&tailing.chunks) else {
					return;
				};
				tailing.chunks.retain(|chunk| {
					chunk.position < lines.end
						&& (chunk.position + chunk.bytes.len().to_u64().unwrap()) > lines.start
				});
				self.view.lock().unwrap().lines.replace(lines);
				self.dirty.store(true, Ordering::Release);
			},
		}
	}

	async fn page(&self, error: scroll::Error, after: AfterPage) {
		let result = match error {
			scroll::Error::Append => self.append().await,
			scroll::Error::Prepend => self.prepend().await,
		};
		match result {
			Err(error) => {
				tracing::error!(?error, "failed to page the log");
				self.queue_error(&error);
			},
			Ok(false) => (),
			Ok(true) => match after {
				AfterPage::Resize(area) => {
					self.sender.send(Event::Resize(area)).ok();
				},
				AfterPage::Scroll(height) => {
					self.sender.send(Event::Scroll(height)).ok();
				},
				AfterPage::Update => self.queue_update(),
			},
		}
	}

	async fn append(&self) -> tg::Result<bool> {
		let mut stream = self.stream.lock().await;
		let scrolling = stream
			.try_unwrap_scrolling_mut()
			.map_err(|_| tg::error!("cannot append while tailing"))?;
		if scrolling.eof {
			return Ok(false);
		}

		// Create a forward stream.
		if !scrolling.forwards || scrolling.stream.is_none() {
			let position = scrolling.chunks.last().map(|chunk| {
				std::io::SeekFrom::Start(chunk.position + chunk.bytes.len().to_u64().unwrap())
			});
			let stream = self
				.read_log(position, None, Some(PAGE_SIZE))
				.await
				.map_err(|error| tg::error!(!error, "failed to create the log stream"))?
				.boxed();
			scrolling.stream.replace(stream);
		}

		// Read a chunk.
		let chunk = scrolling
			.stream
			.as_mut()
			.unwrap()
			.try_next()
			.await
			.map_err(|error| tg::error!(!error, "failed to page down"))?;

		// Update the buffered chunks and EOF state.
		let appended = if let Some(chunk) = chunk
			&& !chunk.bytes.is_empty()
		{
			debug_assert!(matches!(
				chunk.stream,
				tg::process::stdio::Stream::Stderr | tg::process::stdio::Stream::Stdout
			));
			scrolling.chunks.push(chunk);
			true
		} else {
			scrolling.eof = true;
			scrolling.stream.take();
			false
		};

		Ok(appended)
	}

	async fn prepend(&self) -> tg::Result<bool> {
		let mut stream = self.stream.lock().await;
		let scrolling = stream
			.try_unwrap_scrolling_mut()
			.map_err(|_| tg::error!("cannot page while tailing"))?;
		if scrolling.eof {
			return Ok(false);
		}

		// Create a reverse stream.
		if scrolling.forwards || scrolling.stream.is_none() {
			let position = scrolling
				.chunks
				.first()
				.map(|chunk| std::io::SeekFrom::Start(chunk.position));
			let stream = self
				.read_log(position, Some(-(PAGE_SIZE.to_i64().unwrap())), None)
				.await
				.map_err(|error| tg::error!(!error, "failed to create the log stream"))?
				.boxed();
			scrolling.stream.replace(stream);
		}

		// Read a chunk.
		let chunk = scrolling
			.stream
			.as_mut()
			.unwrap()
			.try_next()
			.await
			.map_err(|error| tg::error!(!error, "failed to page up"))?;
		let prepended = if let Some(chunk) = chunk
			&& !chunk.bytes.is_empty()
		{
			debug_assert!(matches!(
				chunk.stream,
				tg::process::stdio::Stream::Stderr | tg::process::stdio::Stream::Stdout
			));
			scrolling.eof = chunk.position == 0;
			scrolling.chunks.insert(0, chunk);
			true
		} else {
			scrolling.eof = true;
			scrolling.stream.take();
			false
		};

		Ok(prepended)
	}

	async fn tail(self: &Arc<Self>) {
		let mut stream = self.stream.lock().await;
		let chunks = match &mut *stream {
			StreamState::Scrolling(scrolling)
				if scrolling.stream.is_some() && scrolling.forwards =>
			{
				scrolling.stream.take().unwrap()
			},
			StreamState::Scrolling(scrolling) => {
				let position = scrolling.chunks.last().map_or(0, |chunk| {
					chunk.position + chunk.bytes.len().to_u64().unwrap()
				});
				let stream = match self
					.read_log(Some(std::io::SeekFrom::Start(position)), None, None)
					.await
				{
					Ok(stream) => stream,
					Err(error) => {
						tracing::error!(?error, "failed to get the log");
						self.queue_error(&error);
						return;
					},
				};
				stream.boxed()
			},
			StreamState::Tailing(_) => return,
		};
		let state = Arc::downgrade(self);
		let task = tokio::spawn(Self::tail_task(state, chunks));
		let chunks = match &*stream {
			StreamState::Scrolling(scrolling) => scrolling.chunks.clone(),
			StreamState::Tailing(scrolling) => scrolling.chunks.clone(),
		};
		let tailing = Tailing { chunks, task };
		*stream = StreamState::Tailing(tailing);
	}

	async fn tail_task(
		state: std::sync::Weak<Self>,
		chunks: BoxStream<'static, tg::Result<Chunk>>,
	) {
		let mut tail_error = None;
		let mut stream = pin!(chunks);
		loop {
			let chunk = match stream.try_next().await {
				Err(error) => {
					tracing::error!(?error, "failed to tail the log");
					tail_error.replace(error);
					break;
				},
				Ok(None) => break,
				Ok(Some(chunk)) => chunk,
			};
			let Some(state) = state.upgrade() else {
				return;
			};
			debug_assert!(matches!(
				chunk.stream,
				tg::process::stdio::Stream::Stderr | tg::process::stdio::Stream::Stdout
			));
			let mut stream = state.stream.lock().await;
			let Ok(tailing) = stream.try_unwrap_tailing_mut() else {
				return;
			};
			if chunk.bytes.is_empty() {
				break;
			}
			tailing.chunks.push(chunk);
			state.queue_update();
		}

		let Some(state) = state.upgrade() else {
			return;
		};
		let mut stream = state.stream.lock().await;
		let Some(chunks) = stream
			.try_unwrap_tailing_ref()
			.ok()
			.map(|tailing| tailing.chunks.clone())
		else {
			return;
		};
		let scrolling = StreamState::Scrolling(Scrolling {
			chunks,
			eof: true,
			forwards: true,
			scroll: None,
			stream: None,
		});
		*stream = scrolling;
		if let Some(error) = tail_error {
			state.queue_error(&error);
		} else {
			state.queue_update();
		}
	}

	async fn read_log(
		&self,
		position: Option<std::io::SeekFrom>,
		length: Option<i64>,
		size: Option<u64>,
	) -> tg::Result<BoxStream<'static, tg::Result<Chunk>>> {
		// Create the request.
		let options = tg::process::stdio::read::Options {
			length,
			position,
			size,
			streams: self.streams.clone(),
			..Default::default()
		};

		// Read the log.
		let stream = self
			.process
			.try_read_stdio_with_handle(&self.client, options)
			.await?
			.ok_or_else(|| tg::error!("failed to get the log stream"))?;

		// Convert the events.
		let stream = stream
			.try_filter_map(|event| async move {
				match event {
					tg::process::stdio::read::Event::Chunk(chunk) => {
						let position = chunk
							.position
							.ok_or_else(|| tg::error!("expected the chunk position"))?;
						let stream = stdio_stream_to_log_stream(chunk.stream)?;
						let chunk = Chunk {
							bytes: chunk.bytes,
							position,
							stream,
						};
						Ok(Some(chunk))
					},
					tg::process::stdio::read::Event::End => Ok(None),
				}
			})
			.boxed();

		Ok(stream)
	}

	fn queue_error(&self, error: &tg::Error) {
		self.sender.send(Event::Error(error.to_string())).ok();
	}

	fn queue_update(&self) {
		if !self.update_queued.swap(true, Ordering::AcqRel) {
			self.sender.send(Event::Update).ok();
		}
	}
}

impl Drop for Log {
	fn drop(&mut self) {
		self.task.abort();
	}
}

impl Drop for Tailing {
	fn drop(&mut self) {
		self.task.abort();
	}
}

fn eof_for_direction(eof: bool, forwards: bool, height: isize) -> bool {
	eof && forwards == (height >= 0)
}

fn stdio_stream_to_log_stream(
	stream: tg::process::stdio::Stream,
) -> tg::Result<tg::process::stdio::Stream> {
	match stream {
		tg::process::stdio::Stream::Stderr => Ok(tg::process::stdio::Stream::Stderr),
		tg::process::stdio::Stream::Stdin => Err(tg::error!("invalid stdio stream")),
		tg::process::stdio::Stream::Stdout => Ok(tg::process::stdio::Stream::Stdout),
	}
}

#[cfg(test)]
mod tests {
	use super::eof_for_direction;

	#[test]
	fn clears_eof_when_direction_changes() {
		assert!(!eof_for_direction(true, true, -1));
		assert!(!eof_for_direction(true, false, 1));
	}

	#[test]
	fn preserves_eof_in_the_same_direction() {
		assert!(eof_for_direction(true, true, 1));
		assert!(eof_for_direction(true, false, -1));
	}
}
