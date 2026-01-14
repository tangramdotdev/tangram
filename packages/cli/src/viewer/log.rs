use {
	futures::{StreamExt as _, TryStreamExt as _, stream::BoxStream},
	num::ToPrimitive as _,
	ratatui::{self as tui, prelude::*},
	scroll::Scroll,
	std::{pin::pin, sync::Arc},
	tangram_client::prelude::*,
	tokio::{sync::mpsc, task::JoinHandle},
};

const PAGE_SIZE: u64 = 4096;

mod scroll;

pub struct Log<H> {
	state: Arc<State<H>>,
	task: JoinHandle<()>,
}

impl<H> Drop for Log<H> {
	fn drop(&mut self) {
		self.task.abort();
	}
}

struct State<H> {
	handle: H,
	sender: mpsc::UnboundedSender<Event>,
	process: tg::Process,
	stream: tokio::sync::Mutex<StreamState>,
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
	chunks: Vec<tg::process::log::get::Chunk>,
	eof: bool,
	forwards: bool,
	scroll: Option<Scroll>,
	stream: Option<BoxStream<'static, tg::Result<tg::process::log::get::Chunk>>>,
}

struct Tailing {
	chunks: Vec<tg::process::log::get::Chunk>,
	task: JoinHandle<()>,
}

impl Drop for Tailing {
	fn drop(&mut self) {
		self.task.abort();
	}
}

#[derive(Debug)]
enum Event {
	Page(scroll::Error),
	Scroll(isize),
	Update,
}

impl<H> Log<H>
where
	H: tg::Handle,
{
	pub fn new(handle: &H, process: &tg::Process) -> Self {
		let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
		let stream = StreamState::Scrolling(Scrolling::default());
		let view = ViewState::default();
		let state = Arc::new(State {
			handle: handle.clone(),
			sender,
			process: process.clone(),
			stream: tokio::sync::Mutex::new(stream),
			view: std::sync::Mutex::new(view),
		});

		let task = tokio::spawn({
			let state = Arc::downgrade(&state);
			async move {
				let state_ = state.upgrade();
				if let Some(state_) = &state_ {
					state_.tail().await;
				}
				drop(state_);
				while let Some(event) = receiver.recv().await {
					let Some(state) = state.upgrade() else {
						break;
					};
					let result = match event {
						Event::Scroll(height) => {
							state.scroll(height).await;
							Ok(())
						},
						Event::Update => {
							state.update_view().await;
							Ok(())
						},
						Event::Page(scroll::Error::Append) => state.append().await,
						Event::Page(scroll::Error::Prepend) => state.prepend().await,
					};
					if let Err(error) = result {
						tracing::error!(?error, "failed to handle event");
					}
				}
			}
		});

		Self { state, task }
	}

	pub fn down(&self) {
		self.state.sender.send(Event::Scroll(1)).ok();
	}

	pub fn up(&self) {
		self.state.sender.send(Event::Scroll(-1)).ok();
	}

	pub fn hit_test(&self, x: u16, y: u16) -> bool {
		let Some(rect) = self.state.view.lock().unwrap().area else {
			return false;
		};
		let position = Position { x, y };
		rect.contains(position)
	}

	pub fn render(&self, area: Rect, buf: &mut tui::buffer::Buffer) {
		let mut view = self.state.view.lock().unwrap();
		view.area.replace(area);
		let lines = view
			.lines
			.as_ref()
			.into_iter()
			.flat_map(|lines| lines.content.iter());
		for (y, line) in (0..area.height).zip(lines) {
			buf.set_line(area.x, area.y + y, &tui::text::Line::raw(line), area.width);
		}
	}
}

impl<H> State<H>
where
	H: tg::Handle,
{
	async fn scroll(self: &Arc<Self>, height: isize) {
		// Do nothing if asked to scroll by 0.
		if height == 0 {
			return;
		}

		// Do nothing if we don't have a height het.
		let Some(rect) = self.view.lock().unwrap().area else {
			return;
		};

		// Get the stream.
		let mut stream = self.stream.lock().await;

		// Cancel the tailing task if it exists and height < 0.
		if let Ok(tailing) = stream.try_unwrap_tailing_mut() {
			// Do nothing if asked to scroll down and we're already tailing.
			if height > 0 {
				return;
			}

			// Check the EOF condition.
			let eof = tailing
				.chunks
				.first()
				.is_some_and(|chunk| chunk.position == 0);

			// Update the state.
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

		// Handle EOF condition:
		// - If the chunks stream is going forwards (down) and height is < 0 (up), clear eof.
		// - If the chunks stream is going backwards (up) and height is > 0 (down), clear eof.
		scrolling.eof &= scrolling.forwards == (height < 0);

		// Note the intended direction of the chunks stream.
		scrolling.forwards = height >= 0;

		// Try to create new scroll state if not exists. If an error occurs send the page event and retry the scroll event.
		if scrolling.scroll.is_none() {
			match Scroll::new(rect, &scrolling.chunks) {
				Ok(scroll) => {
					scrolling.scroll.replace(scroll);
				},
				Err(error) => {
					self.sender.send(Event::Page(error)).ok();
					self.sender.send(Event::Scroll(height)).ok();
					return;
				},
			}
		}

		// Attempt to scroll.
		match scrolling
			.scroll
			.as_mut()
			.unwrap()
			.scroll(height, &scrolling.chunks)
		{
			// If we scrolled down, but we can't scroll anymore, begin tailing.
			Ok(0) if scrolling.forwards && height > 0 && !scrolling.eof => {
				drop(stream);
				self.tail().await;
			},

			// If successful send the update event.
			Ok(_) => {
				self.sender.send(Event::Update).ok();
			},

			// Otherwise send the error event.
			Err(error) => {
				self.sender.send(Event::Page(error)).ok();
				self.sender.send(Event::Scroll(height)).ok();
			},
		}
	}

	async fn append(&self) -> tg::Result<()> {
		let mut stream = self.stream.lock().await;
		let scrolling = stream
			.try_unwrap_scrolling_mut()
			.map_err(|_| tg::error!("cannot append while tailing"))?;
		if scrolling.eof {
			return Ok(());
		}

		// Rebuild the stream if necessary.
		if !scrolling.forwards || scrolling.stream.is_none() {
			let position = scrolling.chunks.last().map(|chunk| {
				std::io::SeekFrom::Start(chunk.position + chunk.bytes.len().to_u64().unwrap())
			});
			let arg = tg::process::log::get::Arg {
				position,
				size: Some(PAGE_SIZE),
				..tg::process::log::get::Arg::default()
			};
			let stream = self
				.process
				.log(&self.handle, arg)
				.await
				.map_err(|source| tg::error!(!source, "failed to create the log stream"))?
				.boxed();
			scrolling.stream.replace(stream);
		}

		// Try to read a chunk.
		let chunk = scrolling
			.stream
			.as_mut()
			.unwrap()
			.try_next()
			.await
			.map_err(|source| tg::error!(!source, "failed to page down"))?;

		// Update EOF.
		if let Some(chunk) = chunk {
			scrolling.eof = chunk.bytes.is_empty();
			scrolling.chunks.push(chunk);
		} else {
			scrolling.stream.take();
		}
		Ok(())
	}

	async fn prepend(&self) -> tg::Result<()> {
		let mut stream = self.stream.lock().await;
		let scrolling = stream
			.try_unwrap_scrolling_mut()
			.map_err(|_| tg::error!("cannot page while tailing"))?;
		if scrolling.eof {
			return Ok(());
		}

		// Rebuild the stream if necessary.
		if scrolling.forwards || scrolling.stream.is_none() {
			let position = scrolling
				.chunks
				.first()
				.map(|chunk| std::io::SeekFrom::Start(chunk.position));
			let arg = tg::process::log::get::Arg {
				position,
				length: Some(-(PAGE_SIZE.to_i64().unwrap())),
				..tg::process::log::get::Arg::default()
			};
			let stream = self
				.process
				.log(&self.handle, arg)
				.await
				.map_err(|source| tg::error!(!source, "failed to create the log stream"))?
				.boxed();
			scrolling.stream.replace(stream);
		}

		// Attempt to read a chunk.
		let chunk = scrolling
			.stream
			.as_mut()
			.unwrap()
			.try_next()
			.await
			.map_err(|source| tg::error!(!source, "failed to page down"))?;
		if let Some(chunk) = chunk {
			scrolling.eof = chunk.position == 0;
			scrolling.chunks.insert(0, chunk);
		} else {
			scrolling.stream.take();
		}
		Ok(())
	}

	async fn update_view(&self) {
		let Some(area) = self.view.lock().unwrap().area else {
			return;
		};
		let mut stream = self.stream.lock().await;
		match &mut *stream {
			StreamState::Scrolling(scrolling) => {
				let Some(scroll) = scrolling.scroll.as_mut() else {
					return;
				};
				let lines = match scroll.read_lines(&scrolling.chunks) {
					Ok(lines) => lines,
					Err(error) => {
						self.sender.send(Event::Page(error)).ok();
						return;
					},
				};
				scrolling.chunks.retain(|chunk| {
					chunk.position < lines.end
						&& (chunk.position + chunk.bytes.len().to_u64().unwrap()) > lines.start
				});
				self.view.lock().unwrap().lines.replace(lines);
			},
			StreamState::Tailing(tailing) => {
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
			},
		}
	}

	async fn tail(self: &Arc<Self>) {
		let mut stream = self.stream.lock().await;
		let position = match &*stream {
			StreamState::Scrolling(scrolling) => scrolling.chunks.last().map_or(0, |chunk| {
				chunk.position + chunk.bytes.len().to_u64().unwrap()
			}),
			StreamState::Tailing(tailing) => tailing.chunks.last().map_or(0, |chunk| {
				chunk.position + chunk.bytes.len().to_u64().unwrap()
			}),
		};
		let state = Arc::downgrade(self);
		let task = tokio::spawn(async move {
			let arg = tg::process::log::get::Arg {
				position: Some(std::io::SeekFrom::Start(position)),
				..tg::process::log::get::Arg::default()
			};
			let Some(state_) = state.upgrade() else {
				return;
			};
			let Some(stream) = state_
				.process
				.log(&state_.handle, arg)
				.await
				.inspect_err(|error| tracing::error!(?error, "failed to tail the log"))
				.ok()
			else {
				return;
			};
			drop(state_);
			let mut stream = pin!(stream);
			while let Ok(Some(chunk)) = stream.try_next().await {
				let Some(state) = state.upgrade() else {
					return;
				};
				let mut stream = state.stream.lock().await;
				let Ok(tailing) = stream.try_unwrap_tailing_mut() else {
					return;
				};
				if chunk.bytes.is_empty() {
					break;
				}
				tailing.chunks.push(chunk);
				state.sender.send(Event::Update).ok();
			}
			let Some(state) = state.upgrade() else {
				return;
			};
			let mut stream = state.stream.lock().await;
			let Some(chunks) = stream
				.try_unwrap_tailing_ref()
				.ok()
				.map(|t| t.chunks.clone())
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
			state.sender.send(Event::Update).ok();
		});
		let chunks = match &*stream {
			StreamState::Scrolling(scrolling) => scrolling.chunks.clone(),
			StreamState::Tailing(scrolling) => scrolling.chunks.clone(),
		};
		let tailing = Tailing { chunks, task };
		*stream = StreamState::Tailing(tailing);
	}
}
