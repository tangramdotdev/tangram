use {
	dashmap::DashMap,
	futures::{SinkExt as _, StreamExt as _},
	std::sync::{
		Arc,
		atomic::{AtomicUsize, Ordering},
	},
	sync_wrapper::SyncWrapper,
	tangram_client::prelude::*,
};

#[derive(Clone)]
pub struct Stdio(Arc<State>);

struct State {
	handle: tg::handle::dynamic::Handle,
	main_runtime_handle: tokio::runtime::Handle,
	next_process_stdio_token: AtomicUsize,
	process_stdio_readers: ProcessStdioReaders,
	process_stdio_writers: ProcessStdioWriters,
}

type ProcessStdioReader =
	SyncWrapper<futures::stream::BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>>;

type ProcessStdioReaders = DashMap<usize, ProcessStdioReader, fnv::FnvBuildHasher>;

type ProcessStdioWriters = DashMap<usize, ProcessStdioWriter, fnv::FnvBuildHasher>;

struct ProcessStdioWriter {
	sender: futures::channel::mpsc::Sender<tg::Result<tg::process::stdio::read::Event>>,
	task: tokio::task::JoinHandle<tg::Result<()>>,
}

impl Stdio {
	pub fn new(
		handle: tg::handle::dynamic::Handle,
		main_runtime_handle: tokio::runtime::Handle,
	) -> Self {
		let state = Arc::new(State {
			handle,
			main_runtime_handle,
			next_process_stdio_token: AtomicUsize::new(0),
			process_stdio_readers: ProcessStdioReaders::default(),
			process_stdio_writers: ProcessStdioWriters::default(),
		});
		Self(state)
	}

	pub async fn process_stdio_read_close(&self, token: usize) {
		self.0.process_stdio_readers.remove(&token);
	}

	pub async fn process_stdio_read_open(
		&self,
		id: tg::process::Id,
		arg: tg::process::stdio::read::Arg,
	) -> tg::Result<Option<usize>> {
		let handle = self.0.handle.clone();
		let stream = self
			.0
			.main_runtime_handle
			.spawn(async move {
				let stream = handle
					.try_read_process_stdio_all(&id, arg)
					.await?
					.map(futures::StreamExt::boxed);
				Ok::<_, tg::Error>(stream)
			})
			.await
			.map_err(|source| tg::error!(!source, "the task panicked"))??;
		let Some(stream) = stream else {
			return Ok(None);
		};
		let token = self
			.0
			.next_process_stdio_token
			.fetch_add(1, Ordering::Relaxed)
			+ 1;
		self.0
			.process_stdio_readers
			.insert(token, SyncWrapper::new(stream));
		Ok(Some(token))
	}

	pub async fn process_stdio_read_read(
		&self,
		token: usize,
	) -> tg::Result<Option<tg::process::stdio::read::Event>> {
		let reader = self.0.process_stdio_readers.remove(&token);
		let Some((_, mut reader)) = reader else {
			return Ok(None);
		};
		let event = reader.get_mut().next().await.transpose()?;
		if event
			.as_ref()
			.is_some_and(|event| !matches!(event, tg::process::stdio::read::Event::End))
		{
			self.0.process_stdio_readers.insert(token, reader);
		}
		Ok(event)
	}

	pub async fn process_stdio_write_close(&self, token: usize) -> tg::Result<()> {
		let writer = self.0.process_stdio_writers.remove(&token);
		let Some((_, mut writer)) = writer else {
			return Ok(());
		};
		let _ = writer
			.sender
			.send(Ok(tg::process::stdio::read::Event::End))
			.await;
		drop(writer.sender);
		writer
			.task
			.await
			.map_err(|source| tg::error!(!source, "the task panicked"))?
	}

	pub async fn process_stdio_write_open(
		&self,
		id: tg::process::Id,
		arg: tg::process::stdio::write::Arg,
	) -> tg::Result<usize> {
		let (sender, receiver) = futures::channel::mpsc::channel(16);
		let handle = self.0.handle.clone();
		let task = self.0.main_runtime_handle.spawn(async move {
			let input = receiver.boxed();
			handle.write_process_stdio_all(&id, arg, input).await
		});
		let token = self
			.0
			.next_process_stdio_token
			.fetch_add(1, Ordering::Relaxed)
			+ 1;
		self.0
			.process_stdio_writers
			.insert(token, ProcessStdioWriter { sender, task });
		Ok(token)
	}

	pub async fn process_stdio_write_write(
		&self,
		token: usize,
		chunk: tg::process::stdio::Chunk,
	) -> tg::Result<()> {
		let mut sender = self
			.0
			.process_stdio_writers
			.get(&token)
			.map(|writer| writer.sender.clone())
			.ok_or_else(|| tg::error!(%token, "failed to find the process stdio writer"))?;
		let event = tg::process::stdio::read::Event::Chunk(chunk);
		sender
			.send(Ok(event))
			.await
			.map_err(|source| tg::error!(!source, %token, "failed to send the process stdio event"))
	}
}
