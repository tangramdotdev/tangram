use {
	futures::{SinkExt as _, StreamExt as _},
	std::{
		collections::BTreeMap,
		sync::{
			Arc,
			atomic::{AtomicUsize, Ordering},
		},
	},
	tangram_client::prelude::*,
};

#[derive(Clone)]
pub struct Stdio {
	inner: Arc<Inner>,
}

struct Inner {
	handle: tg::handle::dynamic::Handle,
	main_runtime_handle: tokio::runtime::Handle,
	next_process_stdio_token: AtomicUsize,
	process_stdio_readers: tokio::sync::Mutex<
		BTreeMap<
			usize,
			futures::stream::BoxStream<'static, tg::Result<tg::process::stdio::read::Event>>,
		>,
	>,
	process_stdio_writers: tokio::sync::Mutex<BTreeMap<usize, ProcessStdioWriter>>,
}

struct ProcessStdioWriter {
	sender: futures::channel::mpsc::Sender<tg::Result<tg::process::stdio::read::Event>>,
	task: tokio::task::JoinHandle<tg::Result<()>>,
}

impl Stdio {
	pub fn new(
		handle: tg::handle::dynamic::Handle,
		main_runtime_handle: tokio::runtime::Handle,
	) -> Self {
		let inner = Arc::new(Inner {
			handle,
			main_runtime_handle,
			next_process_stdio_token: AtomicUsize::new(0),
			process_stdio_readers: tokio::sync::Mutex::new(BTreeMap::new()),
			process_stdio_writers: tokio::sync::Mutex::new(BTreeMap::new()),
		});
		Self { inner }
	}

	pub async fn process_stdio_read_close(&self, token: usize) {
		self.inner.process_stdio_readers.lock().await.remove(&token);
	}

	pub async fn process_stdio_read_open(
		&self,
		id: tg::process::Id,
		arg: tg::process::stdio::read::Arg,
	) -> tg::Result<Option<usize>> {
		let handle = self.inner.handle.clone();
		let stream = self
			.inner
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
			.inner
			.next_process_stdio_token
			.fetch_add(1, Ordering::Relaxed)
			+ 1;
		self.inner
			.process_stdio_readers
			.lock()
			.await
			.insert(token, stream);
		Ok(Some(token))
	}

	pub async fn process_stdio_read_read(
		&self,
		token: usize,
	) -> tg::Result<Option<tg::process::stdio::read::Event>> {
		let reader = self.inner.process_stdio_readers.lock().await.remove(&token);
		let Some(mut reader) = reader else {
			return Ok(None);
		};
		let event = reader.next().await.transpose()?;
		if event
			.as_ref()
			.is_some_and(|event| !matches!(event, tg::process::stdio::read::Event::End))
		{
			self.inner
				.process_stdio_readers
				.lock()
				.await
				.insert(token, reader);
		}
		Ok(event)
	}

	pub async fn process_stdio_write_close(&self, token: usize) -> tg::Result<()> {
		let writer = self.inner.process_stdio_writers.lock().await.remove(&token);
		let Some(mut writer) = writer else {
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
		let handle = self.inner.handle.clone();
		let task = self.inner.main_runtime_handle.spawn(async move {
			let input = receiver.boxed();
			handle.write_process_stdio_all(&id, arg, input).await
		});
		let token = self
			.inner
			.next_process_stdio_token
			.fetch_add(1, Ordering::Relaxed)
			+ 1;
		self.inner
			.process_stdio_writers
			.lock()
			.await
			.insert(token, ProcessStdioWriter { sender, task });
		Ok(token)
	}

	pub async fn process_stdio_write_write(
		&self,
		token: usize,
		chunk: tg::process::stdio::Chunk,
	) -> tg::Result<()> {
		let mut sender = {
			let writers = self.inner.process_stdio_writers.lock().await;
			let writer = writers
				.get(&token)
				.ok_or_else(|| tg::error!(%token, "failed to find the process stdio writer"))?;
			writer.sender.clone()
		};
		let event = tg::process::stdio::read::Event::Chunk(chunk);
		sender
			.send(Ok(event))
			.await
			.map_err(|source| tg::error!(!source, %token, "failed to send the process stdio event"))
	}
}
