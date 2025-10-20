use {
	super::Runtime,
	tangram_client as tg,
	tangram_futures::{
		read::{Ext as _, shared_position_reader::SharedPositionReader},
		stream::Ext as _,
	},
	tokio::io::AsyncBufReadExt as _,
	tokio_util::task::AbortOnDropHandle,
};

impl Runtime {
	pub async fn decompress(&self, process: &tg::Process) -> tg::Result<crate::runtime::Output> {
		let server = &self.server;
		let command = process.command(server).await?;

		// Get the args.
		let args = command.args(server).await?;

		// Get the blob.
		let input = args
			.first()
			.ok_or_else(|| tg::error!("invalid number of arguments"))?;
		let blob = match input {
			tg::Value::Object(tg::Object::Blob(blob)) => blob.clone(),
			tg::Value::Object(tg::Object::File(file)) => file.contents(server).await?,
			_ => {
				return Err(tg::error!("expected a blob or a file"));
			},
		};

		// Create the reader.
		let reader = blob.read(server, tg::read::Options::default()).await?;
		let mut reader = SharedPositionReader::with_reader_and_position(reader, 0)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the shared position reader"))?;

		// Detect the compression format.
		let buffer = reader
			.fill_buf()
			.await
			.map_err(|source| tg::error!(!source, "failed to fill the buffer"))?;
		let format = super::util::detect_compression_format(buffer)?
			.ok_or_else(|| tg::error!("invalid compression format"))?;

		// Spawn a task to log progress.
		let position = reader.shared_position();
		let size = blob.length(server).await?;
		let (sender, receiver) =
			async_channel::bounded::<tg::Result<tg::progress::Event<()>>>(1024);
		let progress_task = AbortOnDropHandle::new(tokio::spawn({
			let position = position.clone();
			async move {
				loop {
					let current = position.load(std::sync::atomic::Ordering::Relaxed);
					let indicator = tg::progress::Indicator {
						current: Some(current),
						format: tg::progress::IndicatorFormat::Bytes,
						name: String::new(),
						title: "decompressing".to_owned(),
						total: Some(size),
					};
					let event = tg::progress::Event::Update::<()>(indicator);
					let result = sender.send(Ok(event)).await;
					if result.is_err() {
						break;
					}
					tokio::time::sleep(std::time::Duration::from_secs(1)).await;
				}
			}
		}));
		let stream = receiver.attach(progress_task);
		let log_task = tokio::spawn({
			let server = server.clone();
			let process = process.clone();
			async move { server.log_progress_stream(&process, stream).await.ok() }
		});
		let log_task_abort_handle = log_task.abort_handle();
		scopeguard::defer! {
			log_task_abort_handle.abort();
		};

		// Decompress the blob.
		let reader = match format {
			tg::CompressionFormat::Bz2 => {
				async_compression::tokio::bufread::BzDecoder::new(reader).boxed()
			},
			tg::CompressionFormat::Gz => {
				async_compression::tokio::bufread::GzipDecoder::new(reader).boxed()
			},
			tg::CompressionFormat::Xz => {
				async_compression::tokio::bufread::XzDecoder::new(reader).boxed()
			},
			tg::CompressionFormat::Zstd => {
				async_compression::tokio::bufread::ZstdDecoder::new(reader).boxed()
			},
		};
		let blob = tg::Blob::with_reader(server, reader).await?;

		// Abort and await the log task.
		log_task.abort();
		log_task.await.ok();

		// Log that the decompression finished.
		let message = "finished decompressing\n";
		crate::runtime::util::log(
			server,
			process,
			tg::process::log::Stream::Stderr,
			message.to_owned(),
		)
		.await?;

		let output = if input.is_blob() {
			blob.into()
		} else if input.is_file() {
			tg::File::with_contents(blob).into()
		} else {
			unreachable!()
		};

		let output = crate::runtime::Output {
			checksum: None,
			error: None,
			exit: 0,
			output: Some(output),
		};

		Ok(output)
	}
}
