use {
	tangram_client::prelude::*,
	tangram_futures::{
		read::{Ext as _, shared_position_reader::SharedPositionReader},
		stream::Ext as _,
		task::Task,
	},
	tokio::io::AsyncBufReadExt as _,
};

pub(crate) async fn decompress<H>(
	handle: &H,
	args: tg::value::data::Array,
	_cwd: std::path::PathBuf,
	_env: tg::value::data::Map,
	_executable: tg::command::data::Executable,
	logger: crate::Logger,
) -> tg::Result<crate::Output>
where
	H: tg::Handle,
{
	// Get the blob.
	let input = args
		.first()
		.ok_or_else(|| tg::error!("invalid number of arguments"))?;
	let (blob, is_file) = match input {
		tg::value::Data::Object(id) => {
			let object = tg::Object::with_id(id.clone());
			match object {
				tg::Object::Blob(blob) => (blob, false),
				tg::Object::File(file) => (file.contents(handle).await?, true),
				_ => return Err(tg::error!("expected a blob or a file")),
			}
		},
		_ => {
			return Err(tg::error!("expected a blob or a file"));
		},
	};

	// Create the reader.
	let reader = blob.read(handle, tg::read::Options::default()).await?;
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
	let size = blob.length(handle).await?;
	let (sender, receiver) = async_channel::bounded::<tg::Result<tg::progress::Event<()>>>(1024);
	let progress_task = Task::spawn({
		let position = position.clone();
		|_| async move {
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
	});
	let stream = receiver.attach(progress_task);
	let log_task = Task::spawn({
		let logger = logger.clone();
		|_| async move { crate::log_progress_stream(&logger, stream).await.ok() }
	});

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
	let blob = tg::Blob::with_reader(handle, reader).await?;

	// Abort the log task.
	log_task.abort();

	// Log that the decompression finished.
	let message = "finished decompressing\n";
	logger(tg::process::log::Stream::Stderr, message.to_owned()).await?;

	let output = if is_file {
		tg::File::with_contents(blob).into()
	} else {
		blob.into()
	};

	let output = crate::Output {
		checksum: None,
		error: None,
		exit: 0,
		output: Some(output),
	};

	Ok(output)
}
