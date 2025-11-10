use {
	tangram_client::prelude::*,
	tangram_futures::{
		read::{Ext as _, shared_position_reader::SharedPositionReader},
		stream::Ext as _,
	},
	tokio_util::task::AbortOnDropHandle,
};

pub(crate) async fn compress<H>(
	handle: &H,
	_process: Option<&tg::Process>,
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

	// Get the format.
	let format = match args.get(1) {
		Some(tg::value::Data::String(s)) => s
			.parse::<tg::CompressionFormat>()
			.map_err(|source| tg::error!(!source, "invalid format"))?,
		_ => return Err(tg::error!("expected a string")),
	};

	// Create the reader.
	let reader = blob.read(handle, tg::read::Options::default()).await?;
	let reader = SharedPositionReader::with_reader_and_position(reader, 0)
		.await
		.map_err(|source| tg::error!(!source, "failed to create the shared position reader"))?;

	// Spawn a task to log progress.
	let position = reader.shared_position();
	let size = blob.length(handle).await?;
	let (sender, receiver) = async_channel::bounded::<tg::Result<tg::progress::Event<()>>>(1024);
	let progress_task = AbortOnDropHandle::new(tokio::spawn({
		let position = position.clone();
		async move {
			loop {
				let current = position.load(std::sync::atomic::Ordering::Relaxed);
				let indicator = tg::progress::Indicator {
					current: Some(current),
					format: tg::progress::IndicatorFormat::Bytes,
					name: String::new(),
					title: "compressing".to_owned(),
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
		let logger = logger.clone();
		async move { crate::log_progress_stream(&logger, stream).await.ok() }
	});
	let log_task_abort_handle = log_task.abort_handle();
	scopeguard::defer! {
		log_task_abort_handle.abort();
	};

	// Compress the blob.
	let reader = match format {
		tg::CompressionFormat::Bz2 => {
			async_compression::tokio::bufread::BzEncoder::new(reader).boxed()
		},
		tg::CompressionFormat::Gz => {
			async_compression::tokio::bufread::GzipEncoder::new(reader).boxed()
		},
		tg::CompressionFormat::Xz => {
			async_compression::tokio::bufread::XzEncoder::new(reader).boxed()
		},
		tg::CompressionFormat::Zstd => {
			async_compression::tokio::bufread::ZstdEncoder::new(reader).boxed()
		},
	};
	let blob = tg::Blob::with_reader(handle, reader).await?;

	// Abort and await the log task.
	log_task.abort();
	log_task.await.ok();

	// Log that the compression finished.
	let message = "finished compressing\n";
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
