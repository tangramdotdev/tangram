use {
	crate::{Server, temp::Temp},
	futures::TryStreamExt as _,
	num::ToPrimitive as _,
	std::sync::{Arc, Mutex, atomic::AtomicU64},
	tangram_client as tg,
	tangram_futures::{read::Ext as _, stream::Ext},
	tangram_uri::Uri,
	tokio::io::AsyncBufReadExt as _,
	tokio_util::{io::StreamReader, task::AbortOnDropHandle},
};

enum Mode {
	Raw,
	Decompress(tg::CompressionFormat),
	Extract(tg::ArchiveFormat, Option<tg::CompressionFormat>),
}

impl Server {
	pub async fn run_builtin_download(
		&self,
		process: &tg::Process,
	) -> tg::Result<crate::run::Output> {
		let command = process.command(self).await?;

		// Get the expected checksum.
		let Some(expected_checksum) = process.load(self).await?.expected_checksum.clone() else {
			return Err(tg::error!("a download must have a checksum"));
		};

		// Get the args.
		let args = command.args(self).await?;
		let url: Uri = args
			.first()
			.ok_or_else(|| tg::error!("invalid number of arguments"))?
			.try_unwrap_string_ref()
			.ok()
			.ok_or_else(|| tg::error!("expected a string"))?
			.parse()
			.map_err(|source| tg::error!(!source, "invalid url"))?;
		let options: Option<tg::DownloadOptions> = args
			.get(1)
			.filter(|value| !value.is_null())
			.cloned()
			.map(TryInto::try_into)
			.transpose()
			.map_err(|source| tg::error!(!source, "invalid options"))?;

		// Send the request.
		let response = reqwest::get(url.as_str())
			.await
			.map_err(|source| tg::error!(!source, %url, "failed to perform the request"))?
			.error_for_status()
			.map_err(|source| tg::error!(!source, %url, "expected a success status"))?;

		// Log that the download started.
		let message = format!("downloading from \"{url}\"\n");
		crate::run::util::log(self, process, tg::process::log::Stream::Stderr, message).await?;

		// Spawn the progress and log tasks.
		let downloaded = Arc::new(AtomicU64::new(0));
		let content_length = response.content_length();
		let (sender, receiver) =
			async_channel::bounded::<tg::Result<tg::progress::Event<()>>>(1024);
		let progress_task = AbortOnDropHandle::new(tokio::spawn({
			let downloaded = downloaded.clone();
			async move {
				loop {
					let current = downloaded.load(std::sync::atomic::Ordering::Relaxed);
					let indicator = tg::progress::Indicator {
						current: Some(current),
						format: tg::progress::IndicatorFormat::Bytes,
						name: String::new(),
						title: "downloading".to_owned(),
						total: content_length,
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
			let server = self.clone();
			let process = process.clone();
			async move { server.log_progress_stream(&process, stream).await.ok() }
		});
		let log_task_abort_handle = log_task.abort_handle();
		scopeguard::defer! {
			log_task_abort_handle.abort();
		};

		// Create the checksum writer.
		let checksum = tg::checksum::Writer::new(expected_checksum.algorithm());
		let checksum = Arc::new(Mutex::new(checksum));

		// Create the reader.
		let stream = response
			.bytes_stream()
			.map_err(std::io::Error::other)
			.inspect_ok({
				let checksum = checksum.clone();
				let n = downloaded.clone();
				move |bytes| {
					// Update the checksum.
					checksum.lock().unwrap().update(bytes);

					// Update the progress.
					n.fetch_add(
						bytes.len().to_u64().unwrap(),
						std::sync::atomic::Ordering::Relaxed,
					);
				}
			});
		let mut reader = StreamReader::new(stream);

		// Fill the buffer.
		let header = reader
			.fill_buf()
			.await
			.map_err(|source| tg::error!(!source, "failed to fill the buffer"))?;

		// Determine the mode.
		let mode = options.and_then(|options| options.mode).unwrap_or_default();
		let mode = match mode {
			tg::DownloadMode::Raw => Mode::Raw,
			tg::DownloadMode::Decompress => {
				let format = super::util::detect_compression_format(header)?
					.ok_or_else(|| tg::error!("failed to determine the compression format"))?;
				Mode::Decompress(format)
			},
			tg::DownloadMode::Extract => {
				let (format, compression) = super::util::detect_archive_format(header)?
					.ok_or_else(|| tg::error!("failed to determine the archive format"))?;
				Mode::Extract(format, compression)
			},
		};

		// Download.
		let temp = Temp::new(self);
		match mode {
			Mode::Raw => {
				let mut file = tokio::fs::File::create(temp.path())
					.await
					.map_err(|source| tg::error!(!source, "failed create the temp file"))?;
				tokio::io::copy(&mut reader, &mut file)
					.await
					.map_err(|source| tg::error!(!source, "failed to write to the temp file"))?;
			},
			Mode::Decompress(format) => {
				let mut reader = match format {
					tg::CompressionFormat::Bz2 => {
						async_compression::tokio::bufread::BzDecoder::new(&mut reader).boxed()
					},
					tg::CompressionFormat::Gz => {
						async_compression::tokio::bufread::GzipDecoder::new(&mut reader).boxed()
					},
					tg::CompressionFormat::Xz => {
						async_compression::tokio::bufread::XzDecoder::new(&mut reader).boxed()
					},
					tg::CompressionFormat::Zstd => {
						async_compression::tokio::bufread::ZstdDecoder::new(&mut reader).boxed()
					},
				};
				let mut file = tokio::fs::File::create(temp.path())
					.await
					.map_err(|source| tg::error!(!source, "failed create the temp file"))?;
				tokio::io::copy(&mut reader, &mut file)
					.await
					.map_err(|source| tg::error!(!source, "failed to write to the temp file"))?;
			},
			Mode::Extract(format, compression) => match format {
				tg::ArchiveFormat::Tar => {
					self.run_builtin_extract_tar(&temp, &mut reader, compression)
						.await?;
				},
				tg::ArchiveFormat::Zip => {
					self.run_builtin_extract_zip(&temp, &mut reader).await?;
				},
			},
		}

		// Drain and drop the reader.
		tokio::io::copy(&mut reader, &mut tokio::io::sink())
			.await
			.map_err(|source| tg::error!(!source, "failed to drain the reader"))?;
		drop(reader);

		// Get the checksum.
		let checksum = Arc::try_unwrap(checksum)
			.unwrap()
			.into_inner()
			.unwrap()
			.finalize();

		// Check in the temp.
		let arg = tg::checkin::Arg {
			options: tg::checkin::Options {
				destructive: true,
				deterministic: true,
				ignore: false,
				lock: false,
				..Default::default()
			},
			path: temp.path().to_owned(),
			updates: Vec::new(),
		};
		let artifact = tg::checkin(self, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to check in the downloaded file"))?;

		// Abort and await the log task.
		log_task.abort();
		log_task.await.ok();

		// Log that the download finished.
		let message = format!("finished download from \"{url}\"\n");
		crate::run::util::log(self, process, tg::process::log::Stream::Stderr, message).await?;

		let output = match mode {
			Mode::Raw => artifact
				.try_unwrap_file()
				.map_err(|_| tg::error!("expected a file"))?
				.contents(self)
				.await?
				.into(),

			_ => artifact.into(),
		};

		let output = crate::run::Output {
			checksum: Some(checksum),
			error: None,
			exit: 0,
			output: Some(output),
		};

		Ok(output)
	}
}
