use super::Runtime;
use crate::{runtime::util, temp::Temp};
use async_zip::base::read::stream::ZipFileReader;
use futures::AsyncReadExt as _;
use std::{
	os::unix::fs::PermissionsExt as _,
	pin::{Pin, pin},
};
use tangram_client as tg;
use tangram_futures::stream::TryExt as _;
use tangram_futures::{read::shared_position_reader::SharedPositionReader, stream::Ext as _};
use tokio::io::{AsyncBufReadExt as _, AsyncRead};
use tokio_util::{compat::FuturesAsyncReadCompatExt as _, task::AbortOnDropHandle};

impl Runtime {
	pub async fn extract(&self, process: &tg::Process) -> tg::Result<crate::runtime::Output> {
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
		let reader = crate::blob::Reader::new(&self.server, blob.clone()).await?;
		let mut reader = SharedPositionReader::with_reader_and_position(reader, 0)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the shared position reader"))?;

		// Detect the archive and compression formats.
		let buffer = reader
			.fill_buf()
			.await
			.map_err(|source| tg::error!(!source, "failed to fill the buffer"))?;
		let (format, compression) = super::util::detect_archive_format(buffer)?
			.ok_or_else(|| tg::error!("invalid archive format"))?;

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
						title: "extracting".to_owned(),
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

		// Create a temp.
		let temp = Temp::new(&self.server);

		// Extract to the temp.
		match format {
			tg::ArchiveFormat::Tar => {
				self.extract_tar(&temp, &mut reader, compression).await?;
			},
			tg::ArchiveFormat::Zip => {
				self.extract_zip(&temp, &mut reader).await?;
			},
		}

		// Check in the temp.
		let stream = self
			.server
			.checkin(tg::checkin::Arg {
				deterministic: false,
				ignore: false,
				lock: false,
				locked: false,
				destructive: true,
				path: temp.path().to_owned(),
				updates: Vec::new(),
			})
			.await?;
		let output = pin!(stream)
			.try_last()
			.await?
			.and_then(|event| event.try_unwrap_output().ok())
			.ok_or_else(|| tg::error!("stream ended without output"))?;
		let artifact = tg::Artifact::with_id(output.referent.item);

		// Abort and await the log task.
		log_task.abort();
		log_task.await.ok();

		// Log that the extraction finished.
		let message = "finished extracting\n";
		util::log(
			server,
			process,
			tg::process::log::Stream::Stderr,
			message.to_owned(),
		)
		.await;

		let output = artifact.into();

		let output = crate::runtime::Output {
			checksum: None,
			error: None,
			exit: 0,
			output: Some(output),
		};

		Ok(output)
	}

	pub(super) async fn extract_tar(
		&self,
		temp: &Temp,
		reader: &mut (impl tokio::io::AsyncBufRead + Send + Unpin + 'static),
		compression: Option<tg::CompressionFormat>,
	) -> tg::Result<()> {
		let reader: Pin<Box<dyn AsyncRead + Send>> = match compression {
			Some(tg::CompressionFormat::Bz2) => {
				Box::pin(async_compression::tokio::bufread::BzDecoder::new(reader))
			},
			Some(tg::CompressionFormat::Gz) => {
				Box::pin(async_compression::tokio::bufread::GzipDecoder::new(reader))
			},
			Some(tg::CompressionFormat::Xz) => {
				Box::pin(async_compression::tokio::bufread::XzDecoder::new(reader))
			},
			Some(tg::CompressionFormat::Zstd) => {
				Box::pin(async_compression::tokio::bufread::ZstdDecoder::new(reader))
			},
			None => Box::pin(reader),
		};
		tokio_tar::ArchiveBuilder::new(reader)
			.set_preserve_permissions(true)
			.build()
			.unpack(&temp)
			.await
			.map_err(|source| tg::error!(!source, "failed to extract the archive"))?;
		Ok(())
	}

	pub(super) async fn extract_zip(
		&self,
		temp: &Temp,
		reader: &mut (impl tokio::io::AsyncBufRead + Send + Unpin + 'static),
	) -> tg::Result<()> {
		// Create the reader.
		let mut reader = Some(ZipFileReader::with_tokio(reader));

		// Extract.
		loop {
			let Some(mut entry) = reader
				.take()
				.unwrap()
				.next_with_entry()
				.await
				.map_err(|source| tg::error!(!source, "failed to read first entry"))?
			else {
				break;
			};

			// Get the reader
			let entry_reader = entry.reader_mut();

			// Get the path.
			let filename = entry_reader
				.entry()
				.filename()
				.as_str()
				.map_err(|source| tg::error!(!source, "failed to get the entry filename"))?;
			let path = temp.path().join(filename);

			// Check if the entry is a directory.
			let is_dir = entry_reader
				.entry()
				.dir()
				.map_err(|source| tg::error!(!source, "failed to get type of entry"))?;

			// Check if the entry is a symlink.
			let is_symlink = entry_reader
				.entry()
				.unix_permissions()
				.is_some_and(|permissions| permissions & 0o120_000 == 0o120_000);

			// Check if the entry is executable.
			let is_executable = entry_reader
				.entry()
				.unix_permissions()
				.is_some_and(|permissions| permissions & 0o000_111 != 0);

			if is_dir {
				tokio::fs::create_dir_all(&path)
					.await
					.map_err(|source| tg::error!(!source, "failed to create the directory"))?;
			} else if is_symlink {
				let mut buffer = Vec::new();
				entry_reader
					.read_to_end(&mut buffer)
					.await
					.map_err(|source| tg::error!(!source, "failed to read symlink target"))?;
				let target = std::str::from_utf8(&buffer)
					.map_err(|source| tg::error!(!source, "symlink target not valid UTF-8"))?;
				tokio::fs::symlink(target, &path)
					.await
					.map_err(|source| tg::error!(!source, "failed to create the symlink"))?;
			} else {
				let parent = path.parent().expect("expected the entry to have a parent");
				tokio::fs::create_dir_all(parent)
					.await
					.map_err(|source| tg::error!(!source, "failed to create the directory"))?;
				let mut file = tokio::fs::OpenOptions::new()
					.write(true)
					.create_new(true)
					.open(&path)
					.await
					.map_err(|source| tg::error!(!source, "failed to create the file"))?;
				tokio::io::copy(&mut entry_reader.compat(), &mut file)
					.await
					.map_err(|source| tg::error!(!source, "failed to write the file"))?;
				if is_executable {
					let permissions = std::fs::Permissions::from_mode(0o755);
					tokio::fs::set_permissions(&path, permissions)
						.await
						.map_err(|source| tg::error!(!source, "failed to set the permissions"))?;
				}
			}

			// Advance the reader.
			let value = entry
				.done()
				.await
				.map_err(|source| tg::error!(!source, "failed to advance the reader"))?;
			reader.replace(value);
		}

		Ok(())
	}
}
