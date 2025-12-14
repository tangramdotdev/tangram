use {
	num::ToPrimitive as _,
	std::sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
	tangram_client::prelude::*,
	tangram_futures::{
		read::shared_position_reader::SharedPositionReader, stream::Ext as _, task::Task,
		write::Ext as _,
	},
	tokio::io::AsyncWriteExt as _,
};

pub(crate) async fn checksum<H>(
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
	// Get the object.
	let object = match args.first() {
		Some(tg::value::Data::Object(id)) => tg::Object::with_id(id.clone()),
		_ => return Err(tg::error!("expected an object")),
	};

	// Get the algorithm.
	let algorithm = match args.get(1) {
		Some(tg::value::Data::String(s)) => s
			.parse::<tg::checksum::Algorithm>()
			.map_err(|source| tg::error!(!source, "invalid algorithm"))?,
		_ => return Err(tg::error!("expected a string")),
	};

	// Compute the checksum.
	let checksum = if let Ok(blob) = tg::Blob::try_from(object.clone()) {
		checksum_blob(handle, &blob, algorithm, &logger).await?
	} else if let Ok(artifact) = tg::Artifact::try_from(object.clone()) {
		checksum_artifact(handle, &artifact, algorithm, &logger).await?
	} else {
		return Err(tg::error!("invalid object"));
	};

	let output = checksum.to_string().into();

	let output = crate::Output {
		checksum: None,
		error: None,
		exit: 0,
		output: Some(output),
	};

	Ok(output)
}

async fn checksum_blob<H>(
	handle: &H,
	blob: &tg::Blob,
	algorithm: tg::checksum::Algorithm,
	logger: &crate::Logger,
) -> tg::Result<tg::Checksum>
where
	H: tg::Handle,
{
	let reader = blob.read(handle, tg::read::Options::default()).await?;
	let reader = SharedPositionReader::with_reader_and_position(reader, 0)
		.await
		.map_err(|source| tg::error!(!source, "failed to create the shared position reader"))?;

	// Spawn a task to log progress.
	let position = reader.shared_position();
	let (sender, receiver) = async_channel::bounded::<tg::Result<tg::progress::Event<()>>>(1024);
	let progress_task = Task::spawn({
		let position = position.clone();
		|_| async move {
			loop {
				let current = position.load(Ordering::Relaxed);
				let indicator = tg::progress::Indicator {
					current: Some(current),
					format: tg::progress::IndicatorFormat::Bytes,
					name: String::new(),
					title: "computing checksum".to_owned(),
					total: None,
				};
				let event = tg::progress::Event::Indicators::<()>(vec![indicator]);
				if sender.send(Ok(event)).await.is_err() {
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

	let mut writer = tg::checksum::Writer::new(algorithm);
	let mut reader = reader;
	tokio::io::copy(&mut reader, &mut writer)
		.await
		.map_err(|source| tg::error!(!source, "failed to write the file contents"))?;

	// Abort the log task.
	log_task.abort();

	// Log that the checksum computation finished.
	let message = "finished computing checksum\n";
	logger(tg::process::log::Stream::Stderr, message.to_owned()).await?;

	let checksum = writer.finalize();
	Ok(checksum)
}

async fn checksum_artifact<H>(
	handle: &H,
	artifact: &tg::Artifact,
	algorithm: tg::checksum::Algorithm,
	logger: &crate::Logger,
) -> tg::Result<tg::Checksum>
where
	H: tg::Handle,
{
	// Create the position tracker.
	let position = Arc::new(AtomicU64::new(0));

	// Spawn a task to log progress.
	let (sender, receiver) = async_channel::bounded::<tg::Result<tg::progress::Event<()>>>(1024);
	let progress_task = Task::spawn({
		let position = position.clone();
		|_| async move {
			loop {
				let current = position.load(Ordering::Relaxed);
				let indicator = tg::progress::Indicator {
					current: Some(current),
					format: tg::progress::IndicatorFormat::Bytes,
					name: String::new(),
					title: "computing checksum".to_owned(),
					total: None,
				};
				let event = tg::progress::Event::Indicators::<()>(vec![indicator]);
				if sender.send(Ok(event)).await.is_err() {
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

	let mut writer = tg::checksum::Writer::new(algorithm);
	writer
		.write_uvarint(0)
		.await
		.map_err(|source| tg::error!(!source, "failed to write the archive version"))?;
	checksum_artifact_inner(handle, &mut writer, artifact, &position).await?;
	let checksum = writer.finalize();

	// Abort the log task.
	log_task.abort();

	// Log that the checksum computation finished.
	let message = "finished computing checksum\n";
	logger(tg::process::log::Stream::Stderr, message.to_owned()).await?;

	Ok(checksum)
}

async fn checksum_artifact_inner<H>(
	handle: &H,
	writer: &mut tg::checksum::Writer,
	artifact: &tg::Artifact,
	position: &Arc<AtomicU64>,
) -> tg::Result<()>
where
	H: tg::Handle,
{
	match artifact {
		tg::Artifact::Directory(directory) => {
			let entries = directory.entries(handle).await?;
			writer
				.write_uvarint(0)
				.await
				.map_err(|source| tg::error!(!source, "failed to write the artifact kind"))?;
			let len = entries.len().to_u64().unwrap();
			writer.write_uvarint(len).await.map_err(|source| {
				tg::error!(!source, "failed to write the number of directory entries")
			})?;
			for (name, artifact) in entries {
				let len = name.len().to_u64().unwrap();
				writer
					.write_uvarint(len)
					.await
					.map_err(|source| tg::error!(!source, "failed to write the name length"))?;
				writer
					.write_all(name.as_bytes())
					.await
					.map_err(|source| tg::error!(!source, "failed to write the name"))?;
				Box::pin(checksum_artifact_inner(
					handle,
					writer,
					&artifact.clone(),
					position,
				))
				.await?;
			}
		},
		tg::Artifact::File(file) => {
			if !file.dependencies(handle).await?.is_empty() {
				return Err(tg::error!("cannot checksum a file with dependencies"));
			}
			let executable = file.executable(handle).await?;
			let length = file.length(handle).await?;
			let mut reader = file.read(handle, tg::read::Options::default()).await?;
			writer
				.write_uvarint(1)
				.await
				.map_err(|source| tg::error!(!source, "failed to write the artifact kind"))?;
			writer
				.write_uvarint(executable.into())
				.await
				.map_err(|source| tg::error!(!source, "failed to write the executable bit"))?;
			writer
				.write_uvarint(length)
				.await
				.map_err(|source| tg::error!(!source, "failed to write the file length"))?;
			tokio::io::copy(&mut reader, writer)
				.await
				.map_err(|source| tg::error!(!source, "failed to write the file contents"))?;
			position.fetch_add(length, Ordering::Relaxed);
		},
		tg::Artifact::Symlink(symlink) => {
			if symlink.artifact(handle).await?.is_some() {
				return Err(tg::error!("cannot checksum a symlink with an artifact"));
			}
			let path = symlink
				.path(handle)
				.await?
				.ok_or_else(|| tg::error!("cannot checksum a symlink without a path"))?;
			let target = path.to_string_lossy();
			writer
				.write_uvarint(2)
				.await
				.map_err(|source| tg::error!(!source, "failed to write the artifact kind"))?;
			let len = target.len().to_u64().unwrap();
			writer
				.write_uvarint(len)
				.await
				.map_err(|source| tg::error!(!source, "failed to write the target length"))?;
			writer
				.write_all(target.as_bytes())
				.await
				.map_err(|source| tg::error!(!source, "failed to write the symlink target"))?;
		},
	}
	Ok(())
}
