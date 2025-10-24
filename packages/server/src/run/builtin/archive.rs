use {
	crate::Server,
	futures::future,
	std::path::Path,
	tangram_client as tg,
	tangram_futures::read::Ext as _,
	tokio_util::compat::{FuturesAsyncWriteCompatExt as _, TokioAsyncWriteCompatExt as _},
};

impl Server {
	pub async fn run_builtin_archive(
		&self,
		process: &tg::Process,
	) -> tg::Result<crate::run::Output> {
		let command = process.command(self).await?;

		// Get the args.
		let args = command.args(self).await?;

		// Get the artifact.
		let artifact: tg::Artifact = args
			.first()
			.ok_or_else(|| tg::error!("invalid number of arguments"))?
			.clone()
			.try_into()
			.ok()
			.ok_or_else(|| tg::error!("expected an artifact"))?;

		// Get the format.
		let format = args
			.get(1)
			.ok_or_else(|| tg::error!("invalid number of arguments"))?
			.try_unwrap_string_ref()
			.ok()
			.ok_or_else(|| tg::error!("expected a string"))?
			.parse::<tg::ArchiveFormat>()
			.map_err(|source| tg::error!(!source, "invalid format"))?;

		// Get the compression.
		let compression = if let Some(tg::Value::String(value)) = args.get(2) {
			let compression = value
				.parse::<tg::CompressionFormat>()
				.map_err(|source| tg::error!(!source, "invalid compression format"))?;
			Some(compression)
		} else {
			None
		};

		if compression.is_some() && matches!(format, tg::ArchiveFormat::Zip) {
			return Err(tg::error!("compression is not supported for zip archives"));
		}

		// Archive.
		let blob = match format {
			tg::ArchiveFormat::Tar => self.run_builtin_tar(&artifact, compression).await?,
			tg::ArchiveFormat::Zip => self.run_builtin_zip(&artifact).await?,
		};
		let output = blob.into();

		let output = crate::run::Output {
			checksum: None,
			error: None,
			exit: 0,
			output: Some(output),
		};

		Ok(output)
	}

	async fn run_builtin_tar(
		&self,
		artifact: &tg::Artifact,
		compression: Option<tg::CompressionFormat>,
	) -> tg::Result<tg::Blob> {
		// Create a duplex stream.
		let (reader, writer) = tokio::io::duplex(8192);

		// Create the archive future.
		let archive_future = async move {
			// Create the tar builder.
			let mut builder = tokio_tar::Builder::new(writer);

			// Archive the artifact.
			let directory = artifact
				.try_unwrap_directory_ref()
				.ok()
				.ok_or_else(|| tg::error!("expected a directory"))?;
			for (name, artifact) in directory.entries(self).await? {
				self.run_builtin_tar_inner(&mut builder, Path::new(&name), &artifact)
					.await?;
			}

			// Finish writing the archive.
			builder
				.finish()
				.await
				.map_err(|source| tg::error!(!source, "failed to finish the archive"))?;

			Ok::<_, tg::Error>(())
		};

		// If compression is requested, use the appropriate encoder.
		let reader = match compression {
			Some(tg::CompressionFormat::Bz2) => {
				async_compression::tokio::bufread::BzEncoder::new(tokio::io::BufReader::new(reader))
					.boxed()
			},
			Some(tg::CompressionFormat::Gz) => async_compression::tokio::bufread::GzipEncoder::new(
				tokio::io::BufReader::new(reader),
			)
			.boxed(),
			Some(tg::CompressionFormat::Xz) => {
				async_compression::tokio::bufread::XzEncoder::new(tokio::io::BufReader::new(reader))
					.boxed()
			},
			Some(tg::CompressionFormat::Zstd) => {
				async_compression::tokio::bufread::ZstdEncoder::new(tokio::io::BufReader::new(
					reader,
				))
				.boxed()
			},
			None => reader.boxed(),
		};

		// Create the blob future.
		let blob_future = tg::Blob::with_reader(self, reader);

		// Join the futures.
		let blob = match future::join(archive_future, blob_future).await {
			(_, Ok(blob)) => blob,
			(Err(source), _) | (_, Err(source)) => {
				return Err(tg::error!(
					!source,
					"failed to join the archive and blob futures"
				));
			},
		};

		Ok(blob)
	}

	async fn run_builtin_tar_inner<W>(
		&self,
		builder: &mut tokio_tar::Builder<W>,
		path: &Path,
		artifact: &tg::Artifact,
	) -> tg::Result<()>
	where
		W: tokio::io::AsyncWrite + Unpin + Send,
	{
		match artifact {
			tg::Artifact::Directory(directory) => {
				let mut header = tokio_tar::Header::new_gnu();
				header.set_size(0);
				header.set_entry_type(tokio_tar::EntryType::Directory);
				header.set_mode(0o755);
				builder
					.append_data(&mut header, path, &[][..])
					.await
					.map_err(|source| tg::error!(!source, "failed to append directory"))?;
				for (name, artifact) in directory.entries(self).await? {
					Box::pin(self.run_builtin_tar_inner(builder, &path.join(name), &artifact))
						.await?;
				}
				Ok(())
			},
			tg::Artifact::File(file) => {
				if !file.dependencies(self).await?.is_empty() {
					return Err(tg::error!("cannot archive a file with dependencies"));
				}
				let size = file.length(self).await?;
				let reader = file.read(self, tg::read::Options::default()).await?;
				let executable = file.executable(self).await?;
				let mut header = tokio_tar::Header::new_gnu();
				header.set_size(size);
				header.set_entry_type(tokio_tar::EntryType::Regular);
				let permissions = if executable { 0o0755 } else { 0o0644 };
				header.set_mode(permissions);
				builder
					.append_data(&mut header, path, reader)
					.await
					.map_err(|source| tg::error!(!source, "failed to append file"))
			},
			tg::Artifact::Symlink(symlink) => {
				if symlink.artifact(self).await?.is_some() {
					return Err(tg::error!("cannot archive a symlink with an artifact"));
				}
				let target = symlink
					.path(self)
					.await?
					.ok_or_else(|| tg::error!("cannot archive a symlink without a path"))?;
				let mut header = tokio_tar::Header::new_gnu();
				header.set_size(0);
				header.set_entry_type(tokio_tar::EntryType::Symlink);
				header.set_mode(0o777);
				header
					.set_link_name(target.to_string_lossy().as_ref())
					.map_err(|source| tg::error!(!source, "failed to set symlink target"))?;
				builder
					.append_data(&mut header, path, &[][..])
					.await
					.map_err(|source| tg::error!(!source, "failed to append symlink"))
			},
		}
	}

	async fn run_builtin_zip(&self, artifact: &tg::Artifact) -> tg::Result<tg::Blob> {
		// Create a duplex stream.
		let (reader, writer) = tokio::io::duplex(8192);

		// Create the archive future.
		let archive_future = async move {
			// Create the tar builder.
			let mut builder = async_zip::base::write::ZipFileWriter::new(writer.compat_write());

			// Archive the artifact.
			let directory = artifact
				.try_unwrap_directory_ref()
				.ok()
				.ok_or_else(|| tg::error!("expected a directory"))?;
			for (name, artifact) in directory.entries(self).await? {
				self.run_builtin_zip_inner(&mut builder, Path::new(&name), &artifact)
					.await?;
			}

			// Finish writing the archive.
			builder
				.close()
				.await
				.map(|_| ())
				.map_err(|source| tg::error!(!source, "failed to write the archive"))?;

			Ok::<_, tg::Error>(())
		};

		// Create the blob future.
		let blob_future = tg::Blob::with_reader(self, reader);

		// Join the futures.
		let blob = match future::join(archive_future, blob_future).await {
			(_, Ok(blob)) => blob,
			(Err(source), _) | (_, Err(source)) => {
				return Err(tg::error!(
					!source,
					"failed to join the archive and blob futures"
				));
			},
		};

		Ok(blob)
	}

	async fn run_builtin_zip_inner<W>(
		&self,
		builder: &mut async_zip::base::write::ZipFileWriter<W>,
		path: &Path,
		artifact: &tg::Artifact,
	) -> tg::Result<()>
	where
		W: futures::io::AsyncWrite + Unpin + Send + Sync,
	{
		match artifact {
			tg::Artifact::Directory(directory) => {
				let filename = format!("{}/", path.to_string_lossy());
				let entry = async_zip::ZipEntryBuilder::new(
					filename.into(),
					async_zip::Compression::Deflate,
				)
				.unix_permissions(0o755);
				builder
					.write_entry_whole(entry.build(), &[][..])
					.await
					.map_err(|source| tg::error!(!source, "failed to write the directory entry"))?;
				for (name, artifact) in directory.entries(self).await? {
					Box::pin(self.run_builtin_zip_inner(
						builder,
						&path.join(name),
						&artifact.clone(),
					))
					.await?;
				}
				Ok(())
			},
			tg::Artifact::File(file) => {
				if !file.dependencies(self).await?.is_empty() {
					return Err(tg::error!("cannot archive a file with dependencies"));
				}
				let executable = file.executable(self).await?;
				let permissions = if executable { 0o0755 } else { 0o0644 };
				let entry = async_zip::ZipEntryBuilder::new(
					path.to_string_lossy().as_ref().into(),
					async_zip::Compression::Deflate,
				)
				.unix_permissions(permissions);
				let mut entry_writer = builder
					.write_entry_stream(entry)
					.await
					.unwrap()
					.compat_write();
				let mut file_reader = file.read(self, tg::read::Options::default()).await?;
				tokio::io::copy(&mut file_reader, &mut entry_writer)
					.await
					.map_err(|source| tg::error!(!source, "failed to write the file entry"))?;
				entry_writer.into_inner().close().await.unwrap();
				Ok(())
			},
			tg::Artifact::Symlink(symlink) => {
				if symlink.artifact(self).await?.is_some() {
					return Err(tg::error!("cannot archive a symlink with an artifact"));
				}
				let target = symlink
					.path(self)
					.await?
					.ok_or_else(|| tg::error!("cannot archive a symlink without a path"))?;
				let entry = async_zip::ZipEntryBuilder::new(
					path.to_string_lossy().as_ref().into(),
					async_zip::Compression::Deflate,
				)
				.unix_permissions(0o120_777);
				builder
					.write_entry_whole(entry.build(), target.to_string_lossy().as_bytes())
					.await
					.map_err(|source| tg::error!(!source, "failed to write the symlink entry"))
			},
		}
	}
}
