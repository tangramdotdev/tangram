use {
	num::ToPrimitive as _, tangram_client::prelude::*, tangram_futures::write::Ext as _,
	tokio::io::AsyncWriteExt as _,
};

pub(crate) async fn checksum<H>(
	handle: &H,
	process: &tg::Process,
	_logger: crate::Logger,
) -> tg::Result<crate::Output>
where
	H: tg::Handle,
{
	let command = process.command(handle).await?;

	// Get the args.
	let args = command.args(handle).await?;

	// Get the object.
	let object = args
		.first()
		.ok_or_else(|| tg::error!("invalid number of arguments"))?
		.clone()
		.try_unwrap_object()
		.ok()
		.ok_or_else(|| tg::error!("expected an object"))?;

	// Get the algorithm.
	let algorithm = args
		.get(1)
		.ok_or_else(|| tg::error!("invalid number of arguments"))?
		.try_unwrap_string_ref()
		.ok()
		.ok_or_else(|| tg::error!("expected a string"))?
		.parse::<tg::checksum::Algorithm>()
		.map_err(|source| tg::error!(!source, "invalid algorithm"))?;

	// Compute the checksum.
	let checksum = if let Ok(blob) = tg::Blob::try_from(object.clone()) {
		checksum_blob(handle, &blob, algorithm).await?
	} else if let Ok(artifact) = tg::Artifact::try_from(object.clone()) {
		checksum_artifact(handle, &artifact, algorithm).await?
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
) -> tg::Result<tg::Checksum>
where
	H: tg::Handle,
{
	let mut writer = tg::checksum::Writer::new(algorithm);
	let mut reader = blob.read(handle, tg::read::Options::default()).await?;
	tokio::io::copy(&mut reader, &mut writer)
		.await
		.map_err(|source| tg::error!(!source, "failed to write the file contents"))?;
	let checksum = writer.finalize();
	Ok(checksum)
}

async fn checksum_artifact<H>(
	handle: &H,
	artifact: &tg::Artifact,
	algorithm: tg::checksum::Algorithm,
) -> tg::Result<tg::Checksum>
where
	H: tg::Handle,
{
	let mut writer = tg::checksum::Writer::new(algorithm);
	writer
		.write_uvarint(0)
		.await
		.map_err(|source| tg::error!(!source, "failed to write the archive version"))?;
	checksum_artifact_inner(handle, &mut writer, artifact).await?;
	let checksum = writer.finalize();
	Ok(checksum)
}

async fn checksum_artifact_inner<H>(
	handle: &H,
	writer: &mut tg::checksum::Writer,
	artifact: &tg::Artifact,
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
				Box::pin(checksum_artifact_inner(handle, writer, &artifact.clone())).await?;
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
