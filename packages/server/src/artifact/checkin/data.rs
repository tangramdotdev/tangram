use crate::Server;
use std::{collections::BTreeMap, os::unix::fs::PermissionsExt, path::Path};
use tangram_client as tg;

impl Server {
	pub(super) async fn create_file_data(&self, path: &Path) -> tg::Result<tg::file::Data> {
		if let Some(data) = xattr::get(path, tg::file::XATTR_NAME)
			.map_err(|source| tg::error!(!source, %path = path.display(), "failed to read xattr"))?
		{
			let data = serde_json::from_slice(&data)
				.map_err(|source| tg::error!(!source, "failed to deserialize file data"))?;
			return Ok(data);
		}

		// Read the file contents.
		let permit = self.file_descriptor_semaphore.acquire().await.unwrap();
		let file = tokio::fs::File::open(path)
			.await
			.map_err(|source| tg::error!(!source, %path = path.display(), "failed to read file"))?;
		let output = self
			.create_blob_inner(file, None)
			.await
			.map_err(|source| tg::error!(!source, "failed to create blob"))?;
		drop(permit);
		let contents = output.blob;

		// If there is no xattr the file has no dependencies.
		let dependencies = BTreeMap::new();

		// Get the executable bit
		let permit = self.file_descriptor_semaphore.acquire().await.unwrap();
		let metadata = tokio::fs::symlink_metadata(path).await.map_err(
			|source| tg::error!(!source, %path = path.display(), "failed to get the file permissions"),
		)?;
		let executable = metadata.permissions().mode() & 0o111 != 0;
		drop(permit);

		// Create the file data.
		Ok(tg::file::Data::Normal {
			contents,
			dependencies,
			executable,
		})
	}

	pub(super) async fn create_symlink_data(&self, path: &Path) -> tg::Result<tg::symlink::Data> {
		// Read the target from the symlink.
		let permit = self.file_descriptor_semaphore.acquire().await.unwrap();
		let target = tokio::fs::read_link(path).await.map_err(
			|source| tg::error!(!source, %path = path.display(), r#"failed to read the symlink at path"#,),
		)?;
		drop(permit);

		// Unrender the target.
		let target = target
			.to_str()
			.ok_or_else(|| tg::error!("the symlink target must be valid UTF-8"))?;
		let artifacts_path = self.artifacts_path();
		let artifacts_path = artifacts_path
			.to_str()
			.ok_or_else(|| tg::error!("the artifacts path must be valid UTF-8"))?;
		let target = tg::template::Data::unrender(artifacts_path, target)?;

		// Get the artifact and path.
		let (artifact, path) = if target.components.len() == 1 {
			let path = target.components[0]
				.try_unwrap_string_ref()
				.ok()
				.ok_or_else(|| tg::error!("invalid symlink"))?
				.clone();
			let path = path
				.parse()
				.map_err(|source| tg::error!(!source, "invalid symlink"))?;
			(None, Some(path))
		} else if target.components.len() == 2 {
			let artifact = target.components[0]
				.try_unwrap_artifact_ref()
				.ok()
				.ok_or_else(|| tg::error!("invalid symlink"))?
				.clone();
			let path = target.components[1]
				.try_unwrap_string_ref()
				.ok()
				.ok_or_else(|| tg::error!("invalid sylink"))?
				.clone();
			let path = &path[1..];
			let path = path
				.parse()
				.map_err(|source| tg::error!(!source, "invalid symlink"))?;
			(Some(artifact), Some(path))
		} else {
			return Err(tg::error!("invalid symlink"));
		};

		Ok(tg::symlink::Data::Normal { artifact, path })
	}
}
