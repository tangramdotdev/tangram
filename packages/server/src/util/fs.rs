use std::path::Path;
use tangram_client as tg;

pub async fn rmrf(path: impl AsRef<Path>) -> tg::Result<()> {
	let path = path.as_ref();

	// Get the metadata for the path.
	let metadata = match tokio::fs::metadata(path).await {
		Ok(metadata) => metadata,

		// If there is no file system object at the path, then return.
		Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
			return Ok(());
		},

		Err(error) => {
			let path = path.display();
			return Err(
				tg::error!(source = error, %path, "failed to get the metadata for the path"),
			);
		},
	};

	if metadata.is_dir() {
		tokio::fs::remove_dir_all(path).await.map_err(|source| {
			let path = path.display();
			tg::error!(!source, %path, "failed to remove the directory")
		})?;
	} else {
		tokio::fs::remove_file(path).await.map_err(|source| {
			let path = path.display();
			tg::error!(!source, %path, "failed to remove the file")
		})?;
	};

	Ok(())
}
