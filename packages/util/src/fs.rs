use std::path::Path;
use tangram_error::{Result, Wrap, WrapErr};

pub async fn rmrf(path: impl AsRef<Path>) -> Result<()> {
	let path = path.as_ref();

	// Get the metadata for the path.
	let metadata = match tokio::fs::metadata(path).await {
		Ok(metadata) => metadata,

		// If there is no file system object at the path, then return.
		Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
			return Ok(());
		},

		Err(error) => {
			return Err(error.wrap("Failed to get the metadata for the path."));
		},
	};

	if metadata.is_dir() {
		tokio::fs::remove_dir_all(path)
			.await
			.wrap_err("Failed to remove the directory.")?;
	} else {
		tokio::fs::remove_file(path)
			.await
			.wrap_err("Failed to remove the file.")?;
	};

	Ok(())
}
