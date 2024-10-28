use futures::TryFutureExt;
use std::path::Path;

pub async fn remove(path: impl AsRef<Path>) -> std::io::Result<()> {
	let path = path.as_ref();
	tokio::fs::remove_file(path)
		.or_else(|_| tokio::fs::remove_dir_all(path))
		.await
}
