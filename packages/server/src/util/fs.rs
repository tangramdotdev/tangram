use futures::TryFutureExt;
use std::path::Path;

pub async fn remove(path: impl AsRef<Path>) -> std::io::Result<()> {
	tokio::fs::remove_file(path.as_ref())
		.or_else(|_| tokio::fs::remove_dir_all(path.as_ref()))
		.await
}
