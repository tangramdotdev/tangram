use futures::TryFutureExt;
use std::path::Path;

pub async fn remove(path: impl AsRef<Path>) -> std::io::Result<()> {
	let path = path.as_ref();
	tokio::fs::remove_file(path)
		.or_else(|_| tokio::fs::remove_dir_all(path))
		.await
}

#[cfg(test)]
pub async fn cleanup_instance(
	temp: tangram_temp::Temp,
	server: crate::Server,
) -> tangram_client::Result<()> {
	server.stop();
	server.wait().await;
	temp.remove()
		.await
		.map_err(|source| tangram_client::error!(!source, "failed to remove temp"))?;
	Ok(())
}
