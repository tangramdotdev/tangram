use crate::Server;
use tangram_client as tg;
use tangram_error::{error, Result};

impl Server {
	pub async fn get_package_metadata(
		&self,
		package: &tg::Directory,
	) -> Result<tg::package::Metadata> {
		let path = tg::package::get_root_module_path(self, package).await?;
		let file = package
			.get(self, &path)
			.await?
			.try_unwrap_file()
			.ok()
			.ok_or_else(|| error!(%path, "Expected the module to be a file."))?;
		let text = file.text(self).await?;
		let analysis = crate::language::Server::analyze_module(text)?;
		if let Some(metadata) = analysis.metadata {
			Ok(metadata)
		} else {
			Err(error!(?path, "Missing package metadata."))
		}
	}
}
