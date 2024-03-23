use crate::Server;
use tangram_client as tg;
use tangram_error::{error, Result};

impl Server {
	pub async fn get_package_metadata(
		&self,
		package: &tg::Directory,
	) -> Result<tg::package::Metadata> {
		self.try_get_package_metadata(package)
			.await?
			.ok_or_else(|| error!("missing package metadata"))
	}

	pub async fn try_get_package_metadata(
		&self,
		package: &tg::Directory,
	) -> Result<Option<tg::package::Metadata>> {
		let path = tg::package::get_root_module_path(self, package).await?;
		let file = package
			.get(self, &path)
			.await?
			.try_unwrap_file()
			.ok()
			.ok_or_else(|| error!(%path, "expected the module to be a file"))?;
		let text = file.text(self).await?;
		let metadata = crate::language::Server::analyze_module(text)
			.map_err(|source| error!(!source, %path, "failed to analyze module"))?
			.metadata;
		Ok(metadata)
	}
}
