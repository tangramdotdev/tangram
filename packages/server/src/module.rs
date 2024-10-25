use crate::Server;
use std::path::{Path, PathBuf};
use tangram_client as tg;
use tangram_either::Either;

impl Server {
	pub(crate) async fn root_module_for_package(
		&self,
		package: Either<tg::directory::Id, PathBuf>,
	) -> tg::Result<tg::Module> {
		todo!()
	}

	pub(crate) async fn module_for_path(&self, path: &Path) -> tg::Result<tg::Module> {
		todo!()
	}
}
