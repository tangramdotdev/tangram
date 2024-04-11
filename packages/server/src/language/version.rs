use super::Server;
use tangram_client as tg;

impl Server {
	pub async fn get_module_version(&self, module: &tg::Module) -> tg::Result<i32> {
		self.try_get_module_version(module)
			.await?
			.ok_or_else(|| tg::error!("expected the module to exist"))
	}

	pub async fn try_get_module_version(&self, module: &tg::Module) -> tg::Result<Option<i32>> {
		match module {
			tg::Module::Library(_) | tg::Module::Normal { .. } => Ok(Some(0)),
			tg::Module::Document(document) => self.try_get_document_version(document).await,
		}
	}
}
