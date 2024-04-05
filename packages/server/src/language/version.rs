use super::Server;
use tangram_client as tg;

impl Server {
	pub async fn get_module_version(&self, module: &tg::Module) -> tg::Result<i32> {
		match module {
			tg::Module::Library(_) | tg::Module::Normal { .. } => Ok(0),
			tg::Module::Document(document) => self.get_document_version(document).await,
		}
	}
}
