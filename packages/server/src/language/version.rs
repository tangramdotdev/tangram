use super::Server;
use tangram_client as tg;

impl Server {
	pub async fn get_module_version(&self, module: &tg::Module) -> tg::Result<i32> {
		self.try_get_module_version(module)
			.await?
			.ok_or_else(|| tg::error!("expected the module to exist"))
	}

	pub async fn try_get_module_version(&self, module: &tg::Module) -> tg::Result<Option<i32>> {
		if matches!(
			module,
			tg::Module::Js(tg::module::Js::PackagePath(_))
				| tg::Module::Ts(tg::module::Js::PackagePath(_))
		) {
			self.try_get_document_version(module).await
		} else {
			Ok(Some(0))
		}
	}
}
