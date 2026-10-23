use {crate::Session, tangram_client::prelude::*};

impl Session {
	#[must_use]
	pub fn create_compiler(&self) -> tangram_compiler::Shared {
		let instance = tg::instance::dynamic::Instance::new(self.clone());
		let store_path = self.server.store_path();
		let library_path = self.server.library_path();
		let main_runtime_handle = tokio::runtime::Handle::current();
		let version = self.server.version.clone();
		tangram_compiler::Compiler::start(
			instance,
			store_path,
			library_path,
			main_runtime_handle,
			version,
		)
	}
}
