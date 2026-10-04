use {super::Compiler, tangram_client::prelude::*};

mod db;
mod library;
mod system;

pub mod load;
pub mod resolve;

impl Compiler {
	pub(super) async fn check_py(
		&self,
		modules: Vec<tg::module::Data>,
	) -> tg::Result<Vec<tg::Diagnostic>> {
		let compiler = self.clone();
		tokio::task::spawn_blocking(move || db::Database::new(compiler)?.check(modules))
			.await
			.map_err(|error| tg::error!(!error, "the Python checker task failed"))?
	}
}
