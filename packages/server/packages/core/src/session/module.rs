use {crate::Session, tangram_client::prelude::*};

impl tg::instance::Module for Session {
	async fn resolve_module(
		&self,
		arg: tg::module::resolve::Arg,
	) -> tg::Result<tg::module::resolve::Output> {
		self.server
			.components
			.module
			.resolve_module(self, arg)
			.await
	}

	async fn load_module(
		&self,
		arg: tg::module::load::Arg,
	) -> tg::Result<tg::module::load::Output> {
		self.server.components.module.load_module(self, arg).await
	}
}
