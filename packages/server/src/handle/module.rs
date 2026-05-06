use {crate::Handle, tangram_client::prelude::*};

impl tg::handle::Module for Handle {
	async fn resolve_module(
		&self,
		arg: tg::module::resolve::Arg,
	) -> tg::Result<tg::module::resolve::Output> {
		self.resolve_module(arg).await
	}

	async fn load_module(
		&self,
		arg: tg::module::load::Arg,
	) -> tg::Result<tg::module::load::Output> {
		self.load_module(arg).await
	}
}
