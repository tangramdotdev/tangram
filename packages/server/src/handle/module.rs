use {crate::Server, tangram_client::prelude::*};

impl tg::handle::Module for Server {
	async fn resolve_module(
		&self,
		arg: tg::module::resolve::Arg,
	) -> tg::Result<tg::module::resolve::Output> {
		// self.session(&self.context).resolve_module(arg).await
		Err(tg::error!("todo"))
	}

	async fn load_module(
		&self,
		arg: tg::module::load::Arg,
	) -> tg::Result<tg::module::load::Output> {
		// self.session(&self.context).load_module(arg).await
		Err(tg::error!("todo"))
	}
}
