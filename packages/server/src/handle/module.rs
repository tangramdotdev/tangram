use {
	super::ServerWithContext,
	crate::{Context, Server, Shared},
	tangram_client::prelude::*,
};

impl tg::handle::Module for Shared {
	async fn resolve_module(
		&self,
		arg: tg::module::resolve::Arg,
	) -> tg::Result<tg::module::resolve::Output> {
		self.0.resolve_module(arg).await
	}

	async fn load_module(
		&self,
		arg: tg::module::load::Arg,
	) -> tg::Result<tg::module::load::Output> {
		self.0.load_module(arg).await
	}
}

impl tg::handle::Module for Server {
	async fn resolve_module(
		&self,
		arg: tg::module::resolve::Arg,
	) -> tg::Result<tg::module::resolve::Output> {
		self.resolve_module_with_context(&Context::default(), arg)
			.await
	}

	async fn load_module(
		&self,
		arg: tg::module::load::Arg,
	) -> tg::Result<tg::module::load::Output> {
		self.load_module_with_context(&Context::default(), arg)
			.await
	}
}

impl tg::handle::Module for ServerWithContext {
	async fn resolve_module(
		&self,
		arg: tg::module::resolve::Arg,
	) -> tg::Result<tg::module::resolve::Output> {
		self.0.resolve_module_with_context(&self.1, arg).await
	}

	async fn load_module(
		&self,
		arg: tg::module::load::Arg,
	) -> tg::Result<tg::module::load::Output> {
		self.0.load_module_with_context(&self.1, arg).await
	}
}
