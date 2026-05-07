use crate::prelude::*;

pub trait Module: Clone + Unpin + Send + Sync + 'static {
	fn resolve_module(
		&self,
		arg: tg::module::resolve::Arg,
	) -> impl Future<Output = tg::Result<tg::module::resolve::Output>> + Send;

	fn load_module(
		&self,
		arg: tg::module::load::Arg,
	) -> impl Future<Output = tg::Result<tg::module::load::Output>> + Send;
}

impl tg::handle::Module for tg::Client {
	async fn resolve_module(
		&self,
		arg: tg::module::resolve::Arg,
	) -> tg::Result<tg::module::resolve::Output> {
		self.session(&self.context).resolve_module(arg).await
	}

	async fn load_module(
		&self,
		arg: tg::module::load::Arg,
	) -> tg::Result<tg::module::load::Output> {
		self.session(&self.context).load_module(arg).await
	}
}
