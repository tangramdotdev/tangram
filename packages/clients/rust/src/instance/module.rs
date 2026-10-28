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

	fn module_interface(
		&self,
		arg: tg::module::interface::Arg,
	) -> impl Future<Output = tg::Result<tg::module::interface::Output>> + Send;
}

impl tg::instance::Module for tg::Client {
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

	async fn module_interface(
		&self,
		arg: tg::module::interface::Arg,
	) -> tg::Result<tg::module::interface::Output> {
		self.session(&self.context).module_interface(arg).await
	}
}
