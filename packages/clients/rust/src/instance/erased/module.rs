use {
	crate::prelude::*,
	futures::{future::BoxFuture, prelude::*},
};

pub trait Module: Send + Sync + 'static {
	fn resolve_module(
		&self,
		arg: tg::module::resolve::Arg,
	) -> BoxFuture<'_, tg::Result<tg::module::resolve::Output>>;

	fn load_module(
		&self,
		arg: tg::module::load::Arg,
	) -> BoxFuture<'_, tg::Result<tg::module::load::Output>>;

	fn module_interface(
		&self,
		arg: tg::module::interface::Arg,
	) -> BoxFuture<'_, tg::Result<tg::module::interface::Output>>;
}

impl<T> Module for T
where
	T: tg::instance::Module,
{
	fn resolve_module(
		&self,
		arg: tg::module::resolve::Arg,
	) -> BoxFuture<'_, tg::Result<tg::module::resolve::Output>> {
		self.resolve_module(arg).boxed()
	}

	fn load_module(
		&self,
		arg: tg::module::load::Arg,
	) -> BoxFuture<'_, tg::Result<tg::module::load::Output>> {
		self.load_module(arg).boxed()
	}

	fn module_interface(
		&self,
		arg: tg::module::interface::Arg,
	) -> BoxFuture<'_, tg::Result<tg::module::interface::Output>> {
		self.module_interface(arg).boxed()
	}
}
