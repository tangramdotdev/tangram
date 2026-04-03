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
}

impl<T> Module for T
where
	T: tg::handle::Module,
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
}
