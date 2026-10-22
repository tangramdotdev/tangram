use {
	crate::prelude::*,
	futures::{FutureExt as _, future::BoxFuture},
};

pub trait Clock: Send + Sync + 'static {
	fn try_get_clock(&self) -> BoxFuture<'_, tg::Result<Option<tg::clock::get::Output>>>;

	fn try_set_clock(
		&self,
		arg: tg::clock::set::Arg,
	) -> BoxFuture<'_, tg::Result<Option<tg::clock::get::Output>>>;

	fn try_advance_clock(
		&self,
		arg: tg::clock::advance::Arg,
	) -> BoxFuture<'_, tg::Result<Option<tg::clock::get::Output>>>;
}

impl<T> Clock for T
where
	T: tg::instance::Clock,
{
	fn try_get_clock(&self) -> BoxFuture<'_, tg::Result<Option<tg::clock::get::Output>>> {
		self.try_get_clock().boxed()
	}

	fn try_set_clock(
		&self,
		arg: tg::clock::set::Arg,
	) -> BoxFuture<'_, tg::Result<Option<tg::clock::get::Output>>> {
		self.try_set_clock(arg).boxed()
	}

	fn try_advance_clock(
		&self,
		arg: tg::clock::advance::Arg,
	) -> BoxFuture<'_, tg::Result<Option<tg::clock::get::Output>>> {
		self.try_advance_clock(arg).boxed()
	}
}
