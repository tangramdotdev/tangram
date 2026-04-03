use {
	crate::prelude::*,
	futures::{future::BoxFuture, prelude::*},
};

pub trait Watch: Send + Sync + 'static {
	fn list_watches(
		&self,
		arg: tg::watch::list::Arg,
	) -> BoxFuture<'_, tg::Result<tg::watch::list::Output>>;

	fn delete_watch(&self, arg: tg::watch::delete::Arg) -> BoxFuture<'_, tg::Result<()>>;
	fn touch_watch(&self, arg: tg::watch::touch::Arg) -> BoxFuture<'_, tg::Result<()>>;
}

impl<T> Watch for T
where
	T: tg::handle::Watch,
{
	fn list_watches(
		&self,
		arg: tg::watch::list::Arg,
	) -> BoxFuture<'_, tg::Result<tg::watch::list::Output>> {
		self.list_watches(arg).boxed()
	}

	fn delete_watch(&self, arg: tg::watch::delete::Arg) -> BoxFuture<'_, tg::Result<()>> {
		self.delete_watch(arg).boxed()
	}

	fn touch_watch(&self, arg: tg::watch::touch::Arg) -> BoxFuture<'_, tg::Result<()>> {
		self.touch_watch(arg).boxed()
	}
}
