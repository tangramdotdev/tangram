use {
	crate::prelude::*,
	futures::{future::BoxFuture, prelude::*},
};

pub trait Watch: Send + Sync + 'static {
	fn list_watches(
		&self,
		arg: tg::watch::list::Arg,
	) -> BoxFuture<'_, tg::Result<tg::watch::list::Output>>;

	fn try_delete_watch(
		&self,
		arg: tg::watch::delete::Arg,
	) -> BoxFuture<'_, tg::Result<Option<()>>>;
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

	fn try_delete_watch(
		&self,
		arg: tg::watch::delete::Arg,
	) -> BoxFuture<'_, tg::Result<Option<()>>> {
		self.try_delete_watch(arg).boxed()
	}

	fn touch_watch(&self, arg: tg::watch::touch::Arg) -> BoxFuture<'_, tg::Result<()>> {
		self.touch_watch(arg).boxed()
	}
}
