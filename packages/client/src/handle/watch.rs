use crate::prelude::*;

pub trait Watch: Clone + Unpin + Send + Sync + 'static {
	fn list_watches(
		&self,
		arg: tg::watch::list::Arg,
	) -> impl Future<Output = tg::Result<tg::watch::list::Output>> + Send;

	fn delete_watch(
		&self,
		arg: tg::watch::delete::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn touch_watch(
		&self,
		arg: tg::watch::touch::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;
}

impl tg::handle::Watch for tg::Client {
	fn list_watches(
		&self,
		arg: tg::watch::list::Arg,
	) -> impl Future<Output = tg::Result<tg::watch::list::Output>> {
		self.list_watches(arg)
	}

	fn delete_watch(&self, arg: tg::watch::delete::Arg) -> impl Future<Output = tg::Result<()>> {
		self.delete_watch(arg)
	}

	fn touch_watch(
		&self,
		arg: crate::watch::touch::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send {
		self.touch_watch(arg)
	}
}
