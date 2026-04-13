use crate::prelude::*;

pub trait Watch: Clone + Unpin + Send + Sync + 'static {
	fn list_watches(
		&self,
		arg: tg::watch::list::Arg,
	) -> impl Future<Output = tg::Result<tg::watch::list::Output>> + Send;

	fn delete_watch(
		&self,
		arg: tg::watch::delete::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send {
		async move {
			self.try_delete_watch(arg)
				.await?
				.ok_or_else(|| tg::error!("failed to find the watch"))
		}
	}

	fn try_delete_watch(
		&self,
		arg: tg::watch::delete::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> + Send;

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

	fn try_delete_watch(
		&self,
		arg: tg::watch::delete::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		self.try_delete_watch(arg)
	}

	fn touch_watch(
		&self,
		arg: crate::watch::touch::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send {
		self.touch_watch(arg)
	}
}
