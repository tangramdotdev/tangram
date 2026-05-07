use crate::prelude::*;

impl tg::handle::Watch for tg::Session {
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
