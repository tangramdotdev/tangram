use {crate::prelude::*, futures::FutureExt as _};

impl<L, R> tg::handle::Watch for tg::Either<L, R>
where
	L: tg::handle::Watch,
	R: tg::handle::Watch,
{
	fn list_watches(
		&self,
		arg: tg::watch::list::Arg,
	) -> impl Future<Output = tg::Result<tg::watch::list::Output>> {
		match self {
			tg::Either::Left(s) => s.list_watches(arg).left_future(),
			tg::Either::Right(s) => s.list_watches(arg).right_future(),
		}
	}

	fn delete_watch(&self, arg: tg::watch::delete::Arg) -> impl Future<Output = tg::Result<()>> {
		match self {
			tg::Either::Left(s) => s.delete_watch(arg).left_future(),
			tg::Either::Right(s) => s.delete_watch(arg).right_future(),
		}
	}

	fn touch_watch(&self, arg: tg::watch::touch::Arg) -> impl Future<Output = tg::Result<()>> {
		match self {
			tg::Either::Left(s) => s.touch_watch(arg).left_future(),
			tg::Either::Right(s) => s.touch_watch(arg).right_future(),
		}
	}
}
