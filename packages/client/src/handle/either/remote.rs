use {crate::prelude::*, futures::FutureExt as _};

impl<L, R> tg::handle::Remote for tg::Either<L, R>
where
	L: tg::handle::Remote,
	R: tg::handle::Remote,
{
	fn list_remotes(
		&self,
		arg: tg::remote::list::Arg,
	) -> impl Future<Output = tg::Result<tg::remote::list::Output>> {
		match self {
			tg::Either::Left(s) => s.list_remotes(arg).left_future(),
			tg::Either::Right(s) => s.list_remotes(arg).right_future(),
		}
	}

	fn try_get_remote(
		&self,
		name: &str,
	) -> impl Future<Output = tg::Result<Option<tg::remote::get::Output>>> {
		match self {
			tg::Either::Left(s) => s.try_get_remote(name).left_future(),
			tg::Either::Right(s) => s.try_get_remote(name).right_future(),
		}
	}

	fn put_remote(
		&self,
		name: &str,
		arg: tg::remote::put::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		match self {
			tg::Either::Left(s) => s.put_remote(name, arg).left_future(),
			tg::Either::Right(s) => s.put_remote(name, arg).right_future(),
		}
	}

	fn delete_remote(&self, name: &str) -> impl Future<Output = tg::Result<()>> {
		match self {
			tg::Either::Left(s) => s.delete_remote(name).left_future(),
			tg::Either::Right(s) => s.delete_remote(name).right_future(),
		}
	}
}
