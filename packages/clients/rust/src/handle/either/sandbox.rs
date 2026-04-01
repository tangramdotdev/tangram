use {crate::prelude::*, futures::FutureExt as _};

impl<L, R> tg::handle::Sandbox for tg::Either<L, R>
where
	L: tg::handle::Sandbox,
	R: tg::handle::Sandbox,
{
	fn create_sandbox(
		&self,
		arg: tg::sandbox::create::Arg,
	) -> impl Future<Output = tg::Result<tg::sandbox::create::Output>> {
		match self {
			tg::Either::Left(s) => s.create_sandbox(arg).left_future(),
			tg::Either::Right(s) => s.create_sandbox(arg).right_future(),
		}
	}

	fn list_sandboxes(
		&self,
		arg: tg::sandbox::list::Arg,
	) -> impl Future<Output = tg::Result<tg::sandbox::list::Output>> {
		match self {
			tg::Either::Left(s) => s.list_sandboxes(arg).left_future(),
			tg::Either::Right(s) => s.list_sandboxes(arg).right_future(),
		}
	}

	fn delete_sandbox(&self, id: &tg::sandbox::Id) -> impl Future<Output = tg::Result<()>> {
		match self {
			tg::Either::Left(s) => s.delete_sandbox(id).left_future(),
			tg::Either::Right(s) => s.delete_sandbox(id).right_future(),
		}
	}
}
