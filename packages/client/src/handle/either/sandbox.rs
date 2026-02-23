use {
	crate::prelude::*,
	futures::{FutureExt as _, TryFutureExt as _},
};

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

	fn delete_sandbox(&self, id: &tg::sandbox::Id) -> impl Future<Output = tg::Result<()>> {
		match self {
			tg::Either::Left(s) => s.delete_sandbox(id).left_future(),
			tg::Either::Right(s) => s.delete_sandbox(id).right_future(),
		}
	}

	fn sandbox_spawn(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::spawn::Arg,
	) -> impl Future<Output = tg::Result<tg::sandbox::spawn::Output>> {
		match self {
			tg::Either::Left(s) => s.sandbox_spawn(id, arg).left_future(),
			tg::Either::Right(s) => s.sandbox_spawn(id, arg).right_future(),
		}
	}

	fn try_sandbox_wait_future(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::wait::Arg,
	) -> impl Future<
		Output = tg::Result<
			Option<
				impl Future<Output = tg::Result<Option<tg::sandbox::wait::Output>>> + Send + 'static,
			>,
		>,
	> {
		match self {
			tg::Either::Left(s) => s
				.try_sandbox_wait_future(id, arg)
				.map_ok(|option| option.map(futures::FutureExt::left_future))
				.left_future(),
			tg::Either::Right(s) => s
				.try_sandbox_wait_future(id, arg)
				.map_ok(|option| option.map(futures::FutureExt::right_future))
				.right_future(),
		}
	}
}
