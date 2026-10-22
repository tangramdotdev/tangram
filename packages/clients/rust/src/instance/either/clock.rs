use {crate::prelude::*, futures::FutureExt as _};

impl<L, R> tg::instance::Clock for tg::Either<L, R>
where
	L: tg::instance::Clock,
	R: tg::instance::Clock,
{
	fn try_get_clock(&self) -> impl Future<Output = tg::Result<Option<tg::clock::get::Output>>> {
		match self {
			tg::Either::Left(instance) => instance.try_get_clock().left_future(),
			tg::Either::Right(instance) => instance.try_get_clock().right_future(),
		}
	}

	fn try_set_clock(
		&self,
		arg: tg::clock::set::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::clock::get::Output>>> {
		match self {
			tg::Either::Left(instance) => instance.try_set_clock(arg).left_future(),
			tg::Either::Right(instance) => instance.try_set_clock(arg).right_future(),
		}
	}

	fn try_advance_clock(
		&self,
		arg: tg::clock::advance::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::clock::get::Output>>> {
		match self {
			tg::Either::Left(instance) => instance.try_advance_clock(arg).left_future(),
			tg::Either::Right(instance) => instance.try_advance_clock(arg).right_future(),
		}
	}
}
