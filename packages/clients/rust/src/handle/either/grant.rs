use {crate::prelude::*, futures::FutureExt as _};

impl<L, R> tg::handle::Grant for tg::Either<L, R>
where
	L: tg::handle::Grant,
	R: tg::handle::Grant,
{
	fn create_grant(
		&self,
		arg: tg::grant::create::Arg,
	) -> impl Future<Output = tg::Result<tg::Grant>> {
		match self {
			tg::Either::Left(s) => s.create_grant(arg).left_future(),
			tg::Either::Right(s) => s.create_grant(arg).right_future(),
		}
	}

	fn delete_grant(
		&self,
		arg: tg::grant::delete::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		match self {
			tg::Either::Left(s) => s.delete_grant(arg).left_future(),
			tg::Either::Right(s) => s.delete_grant(arg).right_future(),
		}
	}
}
