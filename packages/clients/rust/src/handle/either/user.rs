use {crate::prelude::*, futures::FutureExt as _};

impl<L, R> tg::handle::User for tg::Either<L, R>
where
	L: tg::handle::User,
	R: tg::handle::User,
{
	fn get_user(&self, token: &str) -> impl Future<Output = tg::Result<Option<tg::User>>> {
		match self {
			tg::Either::Left(s) => s.get_user(token).left_future(),
			tg::Either::Right(s) => s.get_user(token).right_future(),
		}
	}
}
