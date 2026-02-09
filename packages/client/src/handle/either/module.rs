use {crate::prelude::*, futures::FutureExt as _};

impl<L, R> tg::handle::Module for tg::Either<L, R>
where
	L: tg::handle::Module,
	R: tg::handle::Module,
{
	fn resolve_module(
		&self,
		arg: tg::module::resolve::Arg,
	) -> impl Future<Output = tg::Result<tg::module::resolve::Output>> {
		match self {
			tg::Either::Left(s) => s.resolve_module(arg).left_future(),
			tg::Either::Right(s) => s.resolve_module(arg).right_future(),
		}
	}

	fn load_module(
		&self,
		arg: tg::module::load::Arg,
	) -> impl Future<Output = tg::Result<tg::module::load::Output>> {
		match self {
			tg::Either::Left(s) => s.load_module(arg).left_future(),
			tg::Either::Right(s) => s.load_module(arg).right_future(),
		}
	}
}
