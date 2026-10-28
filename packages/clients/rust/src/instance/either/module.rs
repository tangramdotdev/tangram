use {crate::prelude::*, futures::FutureExt as _};

impl<L, R> tg::instance::Module for tg::Either<L, R>
where
	L: tg::instance::Module,
	R: tg::instance::Module,
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

	fn module_interface(
		&self,
		arg: tg::module::interface::Arg,
	) -> impl Future<Output = tg::Result<tg::module::interface::Output>> {
		match self {
			tg::Either::Left(s) => s.module_interface(arg).left_future(),
			tg::Either::Right(s) => s.module_interface(arg).right_future(),
		}
	}
}
