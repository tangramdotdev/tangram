use {crate::prelude::*, futures::FutureExt as _};

impl<L, R> tg::handle::Object for tg::Either<L, R>
where
	L: tg::handle::Object,
	R: tg::handle::Object,
{
	fn try_get_object_metadata(
		&self,
		id: &tg::object::Id,
		arg: tg::object::metadata::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::object::Metadata>>> {
		match self {
			tg::Either::Left(s) => s.try_get_object_metadata(id, arg).left_future(),
			tg::Either::Right(s) => s.try_get_object_metadata(id, arg).right_future(),
		}
	}

	fn try_get_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::get::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::object::get::Output>>> {
		match self {
			tg::Either::Left(s) => s.try_get_object(id, arg).left_future(),
			tg::Either::Right(s) => s.try_get_object(id, arg).right_future(),
		}
	}

	fn put_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::put::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		match self {
			tg::Either::Left(s) => s.put_object(id, arg).left_future(),
			tg::Either::Right(s) => s.put_object(id, arg).right_future(),
		}
	}

	fn post_object_batch(
		&self,
		arg: tg::object::batch::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		match self {
			tg::Either::Left(s) => s.post_object_batch(arg).left_future(),
			tg::Either::Right(s) => s.post_object_batch(arg).right_future(),
		}
	}

	fn touch_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::touch::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		match self {
			tg::Either::Left(s) => s.touch_object(id, arg).left_future(),
			tg::Either::Right(s) => s.touch_object(id, arg).right_future(),
		}
	}
}
