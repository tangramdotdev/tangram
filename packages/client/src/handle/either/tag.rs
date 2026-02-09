use {crate::prelude::*, futures::FutureExt as _};

impl<L, R> tg::handle::Tag for tg::Either<L, R>
where
	L: tg::handle::Tag,
	R: tg::handle::Tag,
{
	fn list_tags(
		&self,
		arg: tg::tag::list::Arg,
	) -> impl Future<Output = tg::Result<tg::tag::list::Output>> {
		match self {
			tg::Either::Left(s) => s.list_tags(arg).left_future(),
			tg::Either::Right(s) => s.list_tags(arg).right_future(),
		}
	}

	fn try_get_tag(
		&self,
		tag: &tg::Tag,
		arg: tg::tag::get::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::tag::get::Output>>> {
		match self {
			tg::Either::Left(s) => s.try_get_tag(tag, arg.clone()).left_future(),
			tg::Either::Right(s) => s.try_get_tag(tag, arg).right_future(),
		}
	}

	fn put_tag(
		&self,
		tag: &tg::Tag,
		arg: tg::tag::put::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		match self {
			tg::Either::Left(s) => s.put_tag(tag, arg).left_future(),
			tg::Either::Right(s) => s.put_tag(tag, arg).right_future(),
		}
	}

	fn post_tag_batch(&self, arg: tg::tag::batch::Arg) -> impl Future<Output = tg::Result<()>> {
		match self {
			tg::Either::Left(s) => s.post_tag_batch(arg).left_future(),
			tg::Either::Right(s) => s.post_tag_batch(arg).right_future(),
		}
	}

	fn delete_tag(
		&self,
		arg: tg::tag::delete::Arg,
	) -> impl Future<Output = tg::Result<tg::tag::delete::Output>> {
		match self {
			tg::Either::Left(s) => s.delete_tag(arg).left_future(),
			tg::Either::Right(s) => s.delete_tag(arg).right_future(),
		}
	}
}
