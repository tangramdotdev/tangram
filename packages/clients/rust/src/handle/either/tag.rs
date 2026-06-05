use {crate::prelude::*, futures::FutureExt as _};

impl<L, R> tg::handle::Tag for tg::Either<L, R>
where
	L: tg::handle::Tag,
	R: tg::handle::Tag,
{
	fn put_tag(&self, arg: tg::tag::put::Arg) -> impl Future<Output = tg::Result<()>> {
		match self {
			tg::Either::Left(s) => s.put_tag(arg).left_future(),
			tg::Either::Right(s) => s.put_tag(arg).right_future(),
		}
	}

	fn post_tag_batch(&self, arg: tg::tag::batch::Arg) -> impl Future<Output = tg::Result<()>> {
		match self {
			tg::Either::Left(s) => s.post_tag_batch(arg).left_future(),
			tg::Either::Right(s) => s.post_tag_batch(arg).right_future(),
		}
	}

	fn try_get_tag(
		&self,
		tag: &tg::tag::Selector,
	) -> impl Future<Output = tg::Result<Option<tg::tag::get::Output>>> {
		match self {
			tg::Either::Left(s) => s.try_get_tag(tag).left_future(),
			tg::Either::Right(s) => s.try_get_tag(tag).right_future(),
		}
	}

	fn try_get_tag_grants(
		&self,
		tag: &tg::tag::Selector,
		arg: tg::tag::grants::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::tag::grants::Output>>> {
		match self {
			tg::Either::Left(s) => s.try_get_tag_grants(tag, arg).left_future(),
			tg::Either::Right(s) => s.try_get_tag_grants(tag, arg).right_future(),
		}
	}

	fn delete_tags(
		&self,
		arg: tg::tag::delete::Arg,
	) -> impl Future<Output = tg::Result<tg::tag::delete::Output>> {
		match self {
			tg::Either::Left(s) => s.delete_tags(arg).left_future(),
			tg::Either::Right(s) => s.delete_tags(arg).right_future(),
		}
	}
}
