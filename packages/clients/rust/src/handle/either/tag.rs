use {crate::prelude::*, futures::FutureExt as _};

impl<L, R> tg::handle::Tag for tg::Either<L, R>
where
	L: tg::handle::Tag,
	R: tg::handle::Tag,
{
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

	fn create_tag_grant(
		&self,
		arg: tg::tag::grants::create::Arg,
	) -> impl Future<Output = tg::Result<tg::TagGrant>> {
		match self {
			tg::Either::Left(s) => s.create_tag_grant(arg).left_future(),
			tg::Either::Right(s) => s.create_tag_grant(arg).right_future(),
		}
	}

	fn post_tag_batch(&self, arg: tg::tag::batch::Arg) -> impl Future<Output = tg::Result<()>> {
		match self {
			tg::Either::Left(s) => s.post_tag_batch(arg).left_future(),
			tg::Either::Right(s) => s.post_tag_batch(arg).right_future(),
		}
	}

	fn list_tag_grants(
		&self,
		arg: tg::tag::grants::list::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::tag::grants::list::Output>>> {
		match self {
			tg::Either::Left(s) => s.list_tag_grants(arg).left_future(),
			tg::Either::Right(s) => s.list_tag_grants(arg).right_future(),
		}
	}

	fn delete_tag_grant(
		&self,
		arg: tg::tag::grants::delete::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		match self {
			tg::Either::Left(s) => s.delete_tag_grant(arg).left_future(),
			tg::Either::Right(s) => s.delete_tag_grant(arg).right_future(),
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
