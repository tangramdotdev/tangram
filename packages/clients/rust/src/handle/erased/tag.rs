use {
	crate::prelude::*,
	futures::{future::BoxFuture, prelude::*},
};

pub trait Tag: Send + Sync + 'static {
	fn put_tag(&self, arg: tg::tag::put::Arg) -> BoxFuture<'_, tg::Result<()>>;

	fn post_tag_batch(&self, arg: tg::tag::batch::Arg) -> BoxFuture<'_, tg::Result<()>>;

	fn try_get_tag<'a>(
		&'a self,
		tag: &'a tg::tag::Selector,
	) -> BoxFuture<'a, tg::Result<Option<tg::tag::get::Output>>>;

	fn try_get_tag_grants<'a>(
		&'a self,
		tag: &'a tg::tag::Selector,
		arg: tg::tag::grants::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::tag::grants::Output>>>;

	fn delete_tags(
		&self,
		arg: tg::tag::delete::Arg,
	) -> BoxFuture<'_, tg::Result<tg::tag::delete::Output>>;
}

impl<T> Tag for T
where
	T: tg::handle::Tag,
{
	fn put_tag(&self, arg: tg::tag::put::Arg) -> BoxFuture<'_, tg::Result<()>> {
		self.put_tag(arg).boxed()
	}

	fn post_tag_batch(&self, arg: tg::tag::batch::Arg) -> BoxFuture<'_, tg::Result<()>> {
		self.post_tag_batch(arg).boxed()
	}

	fn try_get_tag<'a>(
		&'a self,
		tag: &'a tg::tag::Selector,
	) -> BoxFuture<'a, tg::Result<Option<tg::tag::get::Output>>> {
		self.try_get_tag(tag).boxed()
	}

	fn try_get_tag_grants<'a>(
		&'a self,
		tag: &'a tg::tag::Selector,
		arg: tg::tag::grants::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::tag::grants::Output>>> {
		self.try_get_tag_grants(tag, arg).boxed()
	}

	fn delete_tags(
		&self,
		arg: tg::tag::delete::Arg,
	) -> BoxFuture<'_, tg::Result<tg::tag::delete::Output>> {
		self.delete_tags(arg).boxed()
	}
}
