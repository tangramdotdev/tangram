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

	fn list_tag_grants(
		&self,
		arg: tg::tag::grants::list::Arg,
	) -> BoxFuture<'_, tg::Result<Option<tg::tag::grants::list::Output>>>;

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

	fn list_tag_grants(
		&self,
		arg: tg::tag::grants::list::Arg,
	) -> BoxFuture<'_, tg::Result<Option<tg::tag::grants::list::Output>>> {
		self.list_tag_grants(arg).boxed()
	}

	fn delete_tags(
		&self,
		arg: tg::tag::delete::Arg,
	) -> BoxFuture<'_, tg::Result<tg::tag::delete::Output>> {
		self.delete_tags(arg).boxed()
	}
}
