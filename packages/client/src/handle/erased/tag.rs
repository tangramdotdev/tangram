use {
	crate::prelude::*,
	futures::{future::BoxFuture, prelude::*},
};

pub trait Tag: Send + Sync + 'static {
	fn list_tags(
		&self,
		arg: tg::tag::list::Arg,
	) -> BoxFuture<'_, tg::Result<tg::tag::list::Output>>;

	fn try_get_tag<'a>(
		&'a self,
		tag: &'a tg::Tag,
		arg: tg::tag::get::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::tag::get::Output>>>;

	fn put_tag<'a>(
		&'a self,
		tag: &'a tg::Tag,
		arg: tg::tag::put::Arg,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn post_tag_batch(&self, arg: tg::tag::batch::Arg) -> BoxFuture<'_, tg::Result<()>>;

	fn delete_tag(
		&self,
		arg: tg::tag::delete::Arg,
	) -> BoxFuture<'_, tg::Result<tg::tag::delete::Output>>;
}

impl<T> Tag for T
where
	T: tg::handle::Tag,
{
	fn list_tags(
		&self,
		arg: tg::tag::list::Arg,
	) -> BoxFuture<'_, tg::Result<tg::tag::list::Output>> {
		self.list_tags(arg).boxed()
	}

	fn try_get_tag<'a>(
		&'a self,
		tag: &'a tg::Tag,
		arg: tg::tag::get::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::tag::get::Output>>> {
		self.try_get_tag(tag, arg).boxed()
	}

	fn put_tag<'a>(
		&'a self,
		tag: &'a tg::Tag,
		arg: tg::tag::put::Arg,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.put_tag(tag, arg).boxed()
	}

	fn post_tag_batch(&self, arg: tg::tag::batch::Arg) -> BoxFuture<'_, tg::Result<()>> {
		self.post_tag_batch(arg).boxed()
	}

	fn delete_tag(
		&self,
		arg: tg::tag::delete::Arg,
	) -> BoxFuture<'_, tg::Result<tg::tag::delete::Output>> {
		self.delete_tag(arg).boxed()
	}
}
