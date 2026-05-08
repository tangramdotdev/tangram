use {
	crate::prelude::*,
	futures::{future::BoxFuture, prelude::*},
};

pub trait Tag: Send + Sync + 'static {
	fn put_tag<'a>(
		&'a self,
		tag: &'a tg::Tag,
		arg: tg::tag::put::Arg,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn post_tag_batch(&self, arg: tg::tag::batch::Arg) -> BoxFuture<'_, tg::Result<()>>;

	fn delete_tags(
		&self,
		arg: tg::tag::delete::Arg,
	) -> BoxFuture<'_, tg::Result<tg::tag::delete::Output>>;
}

impl<T> Tag for T
where
	T: tg::handle::Tag,
{
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

	fn delete_tags(
		&self,
		arg: tg::tag::delete::Arg,
	) -> BoxFuture<'_, tg::Result<tg::tag::delete::Output>> {
		self.delete_tags(arg).boxed()
	}
}
