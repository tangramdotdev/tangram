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

	fn create_tag_grant(
		&self,
		arg: tg::tag::grants::create::Arg,
	) -> BoxFuture<'_, tg::Result<tg::TagGrant>>;

	fn post_tag_batch(&self, arg: tg::tag::batch::Arg) -> BoxFuture<'_, tg::Result<()>>;

	fn list_tag_grants(
		&self,
		arg: tg::tag::grants::list::Arg,
	) -> BoxFuture<'_, tg::Result<Option<tg::tag::grants::list::Output>>>;

	fn delete_tag_grant(
		&self,
		arg: tg::tag::grants::delete::Arg,
	) -> BoxFuture<'_, tg::Result<Option<()>>>;

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

	fn create_tag_grant(
		&self,
		arg: tg::tag::grants::create::Arg,
	) -> BoxFuture<'_, tg::Result<tg::TagGrant>> {
		self.create_tag_grant(arg).boxed()
	}

	fn post_tag_batch(&self, arg: tg::tag::batch::Arg) -> BoxFuture<'_, tg::Result<()>> {
		self.post_tag_batch(arg).boxed()
	}

	fn list_tag_grants(
		&self,
		arg: tg::tag::grants::list::Arg,
	) -> BoxFuture<'_, tg::Result<Option<tg::tag::grants::list::Output>>> {
		self.list_tag_grants(arg).boxed()
	}

	fn delete_tag_grant(
		&self,
		arg: tg::tag::grants::delete::Arg,
	) -> BoxFuture<'_, tg::Result<Option<()>>> {
		self.delete_tag_grant(arg).boxed()
	}

	fn delete_tags(
		&self,
		arg: tg::tag::delete::Arg,
	) -> BoxFuture<'_, tg::Result<tg::tag::delete::Output>> {
		self.delete_tags(arg).boxed()
	}
}
