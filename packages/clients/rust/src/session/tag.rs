use crate::prelude::*;

impl tg::handle::Tag for tg::Session {
	fn put_tag(
		&self,
		tag: &tg::Tag,
		arg: tg::tag::put::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.put_tag(tag, arg)
	}

	fn create_tag_grant(
		&self,
		arg: tg::tag::grants::create::Arg,
	) -> impl Future<Output = tg::Result<tg::TagGrant>> {
		self.create_tag_grant(arg)
	}

	fn post_tag_batch(
		&self,
		arg: tg::tag::batch::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send {
		self.post_tag_batch(arg)
	}

	fn list_tag_grants(
		&self,
		arg: tg::tag::grants::list::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::tag::grants::list::Output>>> {
		self.list_tag_grants(arg)
	}

	fn delete_tag_grant(
		&self,
		arg: tg::tag::grants::delete::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		self.delete_tag_grant(arg)
	}

	fn delete_tags(
		&self,
		arg: tg::tag::delete::Arg,
	) -> impl Future<Output = tg::Result<tg::tag::delete::Output>> {
		self.delete_tags(arg)
	}
}
