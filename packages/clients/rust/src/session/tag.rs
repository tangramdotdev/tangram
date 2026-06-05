use crate::prelude::*;

impl tg::handle::Tag for tg::Session {
	fn put_tag(&self, arg: tg::tag::put::Arg) -> impl Future<Output = tg::Result<()>> {
		self.put_tag(arg)
	}

	fn post_tag_batch(
		&self,
		arg: tg::tag::batch::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send {
		self.post_tag_batch(arg)
	}

	fn try_get_tag(
		&self,
		tag: &tg::tag::Selector,
	) -> impl Future<Output = tg::Result<Option<tg::tag::get::Output>>> {
		self.try_get_tag(tag)
	}

	fn try_get_tag_grants(
		&self,
		tag: &tg::tag::Selector,
		arg: tg::tag::grants::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::tag::grants::Output>>> {
		self.try_get_tag_grants(tag, arg)
	}

	fn delete_tags(
		&self,
		arg: tg::tag::delete::Arg,
	) -> impl Future<Output = tg::Result<tg::tag::delete::Output>> {
		self.delete_tags(arg)
	}
}
