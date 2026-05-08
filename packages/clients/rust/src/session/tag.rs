use crate::prelude::*;

impl tg::handle::Tag for tg::Session {
	fn put_tag(
		&self,
		tag: &tg::Tag,
		arg: tg::tag::put::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.put_tag(tag, arg)
	}

	fn post_tag_batch(
		&self,
		arg: tg::tag::batch::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send {
		self.post_tag_batch(arg)
	}

	fn delete_tags(
		&self,
		arg: tg::tag::delete::Arg,
	) -> impl Future<Output = tg::Result<tg::tag::delete::Output>> {
		self.delete_tags(arg)
	}
}
