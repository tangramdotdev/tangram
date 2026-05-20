use {super::Handle, crate::prelude::*, futures::future::BoxFuture};

impl tg::handle::Tag for Handle {
	fn put_tag(
		&self,
		tag: &tg::Tag,
		arg: tg::tag::put::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.put_tag(tag, arg)) }
	}

	fn create_tag_grant(
		&self,
		arg: tg::tag::grants::create::Arg,
	) -> impl Future<Output = tg::Result<tg::TagGrant>> {
		self.0.create_tag_grant(arg)
	}

	fn post_tag_batch(&self, arg: tg::tag::batch::Arg) -> impl Future<Output = tg::Result<()>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.post_tag_batch(arg)) }
	}

	fn list_tag_grants(
		&self,
		arg: tg::tag::grants::list::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::tag::grants::list::Output>>> {
		self.0.list_tag_grants(arg)
	}

	fn delete_tag_grant(
		&self,
		arg: tg::tag::grants::delete::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		self.0.delete_tag_grant(arg)
	}

	fn delete_tags(
		&self,
		arg: tg::tag::delete::Arg,
	) -> impl Future<Output = tg::Result<tg::tag::delete::Output>> {
		self.0.delete_tags(arg)
	}
}
