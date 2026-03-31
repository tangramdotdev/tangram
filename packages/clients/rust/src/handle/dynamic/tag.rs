use {super::Handle, crate::prelude::*, futures::future::BoxFuture};

impl tg::handle::Tag for Handle {
	fn list_tags(
		&self,
		arg: tg::tag::list::Arg,
	) -> impl Future<Output = tg::Result<tg::tag::list::Output>> {
		self.0.list_tags(arg)
	}

	fn put_tag(
		&self,
		tag: &tg::Tag,
		arg: tg::tag::put::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.put_tag(tag, arg)) }
	}

	fn post_tag_batch(&self, arg: tg::tag::batch::Arg) -> impl Future<Output = tg::Result<()>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.post_tag_batch(arg)) }
	}

	fn delete_tag(
		&self,
		arg: tg::tag::delete::Arg,
	) -> impl Future<Output = tg::Result<tg::tag::delete::Output>> {
		self.0.delete_tag(arg)
	}
}
