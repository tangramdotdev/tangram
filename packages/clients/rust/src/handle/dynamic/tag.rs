use {super::Handle, crate::prelude::*, futures::future::BoxFuture};

impl tg::handle::Tag for Handle {
	fn put_tag(&self, arg: tg::tag::put::Arg) -> impl Future<Output = tg::Result<()>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.put_tag(arg)) }
	}

	fn post_tag_batch(&self, arg: tg::tag::batch::Arg) -> impl Future<Output = tg::Result<()>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.post_tag_batch(arg)) }
	}

	fn try_get_tag(
		&self,
		tag: &tg::tag::Selector,
	) -> impl Future<Output = tg::Result<Option<tg::tag::get::Output>>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.try_get_tag(tag)) }
	}

	fn delete_tags(
		&self,
		arg: tg::tag::delete::Arg,
	) -> impl Future<Output = tg::Result<tg::tag::delete::Output>> {
		self.0.delete_tags(arg)
	}
}
