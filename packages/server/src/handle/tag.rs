use {
	crate::{Context, Server, Shared},
	tangram_client::prelude::*,
};

impl tg::handle::Tag for Shared {
	async fn list_tags(&self, arg: tg::tag::list::Arg) -> tg::Result<tg::tag::list::Output> {
		self.0.list_tags(arg).await
	}

	async fn put_tag(&self, tag: &tg::Tag, arg: tg::tag::put::Arg) -> tg::Result<()> {
		self.0.put_tag(tag, arg).await
	}

	async fn post_tag_batch(&self, arg: tg::tag::batch::Arg) -> tg::Result<()> {
		self.0.post_tag_batch(arg).await
	}

	async fn delete_tags(&self, arg: tg::tag::delete::Arg) -> tg::Result<tg::tag::delete::Output> {
		self.0.delete_tags(arg).await
	}
}

impl tg::handle::Tag for Server {
	async fn list_tags(&self, arg: tg::tag::list::Arg) -> tg::Result<tg::tag::list::Output> {
		self.list_tags_with_context(&Context::default(), arg).await
	}

	async fn put_tag(&self, tag: &tg::Tag, arg: tg::tag::put::Arg) -> tg::Result<()> {
		self.put_tag_with_context(&Context::default(), tag, arg)
			.await
	}

	async fn post_tag_batch(&self, arg: tg::tag::batch::Arg) -> tg::Result<()> {
		self.post_tag_batch_with_context(&Context::default(), arg)
			.await
	}

	async fn delete_tags(&self, arg: tg::tag::delete::Arg) -> tg::Result<tg::tag::delete::Output> {
		self.delete_tags_with_context(&Context::default(), arg)
			.await
	}
}
