use {
	super::ServerWithContext,
	crate::{Context, Owned, Server},
	tangram_client::prelude::*,
};

impl tg::handle::Tag for Owned {
	async fn list_tags(&self, arg: tg::tag::list::Arg) -> tg::Result<tg::tag::list::Output> {
		self.0.list_tags(arg).await
	}

	async fn put_tag(&self, tag: &tg::Tag, arg: tg::tag::put::Arg) -> tg::Result<()> {
		self.0.put_tag(tag, arg).await
	}

	async fn post_tag_batch(&self, arg: tg::tag::batch::Arg) -> tg::Result<()> {
		self.0.post_tag_batch(arg).await
	}

	async fn delete_tag(&self, arg: tg::tag::delete::Arg) -> tg::Result<tg::tag::delete::Output> {
		self.0.delete_tag(arg).await
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

	async fn delete_tag(&self, arg: tg::tag::delete::Arg) -> tg::Result<tg::tag::delete::Output> {
		self.delete_tag_with_context(&Context::default(), arg).await
	}
}

impl tg::handle::Tag for ServerWithContext {
	async fn list_tags(&self, arg: tg::tag::list::Arg) -> tg::Result<tg::tag::list::Output> {
		self.0.list_tags_with_context(&self.1, arg).await
	}

	async fn put_tag(&self, tag: &tg::Tag, arg: tg::tag::put::Arg) -> tg::Result<()> {
		self.0.put_tag_with_context(&self.1, tag, arg).await
	}

	async fn post_tag_batch(&self, arg: tg::tag::batch::Arg) -> tg::Result<()> {
		self.0.post_tag_batch_with_context(&self.1, arg).await
	}

	async fn delete_tag(&self, arg: tg::tag::delete::Arg) -> tg::Result<tg::tag::delete::Output> {
		self.0.delete_tag_with_context(&self.1, arg).await
	}
}
