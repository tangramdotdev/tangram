use {crate::Server, tangram_client::prelude::*};

impl tg::handle::Tag for Server {
	async fn put_tag(&self, tag: &tg::Tag, arg: tg::tag::put::Arg) -> tg::Result<()> {
		self.session(&self.context).put_tag(tag, arg).await
	}

	async fn post_tag_batch(&self, arg: tg::tag::batch::Arg) -> tg::Result<()> {
		self.session(&self.context).post_tag_batch(arg).await
	}

	async fn delete_tags(&self, arg: tg::tag::delete::Arg) -> tg::Result<tg::tag::delete::Output> {
		self.session(&self.context).delete_tags(arg).await
	}
}
