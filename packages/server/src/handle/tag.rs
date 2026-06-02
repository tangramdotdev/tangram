use {crate::Server, tangram_client::prelude::*};

impl tg::handle::Tag for Server {
	async fn put_tag(&self, tag: &tg::Tag, arg: tg::tag::put::Arg) -> tg::Result<()> {
		// self.session(&self.context).put_tag(tag, arg).await
		Err(tg::error!("todo"))
	}

	async fn create_tag_grant(
		&self,
		arg: tg::tag::grants::create::Arg,
	) -> tg::Result<tg::TagGrant> {
		// self.session(&self.context).create_tag_grant(arg).await
		Err(tg::error!("todo"))
	}

	async fn post_tag_batch(&self, arg: tg::tag::batch::Arg) -> tg::Result<()> {
		// self.session(&self.context).post_tag_batch(arg).await
		Err(tg::error!("todo"))
	}

	async fn list_tag_grants(
		&self,
		arg: tg::tag::grants::list::Arg,
	) -> tg::Result<Option<tg::tag::grants::list::Output>> {
		// self.session(&self.context).list_tag_grants(arg).await
		Err(tg::error!("todo"))
	}

	async fn delete_tag_grant(&self, arg: tg::tag::grants::delete::Arg) -> tg::Result<Option<()>> {
		// self.session(&self.context).delete_tag_grant(arg).await
		Err(tg::error!("todo"))
	}

	async fn delete_tags(&self, arg: tg::tag::delete::Arg) -> tg::Result<tg::tag::delete::Output> {
		// self.session(&self.context).delete_tags(arg).await
		Err(tg::error!("todo"))
	}
}
