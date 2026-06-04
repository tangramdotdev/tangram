use {crate::Session, tangram_client::prelude::*};

impl tg::handle::Tag for Session {
	async fn put_tag(&self, arg: tg::tag::put::Arg) -> tg::Result<()> {
		self.put_tag(arg).await
	}

	async fn create_tag_grant(
		&self,
		arg: tg::tag::grants::create::Arg,
	) -> tg::Result<tg::TagGrant> {
		self.create_tag_grant(arg).await
	}

	async fn post_tag_batch(&self, arg: tg::tag::batch::Arg) -> tg::Result<()> {
		self.post_tag_batch(arg).await
	}

	async fn try_get_tag(
		&self,
		tag: &tg::tag::Selector,
	) -> tg::Result<Option<tg::tag::get::Output>> {
		self.try_get_tag(tag).await
	}

	async fn list_tag_grants(
		&self,
		arg: tg::tag::grants::list::Arg,
	) -> tg::Result<Option<tg::tag::grants::list::Output>> {
		self.list_tag_grants(arg).await
	}

	async fn delete_tag_grant(&self, arg: tg::tag::grants::delete::Arg) -> tg::Result<Option<()>> {
		self.delete_tag_grant(arg).await
	}

	async fn delete_tags(&self, arg: tg::tag::delete::Arg) -> tg::Result<tg::tag::delete::Output> {
		self.delete_tags(arg).await
	}
}
