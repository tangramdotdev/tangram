use crate::prelude::*;

pub trait Tag: Clone + Unpin + Send + Sync + 'static {
	fn put_tag(&self, arg: tg::tag::put::Arg) -> impl Future<Output = tg::Result<()>> + Send;

	fn post_tag_batch(
		&self,
		arg: tg::tag::batch::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn try_get_tag(
		&self,
		tag: &tg::tag::Selector,
	) -> impl Future<Output = tg::Result<Option<tg::tag::get::Output>>> + Send;

	fn try_get_tag_grants(
		&self,
		tag: &tg::tag::Selector,
		arg: tg::tag::grants::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::tag::grants::Output>>> + Send;

	fn delete_tags(
		&self,
		arg: tg::tag::delete::Arg,
	) -> impl Future<Output = tg::Result<tg::tag::delete::Output>> + Send;
}

impl tg::handle::Tag for tg::Client {
	async fn put_tag(&self, arg: tg::tag::put::Arg) -> tg::Result<()> {
		self.session(&self.context).put_tag(arg).await
	}

	async fn post_tag_batch(&self, arg: tg::tag::batch::Arg) -> tg::Result<()> {
		self.session(&self.context).post_tag_batch(arg).await
	}

	async fn try_get_tag(
		&self,
		tag: &tg::tag::Selector,
	) -> tg::Result<Option<tg::tag::get::Output>> {
		self.session(&self.context).try_get_tag(tag).await
	}

	async fn try_get_tag_grants(
		&self,
		tag: &tg::tag::Selector,
		arg: tg::tag::grants::Arg,
	) -> tg::Result<Option<tg::tag::grants::Output>> {
		self.session(&self.context)
			.try_get_tag_grants(tag, arg)
			.await
	}

	async fn delete_tags(&self, arg: tg::tag::delete::Arg) -> tg::Result<tg::tag::delete::Output> {
		self.session(&self.context).delete_tags(arg).await
	}
}
