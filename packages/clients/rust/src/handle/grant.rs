use crate::prelude::*;

pub trait Grant: Clone + Unpin + Send + Sync + 'static {
	fn create_grant(
		&self,
		arg: tg::grant::create::Arg,
	) -> impl Future<Output = tg::Result<tg::Grant>> + Send;

	fn delete_grant(
		&self,
		arg: tg::grant::delete::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> + Send;

	fn list_grants(
		&self,
		arg: tg::grant::list::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::grant::list::Output>>> + Send;
}

impl tg::handle::Grant for tg::Client {
	async fn create_grant(&self, arg: tg::grant::create::Arg) -> tg::Result<tg::Grant> {
		self.session(&self.context).create_grant(arg).await
	}

	async fn delete_grant(&self, arg: tg::grant::delete::Arg) -> tg::Result<Option<()>> {
		self.session(&self.context).delete_grant(arg).await
	}

	async fn list_grants(
		&self,
		arg: tg::grant::list::Arg,
	) -> tg::Result<Option<tg::grant::list::Output>> {
		self.session(&self.context).list_grants(arg).await
	}
}
