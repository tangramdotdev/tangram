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
}

impl tg::handle::Grant for tg::Client {
	async fn create_grant(&self, arg: tg::grant::create::Arg) -> tg::Result<tg::Grant> {
		self.session(&self.context).create_grant(arg).await
	}

	async fn delete_grant(&self, arg: tg::grant::delete::Arg) -> tg::Result<Option<()>> {
		self.session(&self.context).delete_grant(arg).await
	}
}
