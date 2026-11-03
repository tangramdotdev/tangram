use {crate::Server, tangram_client::prelude::*};

impl tg::instance::Grant for Server {
	async fn grant(&self, arg: tg::grant::grant::Arg) -> tg::Result<tg::grant::grant::Output> {
		self.session(&self.context).grant(arg).await
	}

	async fn revoke(&self, arg: tg::grant::revoke::Arg) -> tg::Result<()> {
		self.session(&self.context).revoke(arg).await
	}

	async fn try_delete_grant(
		&self,
		grant: &tg::grant::Id,
		arg: tg::grant::delete::Arg,
	) -> tg::Result<Option<()>> {
		self.session(&self.context)
			.try_delete_grant(grant, arg)
			.await
	}

	async fn try_get_grant(
		&self,
		grant: &tg::grant::Id,
		arg: tg::grant::get::Arg,
	) -> tg::Result<Option<tg::grant::get::Output>> {
		self.session(&self.context).try_get_grant(grant, arg).await
	}

	async fn list_grants(
		&self,
		arg: tg::grant::list::Arg,
	) -> tg::Result<Option<tg::grant::list::Output>> {
		self.session(&self.context).list_grants(arg).await
	}
}
