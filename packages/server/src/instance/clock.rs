use {crate::Server, tangram_client::prelude::*};

impl tg::instance::Clock for Server {
	async fn try_get_clock(&self) -> tg::Result<Option<tg::clock::get::Output>> {
		self.session(&self.context).try_get_clock().await
	}

	async fn try_set_clock(
		&self,
		arg: tg::clock::set::Arg,
	) -> tg::Result<Option<tg::clock::get::Output>> {
		self.session(&self.context).try_set_clock(arg).await
	}

	async fn try_advance_clock(
		&self,
		arg: tg::clock::advance::Arg,
	) -> tg::Result<Option<tg::clock::get::Output>> {
		self.session(&self.context).try_advance_clock(arg).await
	}
}
