use {crate::Session, tangram_client::prelude::*};

impl tg::instance::Clock for Session {
	async fn try_get_clock(&self) -> tg::Result<Option<tg::clock::get::Output>> {
		self.try_get_clock().await
	}

	async fn try_set_clock(
		&self,
		arg: tg::clock::set::Arg,
	) -> tg::Result<Option<tg::clock::get::Output>> {
		self.try_set_clock(arg).await
	}

	async fn try_advance_clock(
		&self,
		arg: tg::clock::advance::Arg,
	) -> tg::Result<Option<tg::clock::get::Output>> {
		self.try_advance_clock(arg).await
	}
}
