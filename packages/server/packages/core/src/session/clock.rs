use {crate::Session, tangram_client::prelude::*};

impl tg::instance::Clock for Session {
	async fn try_get_clock(&self) -> tg::Result<Option<tg::clock::get::Output>> {
		self.server.components.clock.try_get_clock(self).await
	}

	async fn try_set_clock(
		&self,
		arg: tg::clock::set::Arg,
	) -> tg::Result<Option<tg::clock::get::Output>> {
		self.server.components.clock.try_set_clock(self, arg).await
	}

	async fn try_advance_clock(
		&self,
		arg: tg::clock::advance::Arg,
	) -> tg::Result<Option<tg::clock::get::Output>> {
		self.server
			.components
			.clock
			.try_advance_clock(self, arg)
			.await
	}
}
