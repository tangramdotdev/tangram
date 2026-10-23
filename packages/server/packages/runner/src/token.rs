use {crate::Component, tangram_client::prelude::*, tangram_server_core::Session};

pub mod create;
pub mod delete;
pub mod list;

impl Component {
	pub(super) async fn get_authorized_runner(
		&self,
		session: &Session,
		runner: &tg::runner::Id,
	) -> tg::Result<tg::runner::Data> {
		let data = self
			.try_get_runner_data(session, runner)
			.await?
			.ok_or_else(|| tg::error!("failed to find the runner"))?;
		let owner = data.owner.as_ref().and_then(tg::Principal::to_id);
		self.authorize_runner_owner(session, owner.as_ref()).await?;

		Ok(data)
	}
}
