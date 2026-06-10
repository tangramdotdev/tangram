use {
	crate::fdb::{Index, Request, Response},
	tangram_client::prelude::*,
};

impl Index {
	pub async fn delete_users(&self, ids: &[tg::user::Id]) -> tg::Result<()> {
		if ids.is_empty() {
			return Ok(());
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::DeleteUsers(ids.to_vec());
		self.sender_high
			.send((request, sender))
			.map_err(|error| tg::error!(!error, "failed to send the request"))?;
		let response = receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))??;
		let Response::Unit = response else {
			return Err(tg::error!("unexpected response"));
		};
		Ok(())
	}

	pub(crate) async fn task_delete_users(_ids: &[tg::user::Id]) -> tg::Result<()> {
		Ok(())
	}
}
