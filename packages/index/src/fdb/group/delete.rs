use {
	crate::fdb::{Index, Request, Response},
	tangram_client::prelude::*,
};

impl Index {
	pub async fn delete_groups(&self, ids: &[tg::group::Id]) -> tg::Result<()> {
		if ids.is_empty() {
			return Ok(());
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::DeleteGroups(ids.to_vec());
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

	pub async fn delete_group_members(
		&self,
		args: &[crate::group::member::delete::Arg],
	) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::DeleteGroupMembers(args.to_vec());
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

	pub(crate) async fn task_delete_groups(_ids: &[tg::group::Id]) -> tg::Result<()> {
		Ok(())
	}

	pub(crate) async fn task_delete_group_members(
		_args: &[crate::group::member::delete::Arg],
	) -> tg::Result<()> {
		Ok(())
	}
}
