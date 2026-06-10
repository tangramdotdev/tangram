use {
	crate::fdb::{Index, Request, Response},
	tangram_client::prelude::*,
};

impl Index {
	pub async fn delete_organizations(&self, ids: &[tg::organization::Id]) -> tg::Result<()> {
		if ids.is_empty() {
			return Ok(());
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::DeleteOrganizations(ids.to_vec());
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

	pub async fn delete_organization_members(
		&self,
		args: &[crate::organization::member::delete::Arg],
	) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::DeleteOrganizationMembers(args.to_vec());
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

	pub(crate) async fn task_delete_organizations(_ids: &[tg::organization::Id]) -> tg::Result<()> {
		Ok(())
	}

	pub(crate) async fn task_delete_organization_members(
		_args: &[crate::organization::member::delete::Arg],
	) -> tg::Result<()> {
		Ok(())
	}
}
