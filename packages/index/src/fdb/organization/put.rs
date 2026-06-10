use {
	crate::fdb::{Index, Request, Response},
	tangram_client::prelude::*,
};

impl Index {
	pub async fn put_organizations(
		&self,
		args: &[crate::organization::put::Arg],
	) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::PutOrganizations(args.to_vec());
		self.sender_medium
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

	pub async fn put_organization_members(
		&self,
		args: &[crate::organization::member::put::Arg],
	) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::PutOrganizationMembers(args.to_vec());
		self.sender_medium
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

	pub(crate) async fn task_put_organizations(
		_args: &[crate::organization::put::Arg],
	) -> tg::Result<()> {
		Ok(())
	}

	pub(crate) async fn task_put_organization_members(
		_args: &[crate::organization::member::put::Arg],
	) -> tg::Result<()> {
		Ok(())
	}
}
