#![allow(clippy::unnecessary_wraps)]

use {
	crate::lmdb::{Index, Request},
	tangram_client::prelude::*,
};

impl Index {
	pub async fn put_groups(&self, args: &[crate::group::put::Arg]) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::PutGroups(args.to_vec());
		self.sender_medium
			.send((request, sender))
			.map_err(|error| tg::error!(!error, "failed to send the request"))?;
		receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))??;
		Ok(())
	}

	pub async fn put_group_members(
		&self,
		args: &[crate::group::member::put::Arg],
	) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::PutGroupMembers(args.to_vec());
		self.sender_medium
			.send((request, sender))
			.map_err(|error| tg::error!(!error, "failed to send the request"))?;
		receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))??;
		Ok(())
	}

	pub(crate) fn task_put_groups(_args: &[crate::group::put::Arg]) -> tg::Result<()> {
		Ok(())
	}

	pub(crate) fn task_put_group_members(
		_args: &[crate::group::member::put::Arg],
	) -> tg::Result<()> {
		Ok(())
	}
}
