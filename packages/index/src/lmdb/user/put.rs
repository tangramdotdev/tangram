#![allow(clippy::unnecessary_wraps)]

use {
	crate::lmdb::{Index, Request},
	tangram_client::prelude::*,
};

impl Index {
	pub async fn put_users(&self, args: &[crate::user::put::Arg]) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::PutUsers(args.to_vec());
		self.sender_medium
			.send((request, sender))
			.map_err(|error| tg::error!(!error, "failed to send the request"))?;
		receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))??;
		Ok(())
	}

	pub(crate) fn task_put_users(_args: &[crate::user::put::Arg]) -> tg::Result<()> {
		Ok(())
	}
}
