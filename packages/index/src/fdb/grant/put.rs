use {
	crate::fdb::{Index, Request, Response},
	tangram_client::prelude::*,
};

impl Index {
	pub async fn put_grants(&self, args: &[crate::grant::put::Arg]) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::PutGrants(args.to_vec());
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

	pub(crate) async fn task_put_grants(_args: &[crate::grant::put::Arg]) -> tg::Result<()> {
		Ok(())
	}
}
