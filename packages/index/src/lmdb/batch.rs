use {
	super::{Index, Request},
	tangram_client::prelude::*,
};

impl Index {
	pub async fn batch(&self, arg: crate::batch::Arg) -> tg::Result<()> {
		if arg.is_empty() {
			return Ok(());
		}
		self.delete_grants(&arg.delete_grants).await?;
		self.delete_group_members(&arg.delete_group_members).await?;
		self.delete_organization_members(&arg.delete_organization_members)
			.await?;
		self.delete_groups(&arg.delete_groups).await?;
		self.delete_organizations(&arg.delete_organizations).await?;
		self.delete_tags(&arg.delete_tags).await?;
		self.delete_users(&arg.delete_users).await?;
		if !arg.put_cache_entries.is_empty() {
			self.enqueue_batch_request(Request::PutCacheEntries(arg.put_cache_entries))
				.await?;
		}
		self.put_groups(&arg.put_groups).await?;
		self.put_group_members(&arg.put_group_members).await?;
		if !arg.put_objects.is_empty() {
			self.enqueue_batch_request(Request::PutObjects(arg.put_objects))
				.await?;
		}
		self.put_organizations(&arg.put_organizations).await?;
		self.put_organization_members(&arg.put_organization_members)
			.await?;
		if !arg.put_processes.is_empty() {
			self.enqueue_batch_request(Request::PutProcesses(arg.put_processes))
				.await?;
		}
		if !arg.put_runners.is_empty() {
			self.enqueue_batch_request(Request::PutRunners(arg.put_runners))
				.await?;
		}
		if !arg.put_sandboxes.is_empty() {
			self.enqueue_batch_request(Request::PutSandboxes(arg.put_sandboxes))
				.await?;
		}
		self.put_tags(&arg.put_tags).await?;
		self.put_users(&arg.put_users).await?;
		self.put_grants(&arg.put_grants).await?;
		Ok(())
	}

	async fn enqueue_batch_request(&self, request: Request) -> tg::Result<()> {
		let (sender, receiver) = tokio::sync::oneshot::channel();
		self.sender_medium
			.as_ref()
			.unwrap()
			.send((request, sender))
			.map_err(|error| tg::error!(!error, "failed to send the request"))?;
		receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))??;
		Ok(())
	}
}
