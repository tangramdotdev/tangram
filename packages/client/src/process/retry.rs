use crate as tg;

impl tg::Process {
	pub async fn retry<H>(&self, handle: &H) -> tg::Result<bool>
	where
		H: tg::Handle,
	{
		self.try_get_retry(handle)
			.await?
			.ok_or_else(|| tg::error!("failed to get the process"))
	}

	pub async fn try_get_retry<H>(&self, handle: &H) -> tg::Result<Option<bool>>
	where
		H: tg::Handle,
	{
		let Some(output) = handle.try_get_process(&self.id).await? else {
			return Ok(None);
		};
		Ok(Some(output.retry))
	}
}
