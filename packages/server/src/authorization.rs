use {
	crate::Session, tangram_client::prelude::*, tangram_futures::stream::TryExt,
	tangram_index::prelude::*,
};

impl Session {
	pub(crate) async fn authorize(
		&self,
		resource: tg::grant::Resource,
		permission: tg::grant::Permission,
	) -> tg::Result<Option<bool>> {
		// Authorize a sandbox for its own processes.
		if let (
			tg::grant::Resource::Id(id),
			tg::grant::Permission::Process(_),
			Some(tg::Principal::Sandbox(sandbox)),
		) = (&resource, permission, self.context.principal.as_ref())
			&& let Ok(process) = tg::process::Id::try_from(id.clone())
			&& let Some(output) = self.server.try_get_process_local(&process, false).await?
			&& output.data.sandbox == *sandbox
		{
			return Ok(Some(true));
		}

		// Attempt to authorize.
		if let Some(true) = self
			.server
			.index
			.authorize(
				resource.clone(),
				permission,
				self.context.principal.as_ref(),
			)
			.await?
		{
			return Ok(Some(true));
		}

		// Index.
		self.index()
			.await
			.map_err(|error| tg::error!(!error, "failed to index"))?
			.try_last()
			.await
			.map_err(|error| tg::error!(!error, "failed to index"))?;

		// Attempt to authorize again.
		let output = self
			.server
			.index
			.authorize(resource, permission, self.context.principal.as_ref())
			.await?;

		Ok(output)
	}
}
