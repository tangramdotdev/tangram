use {
	crate::{Session, authorization::IntoResource},
	tangram_client::prelude::*,
	tangram_futures::stream::TryExt as _,
	tangram_index::prelude::*,
};

impl Session {
	pub(crate) async fn exists(
		&self,
		resource: impl IntoResource,
		permissions: impl Into<tg::grant::permission::Set>,
	) -> tg::Result<bool> {
		let permissions = permissions.into();
		let resource = resource.into_resource();
		if self.try_exists(resource.clone(), permissions).await? {
			return Ok(true);
		}

		self.index()
			.await
			.map_err(|error| tg::error!(!error, "failed to index"))?
			.try_last()
			.await
			.map_err(|error| tg::error!(!error, "failed to index"))?;

		self.try_exists(resource, permissions).await
	}

	async fn try_exists(
		&self,
		resource: tg::grant::Resource,
		permissions: tg::grant::permission::Set,
	) -> tg::Result<bool> {
		let output = self
			.server
			.index
			.authorize(resource, permissions, &tg::Principal::Root)
			.await
			.map_err(|error| tg::error!(!error, "failed to check whether the resource exists"))?;
		Ok(output.is_some())
	}
}
