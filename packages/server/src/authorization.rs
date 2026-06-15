use {crate::Session, tangram_client::prelude::*, tangram_index::prelude::*};

impl Session {
	pub(crate) async fn authorize(
		&self,
		resource: tg::grant::Resource,
		permission: tg::grant::Permission,
	) -> tg::Result<Option<bool>> {
		self.server
			.index
			.authorize(resource, permission, self.context.principal.as_ref())
			.await
	}
}
