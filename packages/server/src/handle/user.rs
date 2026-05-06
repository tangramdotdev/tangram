use {crate::Handle, tangram_client::prelude::*};

impl tg::handle::User for Handle {
	async fn get_user(&self, token: &str) -> tg::Result<Option<tg::User>> {
		self.get_user(token).await
	}
}
