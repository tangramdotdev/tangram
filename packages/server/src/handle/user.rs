use {
	super::ServerWithContext,
	crate::{Context, Server, Shared},
	tangram_client::prelude::*,
};

impl tg::handle::User for Shared {
	async fn get_user(&self, token: &str) -> tg::Result<Option<tg::User>> {
		self.0.get_user(token).await
	}
}

impl tg::handle::User for Server {
	async fn get_user(&self, token: &str) -> tg::Result<Option<tg::User>> {
		self.get_user_with_context(&Context::default(), token).await
	}
}

impl tg::handle::User for ServerWithContext {
	async fn get_user(&self, token: &str) -> tg::Result<Option<tg::User>> {
		self.0.get_user_with_context(&self.1, token).await
	}
}
