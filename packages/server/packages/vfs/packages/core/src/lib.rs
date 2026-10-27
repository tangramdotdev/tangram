use {futures::future::BoxFuture, tangram_client::prelude::*};

/// A running vfs.
pub trait Server: Send {
	#[cfg(target_os = "linux")]
	fn provider(&self) -> std::sync::Arc<dyn Provider>;

	fn stop(&self);

	fn wait(self: Box<Self>) -> BoxFuture<'static, ()>;
}

/// A handle to a vfs provider that does not keep the provider alive.
pub trait Provider: Send + Sync {
	fn root_tokens(&self, id: &tg::artifact::Id) -> tg::authorization::Tokens;

	fn seed_tokens(&self, tokens: &tg::authorization::Tokens);

	#[cfg(target_os = "linux")]
	fn set_principal(&self, principal: tg::Principal);
}
