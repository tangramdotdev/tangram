use {
	crate::{Config, Server, checkpoint, messenger::Messenger},
	futures::{
		FutureExt as _,
		future::{self, BoxFuture},
	},
	std::{path::Path, sync::Arc},
	tangram_client::prelude::*,
};

pub struct Arg<'a> {
	pub authentication_cache: &'a Arc<crate::authentication::cache::Cache>,
	pub authentication_cache_events: &'a tokio::sync::broadcast::Sender<()>,
	pub checkpoints: Option<&'a Arc<checkpoint::State>>,
	pub config: &'a Config,
	pub messenger: &'a Messenger,
	pub path: &'a Path,
	pub tangram_path: &'a Path,
}

pub trait Lifecycle: Send + Sync + 'static {
	fn start<'a>(&'a self, _server: &'a Server) -> BoxFuture<'a, tg::Result<()>> {
		future::ok(()).boxed()
	}

	fn stop<'a>(&'a self, _server: &'a Server) -> BoxFuture<'a, ()> {
		future::ready(()).boxed()
	}

	fn abort(&self) {}
}
