use {
	crate::{Server as CoreServer, Session, context::Origin},
	futures::future::BoxFuture,
	std::{os::fd::OwnedFd, path::Path},
	tangram_client::prelude::*,
};

#[derive(Clone, Copy, Debug)]
pub enum Kind {
	Fskit,
	Fuse,
	Nfs,
}

pub struct MountArg<'a> {
	pub kind: Kind,
	pub options: crate::config::Vfs,
	pub origin: Origin,
	pub path: &'a Path,
	pub principal: Option<tg::Principal>,
	pub recvfd: Option<OwnedFd>,
}

/// The vfs component.
pub trait Component: crate::component::Lifecycle {
	fn mount<'a>(
		&'a self,
		server: &'a CoreServer,
		arg: MountArg<'a>,
	) -> BoxFuture<'a, tg::Result<Box<dyn Server>>>;

	fn mount_store<'a>(
		&'a self,
		server: &'a CoreServer,
		arg: MountArg<'a>,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn mounted(&self) -> bool;

	#[cfg(target_os = "linux")]
	fn start_virtiofs<'a>(
		&'a self,
		server: &'a CoreServer,
		socket: &'a Path,
		dax: Option<u64>,
		origin: Origin,
		principal: Option<tg::Principal>,
	) -> BoxFuture<'a, tg::Result<Box<dyn Server>>>;

	fn unmount<'a>(&'a self, kind: Kind, path: &'a Path) -> BoxFuture<'a, tg::Result<()>>;
}

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

	fn seed_tokens(&self, session: &Session, tokens: &tg::authorization::Tokens);

	#[cfg(target_os = "linux")]
	fn set_principal(&self, principal: tg::Principal);
}
