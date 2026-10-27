use {
	crate::{Server as CoreServer, context::Origin},
	futures::future::BoxFuture,
	std::{os::fd::OwnedFd, path::Path},
	tangram_client::prelude::*,
	tangram_server_vfs_core::Server,
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
