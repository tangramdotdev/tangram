use {
	crate::Component,
	std::path::{Path, PathBuf},
	tangram_client::prelude::*,
	tangram_server_core::Session,
};

impl Component {
	pub(crate) fn host_path_for_guest_path(session: &Session, path: &Path) -> tg::Result<PathBuf> {
		Self::host_path_for_guest_path_with_store_path(
			session,
			path,
			&session.server.checkout_path(),
		)
	}

	pub(crate) fn host_path_for_guest_path_with_store_path(
		session: &Session,
		path: &Path,
		store_path: &Path,
	) -> tg::Result<PathBuf> {
		let Some(sandbox) = session
			.server
			.try_get_request_origin_sandbox(session.context.origin)?
		else {
			return Ok(path.to_owned());
		};
		let id = sandbox.id.clone();
		let sandbox = sandbox
			.sandbox
			.clone()
			.ok_or_else(|| tg::error!(%id, "failed to get the origin sandbox"))?;

		// Map the guest store to the filesystem needed by the operation.
		if session.server.components.vfs.mounted()
			&& let Ok(rest) = path.strip_prefix(sandbox.guest_store_path())
		{
			return Ok(store_path.join(rest));
		}

		sandbox
			.host_path_for_guest_path(path)
			.ok_or_else(|| tg::error!(path = %path.display(), "no host path for guest path"))
	}

	pub(crate) fn guest_path_for_host_path(session: &Session, path: &Path) -> tg::Result<PathBuf> {
		let Some(sandbox) = session
			.server
			.try_get_request_origin_sandbox(session.context.origin)?
		else {
			return Ok(path.to_owned());
		};
		let id = sandbox.id.clone();
		let sandbox = sandbox
			.sandbox
			.clone()
			.ok_or_else(|| tg::error!(%id, "failed to get the origin sandbox"))?;

		// Serve a shared store path through the per-sandbox VFS mount.
		if session.server.components.vfs.mounted()
			&& let Ok(rest) = path.strip_prefix(session.server.store_path())
		{
			return Ok(sandbox.guest_store_path().join(rest));
		}

		sandbox
			.guest_path_for_host_path(path)
			.ok_or_else(|| tg::error!(path = %path.display(), "no guest path for host path"))
	}
}
