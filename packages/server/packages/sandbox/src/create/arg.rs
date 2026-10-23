use {crate::Component, tangram_client::prelude::*, tangram_server_core::Session};

impl Component {
	pub async fn validate_sandbox_create_arg_with_parent(
		&self,
		session: &Session,
		arg: &tg::sandbox::create::Arg,
		parent: &tg::sandbox::Id,
	) -> tg::Result<()> {
		let parent = if let Some(parent) = session
			.server
			.components
			.runner
			.state()
			.try_get_sandbox(parent)
		{
			parent
		} else {
			self.try_get_sandbox_from_index(session, parent)
				.await
				.map_err(|error| tg::error!(!error, %parent, "failed to get the parent sandbox"))?
				.and_then(|sandbox| sandbox.data)
				.ok_or_else(|| tg::error!(%parent, "failed to find the parent sandbox"))?
		};

		if !sandbox_network_contains(parent.data.network.as_ref(), arg.network.as_ref()) {
			let parent = &parent.data.id;
			return Err(tg::error!(
				%parent,
				"the child sandbox network is more permissive than the parent sandbox network"
			));
		}

		validate_sandbox_mounts(&parent.data.mounts, &arg.mounts).await?;

		Ok(())
	}
}

fn sandbox_network_contains(
	parent: Option<&tg::sandbox::Network>,
	child: Option<&tg::sandbox::Network>,
) -> bool {
	match child {
		None => true,
		Some(tg::sandbox::Network::Bridge(child)) => match parent {
			Some(tg::sandbox::Network::Bridge(_) | tg::sandbox::Network::Default) => {
				child.ports.is_empty()
			},
			Some(tg::sandbox::Network::Host) => true,
			None => false,
		},
		Some(tg::sandbox::Network::Default) => parent.is_some(),
		Some(tg::sandbox::Network::Host) => {
			matches!(parent, Some(tg::sandbox::Network::Host))
		},
	}
}

async fn validate_sandbox_mounts(
	parent: &[tg::sandbox::Mount],
	child: &[tg::sandbox::Mount],
) -> tg::Result<()> {
	if child.is_empty() {
		return Ok(());
	}

	let mut parent_sources = Vec::with_capacity(parent.len());
	for mount in parent {
		let source = tokio::fs::canonicalize(&mount.source)
			.await
			.map_err(|error| {
				tg::error!(
					!error,
					path = %mount.source.display(),
					"failed to resolve a parent sandbox mount source"
				)
			})?;
		parent_sources.push((source, mount.readonly));
	}

	for mount in child {
		let source = tokio::fs::canonicalize(&mount.source)
			.await
			.map_err(|error| {
				tg::error!(
					!error,
					path = %mount.source.display(),
					"failed to resolve a child sandbox mount source"
				)
			})?;
		let allowed = parent_sources
			.iter()
			.any(|(parent, readonly)| source.starts_with(parent) && (!readonly || mount.readonly));
		if !allowed {
			return Err(tg::error!(
				source = %mount.source.display(),
				target = %mount.target.display(),
				"the child sandbox mount is more permissive than the parent sandbox mounts"
			));
		}
	}

	Ok(())
}
