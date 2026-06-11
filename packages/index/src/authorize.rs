use tangram_client::prelude::*;

/// Validate that the permission is coherent with the resource kind.
pub fn validate(resource: &tg::Id, permission: tg::grant::Permission) -> tg::Result<()> {
	let valid = match permission {
		tg::grant::Permission::Admin
		| tg::grant::Permission::Read
		| tg::grant::Permission::Write => {
			matches!(
				resource.kind(),
				tg::id::Kind::User
					| tg::id::Kind::Group
					| tg::id::Kind::Organization
					| tg::id::Kind::Tag
			)
		},
		tg::grant::Permission::Object(_) => tg::object::Id::try_from(resource.clone()).is_ok(),
		tg::grant::Permission::Process(_) => resource.kind() == tg::id::Kind::Process,
	};
	if !valid {
		return Err(tg::error!(%resource, %permission, "invalid permission for the resource"));
	}
	Ok(())
}

/// Map a process object kind to the node level aspect permission.
#[must_use]
pub fn node_permission(
	kind: crate::process::object::Kind,
) -> tg::grant::permission::process::Permission {
	match kind {
		crate::process::object::Kind::Command => {
			tg::grant::permission::process::Permission::NodeCommand
		},
		crate::process::object::Kind::Error => {
			tg::grant::permission::process::Permission::NodeError
		},
		crate::process::object::Kind::Log => tg::grant::permission::process::Permission::NodeLog,
		crate::process::object::Kind::Output => {
			tg::grant::permission::process::Permission::NodeOutput
		},
	}
}
