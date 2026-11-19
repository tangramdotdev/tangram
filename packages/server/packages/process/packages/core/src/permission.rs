use tangram_client::prelude::*;

/// Create the permission that lets the creator of a process read, wait on, and cancel it, or `None` for the root principal.
pub fn creator_permission_arg(
	principal: &tg::Principal,
	id: &tg::process::Id,
	created_at: i64,
	time_to_live: std::time::Duration,
	time_to_touch: std::time::Duration,
) -> tg::Result<Option<tangram_index::permission::put::Arg>> {
	let subject = match principal {
		tg::Principal::Anonymous => tg::authorization::Subject::Public,
		tg::Principal::Root => return Ok(None),
		principal => principal.try_to_subject()?,
	};
	let permissions = tg::authorization::permission::process::Set::from_permission(
		tg::authorization::permission::process::Permission::Parent,
	);
	let arg = permission_arg(
		subject,
		permissions,
		id,
		created_at,
		time_to_live,
		time_to_touch,
	)?;
	Ok(Some(arg))
}

/// Create the temporary permission that lets anyone read a public process, its children, and their command, error, log, and output objects.
pub fn public_permission_arg(
	id: &tg::process::Id,
	created_at: i64,
	time_to_live: std::time::Duration,
	time_to_touch: std::time::Duration,
) -> tg::Result<tangram_index::permission::put::Arg> {
	let mut permissions = tg::authorization::permission::process::Set::SUBTREE;
	permissions.insert(tg::authorization::permission::process::Set::SUBTREE_COMMAND_OBJECTS);
	permissions.insert(tg::authorization::permission::process::Set::SUBTREE_ERROR_OBJECTS);
	permissions.insert(tg::authorization::permission::process::Set::SUBTREE_LOG_OBJECTS);
	permissions.insert(tg::authorization::permission::process::Set::SUBTREE_OUTPUT_OBJECTS);
	permission_arg(
		tg::authorization::Subject::Public,
		permissions,
		id,
		created_at,
		time_to_live,
		time_to_touch,
	)
}

/// Create the temporary permission that lets anyone read a cached process, its error objects, and its output objects, which a cache lookup proves for the caller.
pub fn cached_public_permission_arg(
	id: &tg::process::Id,
	created_at: i64,
	time_to_live: std::time::Duration,
	time_to_touch: std::time::Duration,
) -> tg::Result<tangram_index::permission::put::Arg> {
	permission_arg(
		tg::authorization::Subject::Public,
		cached_process_permissions(),
		id,
		created_at,
		time_to_live,
		time_to_touch,
	)
}

/// Get the permissions that a cache lookup requires on a cached process, which are the node, error objects, and output objects.
#[must_use]
pub fn cached_process_permissions() -> tg::authorization::permission::process::Set {
	let mut permissions = tg::authorization::permission::process::Set::NODE;
	permissions.insert(tg::authorization::permission::process::Set::NODE_ERROR_OBJECTS);
	permissions.insert(tg::authorization::permission::process::Set::NODE_OUTPUT_OBJECTS);
	permissions
}

/// Create a direct permission on a process that expires after the ttl unless it is touched.
pub fn permission_arg(
	subject: tg::authorization::Subject,
	permissions: tg::authorization::permission::process::Set,
	id: &tg::process::Id,
	created_at: i64,
	time_to_live: std::time::Duration,
	time_to_touch: std::time::Duration,
) -> tg::Result<tangram_index::permission::put::Arg> {
	let time_to_live = i64::try_from(time_to_live.as_secs())
		.map_err(|error| tg::error!(!error, "failed to convert the permission ttl"))?;
	let expires_at = created_at
		.checked_add(time_to_live)
		.ok_or_else(|| tg::error!("the permission expiration overflowed"))?;
	let arg = tangram_index::permission::put::Arg {
		created_at,
		permissions: tg::authorization::permission::Set::Process(permissions),
		resource: id.clone().into(),
		source: tangram_index::permission::Source::Direct {
			expires_at: Some(expires_at),
		},
		subject,
		time_to_touch: Some(time_to_touch),
		version: None,
	};
	Ok(arg)
}
