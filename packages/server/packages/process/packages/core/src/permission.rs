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
	let time_to_live = i64::try_from(time_to_live.as_secs())
		.map_err(|error| tg::error!(!error, "failed to convert the permission ttl"))?;
	let expires_at = created_at
		.checked_add(time_to_live)
		.ok_or_else(|| tg::error!("the permission expiration overflowed"))?;
	let permission = tg::authorization::Permission::Process(
		tg::authorization::permission::process::Permission::Parent,
	);
	let arg = tangram_index::permission::put::Arg {
		created_at,
		permissions: permission.into(),
		resource: id.clone().into(),
		source: tangram_index::permission::Source::Direct {
			expires_at: Some(expires_at),
		},
		subject,
		time_to_touch: Some(time_to_touch),
		version: None,
	};
	Ok(Some(arg))
}
