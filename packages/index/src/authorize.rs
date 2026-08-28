use tangram_client::prelude::*;

const SEARCH_EXHAUSTED: &str = "authorization_search_exhausted";

#[derive(Clone, Debug)]
pub struct Arg {
	pub required: tg::authorization::permission::Set,
	pub requested: tg::authorization::permission::Set,
	pub resource: tg::Selector<tg::Id>,
	pub token: Option<tg::authorization::Body>,
}

#[derive(Clone, Debug)]
pub struct Output {
	pub permissions: tg::authorization::permission::Set,
}

#[derive(Clone, Debug)]
pub enum Outcome {
	Authorized(Output),
	Denied(Option<Output>),
	Exhausted,
}

#[derive(
	Clone, Copy, Debug, Default, tangram_serialize::Deserialize, tangram_serialize::Serialize,
)]
pub struct Config {
	#[tangram_serialize(id = 0)]
	pub ancestor: SearchConfig,

	#[tangram_serialize(id = 1)]
	pub descendant: SearchConfig,

	#[tangram_serialize(id = 2)]
	pub subtree: SubtreeConfig,
}

#[derive(Clone, Copy, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct SearchConfig {
	#[tangram_serialize(id = 0)]
	pub max_depth: usize,

	#[tangram_serialize(id = 1)]
	pub max_edges: usize,

	#[tangram_serialize(id = 2)]
	pub max_nodes: usize,

	#[tangram_serialize(id = 3)]
	pub page_size: usize,
}

#[derive(Clone, Copy, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct SubtreeConfig {
	#[tangram_serialize(id = 0)]
	pub max_depth: usize,

	#[tangram_serialize(id = 1)]
	pub max_objects: usize,

	#[tangram_serialize(id = 2)]
	pub max_processes: usize,
}

enum NamedPermission {
	Admin,
	Read,
	Write,
}

impl Arg {
	pub(crate) fn validate(&self) -> tg::Result<()> {
		if !self.requested.contains(self.required) {
			return Err(tg::error!(
				"the required permissions must be contained in the requested permissions"
			));
		}

		Ok(())
	}
}

impl Outcome {
	#[must_use]
	pub fn output(&self) -> Option<&Output> {
		match self {
			Self::Authorized(output) | Self::Denied(Some(output)) => Some(output),
			Self::Denied(None) | Self::Exhausted => None,
		}
	}

	pub fn into_result(self) -> tg::Result<Output> {
		match self {
			Self::Authorized(output) => Ok(output),
			Self::Denied(_) => Err(tg::error!("authorization denied")),
			Self::Exhausted => Err(search_exhausted_error("the authorization search exhausted")),
		}
	}

	#[must_use]
	pub(crate) fn from_output(
		output: Option<Output>,
		permissions: tg::authorization::permission::Set,
	) -> Self {
		match output {
			Some(output) if output.permissions.contains(permissions) => Self::Authorized(output),
			output => Self::Denied(output),
		}
	}
}

impl Config {
	pub fn validate(&self) -> tg::Result<()> {
		if self.ancestor.page_size == 0 || self.descendant.page_size == 0 {
			return Err(tg::error!(
				"the authorization search page size must be greater than zero"
			));
		}

		Ok(())
	}
}

impl Default for SearchConfig {
	fn default() -> Self {
		Self {
			max_depth: 256,
			max_edges: 1024,
			max_nodes: 1024,
			page_size: 64,
		}
	}
}

impl Default for SubtreeConfig {
	fn default() -> Self {
		Self {
			max_depth: 256,
			max_objects: 1024,
			max_processes: 1024,
		}
	}
}

#[must_use]
pub(crate) fn search_exhausted_error(message: &str) -> tg::Error {
	let authorization_search_exhausted = true;

	tg::error!(?authorization_search_exhausted, "{message}")
}

#[must_use]
pub(crate) fn is_search_exhausted(error: &tg::Error) -> bool {
	error.state().object().is_some_and(|object| {
		object
			.try_unwrap_error_ref()
			.is_ok_and(|object| object.values.contains_key(SEARCH_EXHAUSTED))
	})
}

/// Validate that the permission is coherent with the resource kind.
pub fn validate_permission(
	resource: &tg::Id,
	permission: tg::authorization::Permission,
) -> tg::Result<()> {
	let valid = match permission {
		tg::authorization::Permission::Group(_) => resource.kind() == tg::id::Kind::Group,
		tg::authorization::Permission::Object(_) => {
			tg::object::Id::try_from(resource.clone()).is_ok()
		},
		tg::authorization::Permission::Organization(_) => {
			resource.kind() == tg::id::Kind::Organization
		},
		tg::authorization::Permission::Process(_) => resource.kind() == tg::id::Kind::Process,
		tg::authorization::Permission::Sandbox(_) => resource.kind() == tg::id::Kind::Sandbox,
		tg::authorization::Permission::Tag(_) => resource.kind() == tg::id::Kind::Tag,
		tg::authorization::Permission::User(_) => resource.kind() == tg::id::Kind::User,
	};
	if !valid {
		return Err(tg::error!(%resource, %permission, "invalid permission for the resource"));
	}
	Ok(())
}

pub fn validate(
	resource: &tg::Id,
	permissions: tg::authorization::permission::Set,
) -> tg::Result<()> {
	for permission in permissions.iter() {
		validate_permission(resource, permission)?;
	}
	Ok(())
}

pub(crate) fn permissions_for_specifier_prefix(
	resource: &tg::Id,
	permissions: tg::authorization::permission::Set,
) -> tg::Result<Option<tg::authorization::permission::Set>> {
	let mut permissions_ = permissions.iter();
	let Some(permission) = permissions_.next() else {
		return Ok(None);
	};
	if permissions_.next().is_some()
		|| !matches!(
			permission,
			tg::authorization::Permission::Group(
				tg::authorization::permission::group::Permission::Write
			) | tg::authorization::Permission::Tag(
				tg::authorization::permission::tag::Permission::Write
			)
		) {
		return Ok(None);
	}
	let permission = write_permission_for_resource(resource)?;
	let permissions = tg::authorization::permission::Set::from_permission(permission);

	Ok(Some(permissions))
}

pub(crate) fn write_permission_for_resource(
	resource: &tg::Id,
) -> tg::Result<tg::authorization::Permission> {
	match resource.kind() {
		tg::id::Kind::Group => Ok(tg::authorization::Permission::Group(
			tg::authorization::permission::group::Permission::Write,
		)),
		tg::id::Kind::Organization => Ok(tg::authorization::Permission::Organization(
			tg::authorization::permission::organization::Permission::Write,
		)),
		tg::id::Kind::Process => Ok(tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::Parent,
		)),
		tg::id::Kind::Sandbox => Ok(tg::authorization::Permission::Sandbox(
			tg::authorization::permission::sandbox::Permission::Write,
		)),
		tg::id::Kind::Tag => Ok(tg::authorization::Permission::Tag(
			tg::authorization::permission::tag::Permission::Write,
		)),
		tg::id::Kind::User => Ok(tg::authorization::Permission::User(
			tg::authorization::permission::user::Permission::Write,
		)),
		_ => Err(tg::error!(%resource, "invalid resource")),
	}
}

pub(crate) fn process_object_permission(
	kind: crate::process::object::Kind,
	permission: tg::authorization::permission::object::Permission,
) -> tg::authorization::permission::process::Permission {
	let process_permission = match kind {
		crate::process::object::Kind::Command => {
			tg::authorization::permission::process::Permission::NodeCommand
		},
		crate::process::object::Kind::Error => {
			tg::authorization::permission::process::Permission::NodeError
		},
		crate::process::object::Kind::Log => {
			tg::authorization::permission::process::Permission::NodeLog
		},
		crate::process::object::Kind::Output => {
			tg::authorization::permission::process::Permission::NodeOutput
		},
	};
	match permission {
		tg::authorization::permission::object::Permission::Node => process_permission,
		tg::authorization::permission::object::Permission::Subtree => {
			process_permission.to_subtree()
		},
	}
}

#[must_use]
pub(crate) fn permissions_implied_by(
	permission: tg::authorization::Permission,
) -> Vec<tg::authorization::Permission> {
	let permissions = match permission {
		tg::authorization::Permission::Group(_) => vec![
			tg::authorization::Permission::Group(
				tg::authorization::permission::group::Permission::Admin,
			),
			tg::authorization::Permission::Group(
				tg::authorization::permission::group::Permission::Read,
			),
			tg::authorization::Permission::Group(
				tg::authorization::permission::group::Permission::Write,
			),
		],
		tg::authorization::Permission::Object(_) => vec![
			tg::authorization::Permission::Object(
				tg::authorization::permission::object::Permission::Node,
			),
			tg::authorization::Permission::Object(
				tg::authorization::permission::object::Permission::Subtree,
			),
		],
		tg::authorization::Permission::Organization(_) => vec![
			tg::authorization::Permission::Organization(
				tg::authorization::permission::organization::Permission::Admin,
			),
			tg::authorization::Permission::Organization(
				tg::authorization::permission::organization::Permission::Read,
			),
			tg::authorization::Permission::Organization(
				tg::authorization::permission::organization::Permission::Write,
			),
		],
		tg::authorization::Permission::Process(_) => [
			tg::authorization::permission::process::Permission::Node,
			tg::authorization::permission::process::Permission::NodeCommand,
			tg::authorization::permission::process::Permission::NodeError,
			tg::authorization::permission::process::Permission::NodeLog,
			tg::authorization::permission::process::Permission::NodeOutput,
			tg::authorization::permission::process::Permission::Parent,
			tg::authorization::permission::process::Permission::Subtree,
			tg::authorization::permission::process::Permission::SubtreeCommand,
			tg::authorization::permission::process::Permission::SubtreeError,
			tg::authorization::permission::process::Permission::SubtreeLog,
			tg::authorization::permission::process::Permission::SubtreeOutput,
		]
		.into_iter()
		.map(tg::authorization::Permission::Process)
		.collect(),
		tg::authorization::Permission::Sandbox(_) => vec![
			tg::authorization::Permission::Sandbox(
				tg::authorization::permission::sandbox::Permission::Read,
			),
			tg::authorization::Permission::Sandbox(
				tg::authorization::permission::sandbox::Permission::Write,
			),
		],
		tg::authorization::Permission::Tag(_) => vec![
			tg::authorization::Permission::Tag(
				tg::authorization::permission::tag::Permission::Admin,
			),
			tg::authorization::Permission::Tag(
				tg::authorization::permission::tag::Permission::Read,
			),
			tg::authorization::Permission::Tag(
				tg::authorization::permission::tag::Permission::Write,
			),
		],
		tg::authorization::Permission::User(_) => vec![
			tg::authorization::Permission::User(
				tg::authorization::permission::user::Permission::Admin,
			),
			tg::authorization::Permission::User(
				tg::authorization::permission::user::Permission::Read,
			),
			tg::authorization::Permission::User(
				tg::authorization::permission::user::Permission::Write,
			),
		],
	};
	permissions
		.into_iter()
		.filter(|needed| permission.implies(*needed))
		.collect()
}

pub(crate) fn insert_implied_permissions(
	authorized: &mut tg::authorization::permission::Set,
	requested: tg::authorization::permission::Set,
	permission: tg::authorization::Permission,
) {
	for permission in permissions_implied_by(permission) {
		let permission = tg::authorization::permission::Set::from_permission(permission);
		if requested.contains(permission) {
			authorized.insert(permission);
		}
	}
}

#[must_use]
pub(crate) fn permissions_in_search_order(
	permissions: tg::authorization::permission::Set,
) -> Vec<tg::authorization::Permission> {
	let mut permissions = permissions.iter().collect::<Vec<_>>();
	permissions.sort();
	let mut ordered = Vec::with_capacity(permissions.len());
	while !permissions.is_empty() {
		let index = permissions
			.iter()
			.position(|permission| {
				!permissions.iter().any(|candidate| {
					candidate != permission
						&& candidate.implies(*permission)
						&& !permission.implies(*candidate)
				})
			})
			.unwrap_or_default();
		ordered.push(permissions.remove(index));
	}

	ordered
}

pub(crate) fn permission_for_named_parent(
	parent: &tg::Id,
	permission: tg::authorization::Permission,
) -> tg::Result<tg::authorization::Permission> {
	let permission = match permission {
		tg::authorization::Permission::Group(permission) => match permission {
			tg::authorization::permission::group::Permission::Admin => NamedPermission::Admin,
			tg::authorization::permission::group::Permission::Read => NamedPermission::Read,
			tg::authorization::permission::group::Permission::Write => NamedPermission::Write,
		},
		tg::authorization::Permission::Organization(permission) => match permission {
			tg::authorization::permission::organization::Permission::Admin => {
				NamedPermission::Admin
			},
			tg::authorization::permission::organization::Permission::Read => NamedPermission::Read,
			tg::authorization::permission::organization::Permission::Write => {
				NamedPermission::Write
			},
		},
		tg::authorization::Permission::Tag(permission) => match permission {
			tg::authorization::permission::tag::Permission::Admin => NamedPermission::Admin,
			tg::authorization::permission::tag::Permission::Read => NamedPermission::Read,
			tg::authorization::permission::tag::Permission::Write => NamedPermission::Write,
		},
		tg::authorization::Permission::User(permission) => match permission {
			tg::authorization::permission::user::Permission::Admin => NamedPermission::Admin,
			tg::authorization::permission::user::Permission::Read => NamedPermission::Read,
			tg::authorization::permission::user::Permission::Write => NamedPermission::Write,
		},
		_ => return Err(tg::error!(%parent, %permission, "invalid named node permission")),
	};
	let permission = match (parent.kind(), permission) {
		(tg::id::Kind::Group, NamedPermission::Admin) => tg::authorization::Permission::Group(
			tg::authorization::permission::group::Permission::Admin,
		),
		(tg::id::Kind::Group, NamedPermission::Read) => tg::authorization::Permission::Group(
			tg::authorization::permission::group::Permission::Read,
		),
		(tg::id::Kind::Group, NamedPermission::Write) => tg::authorization::Permission::Group(
			tg::authorization::permission::group::Permission::Write,
		),
		(tg::id::Kind::Organization, NamedPermission::Admin) => {
			tg::authorization::Permission::Organization(
				tg::authorization::permission::organization::Permission::Admin,
			)
		},
		(tg::id::Kind::Organization, NamedPermission::Read) => {
			tg::authorization::Permission::Organization(
				tg::authorization::permission::organization::Permission::Read,
			)
		},
		(tg::id::Kind::Organization, NamedPermission::Write) => {
			tg::authorization::Permission::Organization(
				tg::authorization::permission::organization::Permission::Write,
			)
		},
		(tg::id::Kind::Tag, NamedPermission::Admin) => tg::authorization::Permission::Tag(
			tg::authorization::permission::tag::Permission::Admin,
		),
		(tg::id::Kind::Tag, NamedPermission::Read) => {
			tg::authorization::Permission::Tag(tg::authorization::permission::tag::Permission::Read)
		},
		(tg::id::Kind::Tag, NamedPermission::Write) => tg::authorization::Permission::Tag(
			tg::authorization::permission::tag::Permission::Write,
		),
		(tg::id::Kind::User, NamedPermission::Admin) => tg::authorization::Permission::User(
			tg::authorization::permission::user::Permission::Admin,
		),
		(tg::id::Kind::User, NamedPermission::Read) => tg::authorization::Permission::User(
			tg::authorization::permission::user::Permission::Read,
		),
		(tg::id::Kind::User, NamedPermission::Write) => tg::authorization::Permission::User(
			tg::authorization::permission::user::Permission::Write,
		),
		_ => return Err(tg::error!(%parent, "invalid named node parent")),
	};

	Ok(permission)
}
