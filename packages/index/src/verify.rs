use tangram_client::prelude::*;

mod discover;
mod engine;
pub use engine::Batch;
#[doc(hidden)]
pub mod facts;
pub(crate) mod search;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) enum Check {
	ObjectChild {
		child: tg::object::Id,
		parent: tg::object::Id,
	},
	ProcessChild {
		child: tg::process::Id,
		parent: tg::process::Id,
	},
	ProcessObject {
		kind: crate::process::object::Kind,
		object: tg::object::Id,
		process: tg::process::Id,
	},
	ProcessObjectPermission {
		object: tg::object::Id,
		permission: tg::authorization::permission::object::Permission,
		process: tg::process::Id,
	},
}

#[derive(Clone, Debug)]
pub struct Arg {
	pub requested: tg::authorization::permission::Set,
	pub required: tg::authorization::permission::Set,
	pub resource: tg::Selector<tg::Id>,
	pub storage: tg::storage::Set,
	/// The subject to verify, or the request principal when absent.
	pub subject: Option<tg::authorization::Subject>,
	/// Validated token bodies available to the subject being verified.
	pub tokens: Vec<tg::authorization::Body>,
}

#[derive(Clone, Debug)]
pub struct Output {
	pub expires_at: Option<i64>,
	pub outcome: Outcome,
	pub permissions: tg::authorization::permission::Set,
	pub storage: tg::storage::Set,
	pub syncs: Vec<Sync>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Outcome {
	Exhausted,
	Satisfied,
	Unsatisfied,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Sync {
	pub permission: tg::authorization::Permission,
	pub resource: tg::Id,
	pub sync: tg::sync::Id,
}

#[derive(
	Clone, Copy, Debug, Default, tangram_serialize::Deserialize, tangram_serialize::Serialize,
)]
pub struct Config {
	#[tangram_serialize(id = 0)]
	pub permissions: PermissionsConfig,
}

#[derive(
	Clone, Copy, Debug, Default, tangram_serialize::Deserialize, tangram_serialize::Serialize,
)]
pub struct PermissionsConfig {
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

impl Check {
	fn matches(&self, output: facts::Output) -> tg::Result<bool> {
		let value = match self {
			Self::ProcessObject { kind, .. } => output.into_process_object_kinds()?.contains(kind),
			Self::ObjectChild { .. }
			| Self::ProcessChild { .. }
			| Self::ProcessObjectPermission { .. } => output.into_bool()?,
		};

		Ok(value)
	}

	fn request(&self) -> facts::Request {
		match self {
			Self::ObjectChild { child, parent } => facts::Request::ObjectChild {
				child: child.clone(),
				parent: parent.clone(),
			},
			Self::ProcessChild { child, parent } => facts::Request::ProcessChild {
				child: child.clone(),
				parent: parent.clone(),
			},
			Self::ProcessObject {
				object, process, ..
			} => facts::Request::ProcessObject {
				object: object.clone(),
				process: process.clone(),
			},
			Self::ProcessObjectPermission {
				object,
				permission,
				process,
			} => facts::Request::ProcessObjectPermission {
				object: object.clone(),
				permission: *permission,
				process: process.clone(),
			},
		}
	}
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

impl Output {
	pub fn into_result(self) -> tg::Result<Self> {
		match self.outcome {
			Outcome::Exhausted => Err(search_exhausted_error("the verification search exhausted")),
			Outcome::Satisfied => Ok(self),
			Outcome::Unsatisfied => Err(tg::error!("verification denied")),
		}
	}
}

impl Config {
	pub fn validate(&self) -> tg::Result<()> {
		self.permissions.validate()?;
		Ok(())
	}
}

impl PermissionsConfig {
	pub fn validate(&self) -> tg::Result<()> {
		if self.ancestor.page_size == 0 || self.descendant.page_size == 0 {
			return Err(tg::error!(
				"the verification search page size must be greater than zero"
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
pub fn search_exhausted_error(message: &str) -> tg::Error {
	let verification_search_exhausted = true;

	tg::error!(?verification_search_exhausted, "{message}")
}

/// Validate that the permission is coherent with the resource kind.
pub fn validate_permission(
	resource: &tg::Id,
	permission: tg::authorization::Permission,
) -> tg::Result<()> {
	let valid = match permission {
		tg::authorization::Permission::Sync(_) => resource.kind() == tg::id::Kind::Sync,
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
			tg::authorization::permission::sandbox::Permission::Parent,
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
) -> tg::authorization::permission::process::Permission {
	// The node field permission covers the object's subtree; the subtree field permission additionally covers child processes.
	match kind {
		crate::process::object::Kind::Command => {
			tg::authorization::permission::process::Permission::NodeCommandObjects
		},
		crate::process::object::Kind::Error => {
			tg::authorization::permission::process::Permission::NodeErrorObjects
		},
		crate::process::object::Kind::Log => {
			tg::authorization::permission::process::Permission::NodeLogObjects
		},
		crate::process::object::Kind::Output => {
			tg::authorization::permission::process::Permission::NodeOutputObjects
		},
	}
}

#[must_use]
pub(crate) fn permissions_implied_by(
	permission: tg::authorization::Permission,
) -> Vec<tg::authorization::Permission> {
	let permissions = match permission {
		tg::authorization::Permission::Sync(_) => vec![tg::authorization::Permission::Sync(
			tg::authorization::permission::sync::Permission::Read,
		)],
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
			tg::authorization::permission::process::Permission::NodeCommandObjects,
			tg::authorization::permission::process::Permission::NodeErrorObjects,
			tg::authorization::permission::process::Permission::NodeLogObjects,
			tg::authorization::permission::process::Permission::NodeOutputObjects,
			tg::authorization::permission::process::Permission::Parent,
			tg::authorization::permission::process::Permission::Subtree,
			tg::authorization::permission::process::Permission::SubtreeCommandObjects,
			tg::authorization::permission::process::Permission::SubtreeErrorObjects,
			tg::authorization::permission::process::Permission::SubtreeLogObjects,
			tg::authorization::permission::process::Permission::SubtreeOutputObjects,
		]
		.into_iter()
		.map(tg::authorization::Permission::Process)
		.collect(),
		tg::authorization::Permission::Sandbox(_) => vec![
			tg::authorization::Permission::Sandbox(
				tg::authorization::permission::sandbox::Permission::Node,
			),
			tg::authorization::Permission::Sandbox(
				tg::authorization::permission::sandbox::Permission::Parent,
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
	verified: &mut tg::authorization::permission::Set,
	requested: tg::authorization::permission::Set,
	permission: tg::authorization::Permission,
) {
	for permission in permissions_implied_by(permission) {
		let permission = tg::authorization::permission::Set::from_permission(permission);
		if requested.contains(permission) {
			verified.insert(permission);
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

#[must_use]
pub fn storage_permissions(
	storage: tg::storage::Set,
	permissions: tg::authorization::permission::Set,
) -> tg::authorization::permission::Set {
	if storage.is_empty() {
		return permissions.empty_like();
	}
	match storage {
		tg::storage::Set::Object(storage) => {
			let mut permissions = tg::authorization::permission::object::Set::empty();
			for storage in storage.iter() {
				let permission = match storage {
					tg::object::storage::Storage::Node => {
						tg::authorization::permission::object::Permission::Node
					},
					tg::object::storage::Storage::Subtree => {
						tg::authorization::permission::object::Permission::Subtree
					},
				};
				permissions.insert(tg::authorization::permission::object::Set::from_permission(
					permission,
				));
			}
			tg::authorization::permission::Set::Object(permissions)
		},
		tg::storage::Set::Process(storage) => {
			let mut permissions = tg::authorization::permission::process::Set::empty();
			for storage in storage.iter() {
				let permission = match storage {
					tg::process::storage::Storage::Node => {
						tg::authorization::permission::process::Permission::Node
					},
					tg::process::storage::Storage::NodeCommandObjects => {
						tg::authorization::permission::process::Permission::NodeCommandObjects
					},
					tg::process::storage::Storage::NodeErrorObjects => {
						tg::authorization::permission::process::Permission::NodeErrorObjects
					},
					tg::process::storage::Storage::NodeLogObjects => {
						tg::authorization::permission::process::Permission::NodeLogObjects
					},
					tg::process::storage::Storage::NodeOutputObjects => {
						tg::authorization::permission::process::Permission::NodeOutputObjects
					},
					tg::process::storage::Storage::Subtree => {
						tg::authorization::permission::process::Permission::Subtree
					},
					tg::process::storage::Storage::SubtreeCommandObjects => {
						tg::authorization::permission::process::Permission::SubtreeCommandObjects
					},
					tg::process::storage::Storage::SubtreeErrorObjects => {
						tg::authorization::permission::process::Permission::SubtreeErrorObjects
					},
					tg::process::storage::Storage::SubtreeLogObjects => {
						tg::authorization::permission::process::Permission::SubtreeLogObjects
					},
					tg::process::storage::Storage::SubtreeOutputObjects => {
						tg::authorization::permission::process::Permission::SubtreeOutputObjects
					},
				};
				permissions.insert(
					tg::authorization::permission::process::Set::from_permission(permission),
				);
			}
			tg::authorization::permission::Set::Process(permissions)
		},
	}
}
