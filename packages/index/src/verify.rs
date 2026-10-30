use tangram_client::prelude::*;

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
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Outcome {
	Exhausted,
	Satisfied,
	Unsatisfied,
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

impl Arg {
	pub fn validate(&self) -> tg::Result<()> {
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
