use crate::prelude::*;

pub mod group;
pub mod object;
pub mod organization;
pub mod process;
pub mod sandbox;
pub mod tag;
pub mod user;

#[derive(
	Clone,
	Copy,
	Debug,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	derive_more::Display,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[display(rename_all = "snake_case")]
#[tangram_serialize(display, from_str)]
pub enum Permission {
	#[display("group_{_0}")]
	Group(group::Permission),

	#[display("object_{_0}")]
	Object(object::Permission),

	#[display("organization_{_0}")]
	Organization(organization::Permission),

	#[display("process_{_0}")]
	Process(process::Permission),

	#[display("sandbox_{_0}")]
	Sandbox(sandbox::Permission),

	#[display("tag_{_0}")]
	Tag(tag::Permission),

	#[display("user_{_0}")]
	User(user::Permission),
}

#[derive(
	Clone,
	Copy,
	Debug,
	Eq,
	Hash,
	PartialEq,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
)]
pub enum Set {
	Group(group::Set),
	Object(object::Set),
	Organization(organization::Set),
	Process(process::Set),
	Sandbox(sandbox::Set),
	Tag(tag::Set),
	User(user::Set),
}

impl Permission {
	pub fn parse_for_kind(kind: tg::grant::resource::Kind, s: &str) -> tg::Result<Self> {
		match kind {
			tg::grant::resource::Kind::Group => {
				let permission = s
					.parse()
					.map_err(|_| tg::error!("invalid grant permission"))?;
				Ok(Self::Group(permission))
			},
			tg::grant::resource::Kind::Object => {
				let permission = s
					.parse()
					.map_err(|_| tg::error!("invalid grant permission"))?;
				Ok(Self::Object(permission))
			},
			tg::grant::resource::Kind::Organization => {
				let permission = s
					.parse()
					.map_err(|_| tg::error!("invalid grant permission"))?;
				Ok(Self::Organization(permission))
			},
			tg::grant::resource::Kind::Process => {
				let permission = s
					.parse()
					.map_err(|_| tg::error!("invalid grant permission"))?;
				Ok(Self::Process(permission))
			},
			tg::grant::resource::Kind::Sandbox => {
				let permission = s
					.parse()
					.map_err(|_| tg::error!("invalid grant permission"))?;
				Ok(Self::Sandbox(permission))
			},
			tg::grant::resource::Kind::Tag => {
				let permission = s
					.parse()
					.map_err(|_| tg::error!("invalid grant permission"))?;
				Ok(Self::Tag(permission))
			},
			tg::grant::resource::Kind::User => {
				let permission = s
					.parse()
					.map_err(|_| tg::error!("invalid grant permission"))?;
				Ok(Self::User(permission))
			},
		}
	}

	#[must_use]
	pub fn kind(self) -> tg::grant::resource::Kind {
		match self {
			Self::Group(_) => tg::grant::resource::Kind::Group,
			Self::Object(_) => tg::grant::resource::Kind::Object,
			Self::Organization(_) => tg::grant::resource::Kind::Organization,
			Self::Process(_) => tg::grant::resource::Kind::Process,
			Self::Sandbox(_) => tg::grant::resource::Kind::Sandbox,
			Self::Tag(_) => tg::grant::resource::Kind::Tag,
			Self::User(_) => tg::grant::resource::Kind::User,
		}
	}

	#[must_use]
	pub fn implies(self, needed: Self) -> bool {
		match (self, needed) {
			(Self::Group(granted), Self::Group(needed)) => granted.implies(needed),
			(Self::Object(granted), Self::Object(needed)) => granted.implies(needed),
			(Self::Organization(granted), Self::Organization(needed)) => granted.implies(needed),
			(Self::Process(granted), Self::Process(needed)) => granted.implies(needed),
			(Self::Sandbox(granted), Self::Sandbox(needed)) => granted.implies(needed),
			(Self::Tag(granted), Self::Tag(needed)) => granted.implies(needed),
			(Self::User(granted), Self::User(needed)) => granted.implies(needed),
			_ => false,
		}
	}

	/// Raise an object or process permission to the subtree variant of its aspect.
	#[must_use]
	pub fn subtree(self) -> Self {
		match self {
			Self::Object(permission) => Self::Object(permission.subtree()),
			Self::Process(permission) => Self::Process(permission.to_subtree()),
			permission => permission,
		}
	}
}

impl Set {
	pub fn parse_for_kind(kind: tg::grant::resource::Kind, s: &str) -> tg::Result<Self> {
		let mut permissions: Option<Self> = None;
		for part in s.split(',') {
			if part.is_empty() {
				return Err(tg::error!("invalid grant permissions"));
			}
			let permission = part
				.parse::<Permission>()
				.or_else(|_| Permission::parse_for_kind(kind, part))?;
			let next = Self::from_permission(permission);
			match &mut permissions {
				Some(permissions) if permissions.kind() == next.kind() => permissions.insert(next),
				Some(_) => return Err(tg::error!("invalid grant permissions")),
				None => permissions = Some(next),
			}
		}
		permissions.ok_or_else(|| tg::error!("invalid grant permissions"))
	}

	#[must_use]
	pub fn from_permission(permission: Permission) -> Self {
		match permission {
			Permission::Group(permission) => Self::Group(group::Set::from_permission(permission)),
			Permission::Object(permission) => {
				Self::Object(object::Set::from_permission(permission))
			},
			Permission::Organization(permission) => {
				Self::Organization(organization::Set::from_permission(permission))
			},
			Permission::Process(permission) => {
				Self::Process(process::Set::from_permission(permission))
			},
			Permission::Sandbox(permission) => {
				Self::Sandbox(sandbox::Set::from_permission(permission))
			},
			Permission::Tag(permission) => Self::Tag(tag::Set::from_permission(permission)),
			Permission::User(permission) => Self::User(user::Set::from_permission(permission)),
		}
	}

	#[must_use]
	pub fn contains(self, other: impl Into<Self>) -> bool {
		let other = other.into();
		match (self, other) {
			(Self::Group(this), Self::Group(other)) => this.contains(other),
			(Self::Object(this), Self::Object(other)) => this.contains(other),
			(Self::Organization(this), Self::Organization(other)) => this.contains(other),
			(Self::Process(this), Self::Process(other)) => this.contains(other),
			(Self::Sandbox(this), Self::Sandbox(other)) => this.contains(other),
			(Self::Tag(this), Self::Tag(other)) => this.contains(other),
			(Self::User(this), Self::User(other)) => this.contains(other),
			_ => false,
		}
	}

	#[must_use]
	pub fn kind(self) -> tg::grant::resource::Kind {
		match self {
			Self::Group(_) => tg::grant::resource::Kind::Group,
			Self::Object(_) => tg::grant::resource::Kind::Object,
			Self::Organization(_) => tg::grant::resource::Kind::Organization,
			Self::Process(_) => tg::grant::resource::Kind::Process,
			Self::Sandbox(_) => tg::grant::resource::Kind::Sandbox,
			Self::Tag(_) => tg::grant::resource::Kind::Tag,
			Self::User(_) => tg::grant::resource::Kind::User,
		}
	}

	#[must_use]
	pub fn empty_like(self) -> Self {
		Self::empty_for_kind(self.kind())
	}

	#[must_use]
	pub fn empty_for_kind(kind: tg::grant::resource::Kind) -> Self {
		match kind {
			tg::grant::resource::Kind::Group => Self::Group(group::Set::empty()),
			tg::grant::resource::Kind::Object => Self::Object(object::Set::empty()),
			tg::grant::resource::Kind::Organization => {
				Self::Organization(organization::Set::empty())
			},
			tg::grant::resource::Kind::Process => Self::Process(process::Set::empty()),
			tg::grant::resource::Kind::Sandbox => Self::Sandbox(sandbox::Set::empty()),
			tg::grant::resource::Kind::Tag => Self::Tag(tag::Set::empty()),
			tg::grant::resource::Kind::User => Self::User(user::Set::empty()),
		}
	}

	#[must_use]
	pub fn is_empty(self) -> bool {
		match self {
			Self::Group(permissions) => permissions.is_empty(),
			Self::Object(permissions) => permissions.is_empty(),
			Self::Organization(permissions) => permissions.is_empty(),
			Self::Process(permissions) => permissions.is_empty(),
			Self::Sandbox(permissions) => permissions.is_empty(),
			Self::Tag(permissions) => permissions.is_empty(),
			Self::User(permissions) => permissions.is_empty(),
		}
	}

	pub fn insert(&mut self, other: Self) {
		match (self, other) {
			(Self::Group(this), Self::Group(other)) => this.insert(other),
			(Self::Object(this), Self::Object(other)) => this.insert(other),
			(Self::Organization(this), Self::Organization(other)) => this.insert(other),
			(Self::Process(this), Self::Process(other)) => this.insert(other),
			(Self::Sandbox(this), Self::Sandbox(other)) => this.insert(other),
			(Self::Tag(this), Self::Tag(other)) => this.insert(other),
			(Self::User(this), Self::User(other)) => this.insert(other),
			_ => {},
		}
	}

	pub fn remove(&mut self, other: Self) {
		match (self, other) {
			(Self::Group(this), Self::Group(other)) => this.remove(other),
			(Self::Object(this), Self::Object(other)) => this.remove(other),
			(Self::Organization(this), Self::Organization(other)) => this.remove(other),
			(Self::Process(this), Self::Process(other)) => this.remove(other),
			(Self::Sandbox(this), Self::Sandbox(other)) => this.remove(other),
			(Self::Tag(this), Self::Tag(other)) => this.remove(other),
			(Self::User(this), Self::User(other)) => this.remove(other),
			_ => {},
		}
	}

	#[must_use]
	pub fn same_kind(self, other: Self) -> bool {
		self.kind() == other.kind()
	}

	pub fn iter(self) -> impl Iterator<Item = Permission> {
		let entries = match self {
			Self::Group(permissions) => Self::group_entries(permissions),
			Self::Object(permissions) => [
				permissions
					.contains(object::Set::NODE)
					.then_some(Permission::Object(object::Permission::Node)),
				permissions
					.contains(object::Set::SUBTREE)
					.then_some(Permission::Object(object::Permission::Subtree)),
				None,
				None,
				None,
				None,
				None,
				None,
				None,
				None,
				None,
			],
			Self::Organization(permissions) => Self::organization_entries(permissions),
			Self::Process(permissions) => [
				Self::process_entry(permissions, process::Permission::Node),
				Self::process_entry(permissions, process::Permission::NodeCommand),
				Self::process_entry(permissions, process::Permission::NodeError),
				Self::process_entry(permissions, process::Permission::NodeLog),
				Self::process_entry(permissions, process::Permission::NodeOutput),
				Self::process_entry(permissions, process::Permission::Subtree),
				Self::process_entry(permissions, process::Permission::SubtreeCommand),
				Self::process_entry(permissions, process::Permission::SubtreeError),
				Self::process_entry(permissions, process::Permission::SubtreeLog),
				Self::process_entry(permissions, process::Permission::SubtreeOutput),
				Self::process_entry(permissions, process::Permission::Write),
			],
			Self::Sandbox(permissions) => Self::sandbox_entries(permissions),
			Self::Tag(permissions) => Self::tag_entries(permissions),
			Self::User(permissions) => Self::user_entries(permissions),
		};
		entries.into_iter().flatten()
	}

	fn group_entries(permissions: group::Set) -> [Option<Permission>; 11] {
		[
			Self::group_entry(permissions, group::Permission::Read),
			Self::group_entry(permissions, group::Permission::Write),
			Self::group_entry(permissions, group::Permission::Admin),
			None,
			None,
			None,
			None,
			None,
			None,
			None,
			None,
		]
	}

	fn group_entry(permissions: group::Set, permission: group::Permission) -> Option<Permission> {
		permissions
			.contains(group::Set::from_permission(permission))
			.then_some(Permission::Group(permission))
	}

	fn organization_entries(permissions: organization::Set) -> [Option<Permission>; 11] {
		[
			Self::organization_entry(permissions, organization::Permission::Read),
			Self::organization_entry(permissions, organization::Permission::Write),
			Self::organization_entry(permissions, organization::Permission::Admin),
			None,
			None,
			None,
			None,
			None,
			None,
			None,
			None,
		]
	}

	fn organization_entry(
		permissions: organization::Set,
		permission: organization::Permission,
	) -> Option<Permission> {
		permissions
			.contains(organization::Set::from_permission(permission))
			.then_some(Permission::Organization(permission))
	}

	fn process_entry(
		permissions: process::Set,
		permission: process::Permission,
	) -> Option<Permission> {
		permissions
			.contains(process::Set::from_permission(permission))
			.then_some(Permission::Process(permission))
	}

	fn sandbox_entries(permissions: sandbox::Set) -> [Option<Permission>; 11] {
		[
			Self::sandbox_entry(permissions, sandbox::Permission::Read),
			Self::sandbox_entry(permissions, sandbox::Permission::Write),
			None,
			None,
			None,
			None,
			None,
			None,
			None,
			None,
			None,
		]
	}

	fn sandbox_entry(
		permissions: sandbox::Set,
		permission: sandbox::Permission,
	) -> Option<Permission> {
		permissions
			.contains(sandbox::Set::from_permission(permission))
			.then_some(Permission::Sandbox(permission))
	}

	fn tag_entries(permissions: tag::Set) -> [Option<Permission>; 11] {
		[
			Self::tag_entry(permissions, tag::Permission::Read),
			Self::tag_entry(permissions, tag::Permission::Write),
			Self::tag_entry(permissions, tag::Permission::Admin),
			None,
			None,
			None,
			None,
			None,
			None,
			None,
			None,
		]
	}

	fn tag_entry(permissions: tag::Set, permission: tag::Permission) -> Option<Permission> {
		permissions
			.contains(tag::Set::from_permission(permission))
			.then_some(Permission::Tag(permission))
	}

	fn user_entries(permissions: user::Set) -> [Option<Permission>; 11] {
		[
			Self::user_entry(permissions, user::Permission::Read),
			Self::user_entry(permissions, user::Permission::Write),
			Self::user_entry(permissions, user::Permission::Admin),
			None,
			None,
			None,
			None,
			None,
			None,
			None,
			None,
		]
	}

	fn user_entry(permissions: user::Set, permission: user::Permission) -> Option<Permission> {
		permissions
			.contains(user::Set::from_permission(permission))
			.then_some(Permission::User(permission))
	}
}

impl From<Permission> for Set {
	fn from(permission: Permission) -> Self {
		Self::from_permission(permission)
	}
}

impl std::fmt::Display for Set {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let string = self
			.iter()
			.map(|permission| permission.to_string())
			.collect::<Vec<_>>()
			.join(",");
		f.write_str(&string)
	}
}

impl std::str::FromStr for Set {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		let mut permissions: Option<Self> = None;
		for part in s.split(',') {
			if part.is_empty() {
				return Err(tg::error!("invalid grant permissions"));
			}
			let permission = part.parse::<Permission>()?;
			let next = Self::from_permission(permission);
			match &mut permissions {
				Some(permissions) if permissions.same_kind(next) => permissions.insert(next),
				Some(_) => return Err(tg::error!("invalid grant permissions")),
				None => permissions = Some(next),
			}
		}
		permissions.ok_or_else(|| tg::error!("invalid grant permissions"))
	}
}

impl std::str::FromStr for Permission {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		if let Some(s) = s.strip_prefix("group_") {
			let permission = s
				.parse()
				.map_err(|_| tg::error!("invalid grant permission"))?;
			return Ok(Self::Group(permission));
		}
		if let Some(s) = s.strip_prefix("object_") {
			let permission = s
				.parse()
				.map_err(|_| tg::error!("invalid grant permission"))?;
			return Ok(Self::Object(permission));
		}
		if let Some(s) = s.strip_prefix("organization_") {
			let permission = s
				.parse()
				.map_err(|_| tg::error!("invalid grant permission"))?;
			return Ok(Self::Organization(permission));
		}
		if let Some(s) = s.strip_prefix("process_") {
			let permission = s
				.parse()
				.map_err(|_| tg::error!("invalid grant permission"))?;
			return Ok(Self::Process(permission));
		}
		if let Some(s) = s.strip_prefix("sandbox_") {
			let permission = s
				.parse()
				.map_err(|_| tg::error!("invalid grant permission"))?;
			return Ok(Self::Sandbox(permission));
		}
		if let Some(s) = s.strip_prefix("tag_") {
			let permission = s
				.parse()
				.map_err(|_| tg::error!("invalid grant permission"))?;
			return Ok(Self::Tag(permission));
		}
		if let Some(s) = s.strip_prefix("user_") {
			let permission = s
				.parse()
				.map_err(|_| tg::error!("invalid grant permission"))?;
			return Ok(Self::User(permission));
		}
		Err(tg::error!("invalid grant permission"))
	}
}

#[cfg(test)]
mod tests {
	use super::{Permission, group, object, organization, process, sandbox, tag, user};
	use crate as tg;

	#[test]
	fn display_from_str_round_trip() {
		let permissions = [
			(Permission::Group(group::Permission::Admin), "group_admin"),
			(Permission::Group(group::Permission::Read), "group_read"),
			(Permission::Group(group::Permission::Write), "group_write"),
			(Permission::Object(object::Permission::Node), "object_node"),
			(
				Permission::Object(object::Permission::Subtree),
				"object_subtree",
			),
			(
				Permission::Organization(organization::Permission::Admin),
				"organization_admin",
			),
			(
				Permission::Organization(organization::Permission::Read),
				"organization_read",
			),
			(
				Permission::Organization(organization::Permission::Write),
				"organization_write",
			),
			(
				Permission::Process(process::Permission::Node),
				"process_node",
			),
			(
				Permission::Process(process::Permission::NodeCommand),
				"process_node_command",
			),
			(
				Permission::Process(process::Permission::NodeError),
				"process_node_error",
			),
			(
				Permission::Process(process::Permission::NodeLog),
				"process_node_log",
			),
			(
				Permission::Process(process::Permission::NodeOutput),
				"process_node_output",
			),
			(
				Permission::Process(process::Permission::Subtree),
				"process_subtree",
			),
			(
				Permission::Process(process::Permission::SubtreeCommand),
				"process_subtree_command",
			),
			(
				Permission::Process(process::Permission::SubtreeError),
				"process_subtree_error",
			),
			(
				Permission::Process(process::Permission::SubtreeLog),
				"process_subtree_log",
			),
			(
				Permission::Process(process::Permission::SubtreeOutput),
				"process_subtree_output",
			),
			(
				Permission::Process(process::Permission::Write),
				"process_write",
			),
			(
				Permission::Sandbox(sandbox::Permission::Read),
				"sandbox_read",
			),
			(
				Permission::Sandbox(sandbox::Permission::Write),
				"sandbox_write",
			),
			(Permission::Tag(tag::Permission::Admin), "tag_admin"),
			(Permission::Tag(tag::Permission::Read), "tag_read"),
			(Permission::Tag(tag::Permission::Write), "tag_write"),
			(Permission::User(user::Permission::Admin), "user_admin"),
			(Permission::User(user::Permission::Read), "user_read"),
			(Permission::User(user::Permission::Write), "user_write"),
		];
		for (permission, string) in permissions {
			assert_eq!(permission.to_string(), string);
			assert_eq!(string.parse::<Permission>().unwrap(), permission);
		}
	}

	#[test]
	fn implies() {
		assert!(
			Permission::Group(group::Permission::Admin)
				.implies(Permission::Group(group::Permission::Read))
		);
		assert!(
			Permission::Group(group::Permission::Admin)
				.implies(Permission::Group(group::Permission::Write))
		);
		assert!(
			Permission::Group(group::Permission::Write)
				.implies(Permission::Group(group::Permission::Read))
		);
		assert!(
			!Permission::Group(group::Permission::Read)
				.implies(Permission::Group(group::Permission::Write))
		);
		assert!(
			!Permission::Group(group::Permission::Write)
				.implies(Permission::Group(group::Permission::Admin))
		);
		assert!(
			Permission::Object(object::Permission::Subtree)
				.implies(Permission::Object(object::Permission::Node))
		);
		assert!(
			!Permission::Object(object::Permission::Node)
				.implies(Permission::Object(object::Permission::Subtree))
		);
		assert!(
			Permission::Process(process::Permission::SubtreeOutput)
				.implies(Permission::Process(process::Permission::NodeOutput))
		);
		assert!(
			!Permission::Process(process::Permission::SubtreeOutput)
				.implies(Permission::Process(process::Permission::NodeLog))
		);
		assert!(
			!Permission::Process(process::Permission::Subtree)
				.implies(Permission::Process(process::Permission::NodeOutput))
		);
		assert!(
			Permission::Process(process::Permission::Write)
				.implies(Permission::Process(process::Permission::Write))
		);
		assert!(
			Permission::Process(process::Permission::Write)
				.implies(Permission::Process(process::Permission::Node))
		);
		assert!(
			!Permission::Process(process::Permission::Subtree)
				.implies(Permission::Process(process::Permission::Write))
		);
		assert!(
			!Permission::Group(group::Permission::Read)
				.implies(Permission::Object(object::Permission::Node))
		);
		assert!(
			!Permission::Group(group::Permission::Admin)
				.implies(Permission::Process(process::Permission::Node))
		);
	}

	#[test]
	fn parse_for_kind() {
		assert_eq!(
			super::Set::parse_for_kind(tg::grant::resource::Kind::Group, "read,write").unwrap(),
			super::Set::Group({
				let mut set = super::group::Set::empty();
				set.insert(super::group::Set::READ);
				set.insert(super::group::Set::WRITE);
				set
			})
		);

		assert!(super::Set::parse_for_kind(tg::grant::resource::Kind::Sandbox, "admin").is_err());
	}
}
