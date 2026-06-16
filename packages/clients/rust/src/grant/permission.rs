use crate::prelude::*;

pub mod object;
pub mod process;

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
	Admin,

	#[display("object_{_0}")]
	Object(object::Permission),

	#[display("process_{_0}")]
	Process(process::Permission),

	Read,

	Write,
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
	Resource(ResourceSet),
	Object(object::Set),
	Process(process::Set),
}

bitflags::bitflags! {
	#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
	pub struct ResourceSet: u8 {
		const READ = 1 << 0;
		const WRITE = 1 << 1;
		const ADMIN = 1 << 2;
	}
}

impl Permission {
	#[must_use]
	pub fn implies(self, needed: Self) -> bool {
		match (self, needed) {
			(Self::Admin, Self::Admin | Self::Write | Self::Read)
			| (Self::Write, Self::Write | Self::Read)
			| (Self::Read, Self::Read) => true,
			(Self::Object(granted), Self::Object(needed)) => granted.implies(needed),
			(Self::Process(granted), Self::Process(needed)) => granted.implies(needed),
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
	#[must_use]
	pub fn from_permission(permission: Permission) -> Self {
		match permission {
			Permission::Admin => Self::Resource(ResourceSet::ADMIN),
			Permission::Read => Self::Resource(ResourceSet::READ),
			Permission::Write => Self::Resource(ResourceSet::WRITE),
			Permission::Object(permission) => {
				Self::Object(object::Set::from_permission(permission))
			},
			Permission::Process(permission) => {
				Self::Process(process::Set::from_permission(permission))
			},
		}
	}

	#[must_use]
	pub fn contains(self, other: impl Into<Self>) -> bool {
		let other = other.into();
		match (self, other) {
			(Self::Resource(this), Self::Resource(other)) => this.contains(other),
			(Self::Object(this), Self::Object(other)) => this.contains(other),
			(Self::Process(this), Self::Process(other)) => this.contains(other),
			_ => false,
		}
	}

	#[must_use]
	pub fn empty_like(self) -> Self {
		match self {
			Self::Resource(_) => Self::Resource(ResourceSet::empty()),
			Self::Object(_) => Self::Object(object::Set::empty()),
			Self::Process(_) => Self::Process(process::Set::empty()),
		}
	}

	#[must_use]
	pub fn is_empty(self) -> bool {
		match self {
			Self::Resource(permissions) => permissions.is_empty(),
			Self::Object(permissions) => permissions.is_empty(),
			Self::Process(permissions) => permissions.is_empty(),
		}
	}

	pub fn insert(&mut self, other: Self) {
		match (self, other) {
			(Self::Resource(this), Self::Resource(other)) => this.insert(other),
			(Self::Object(this), Self::Object(other)) => this.insert(other),
			(Self::Process(this), Self::Process(other)) => this.insert(other),
			_ => {},
		}
	}

	pub fn remove(&mut self, other: Self) {
		match (self, other) {
			(Self::Resource(this), Self::Resource(other)) => this.remove(other),
			(Self::Object(this), Self::Object(other)) => this.remove(other),
			(Self::Process(this), Self::Process(other)) => this.remove(other),
			_ => {},
		}
	}

	#[must_use]
	pub fn same_kind(self, other: Self) -> bool {
		matches!(
			(self, other),
			(Self::Resource(_), Self::Resource(_))
				| (Self::Object(_), Self::Object(_))
				| (Self::Process(_), Self::Process(_))
		)
	}

	pub fn iter(self) -> impl Iterator<Item = Permission> {
		let entries = match self {
			Self::Resource(permissions) => [
				permissions
					.contains(ResourceSet::READ)
					.then_some(Permission::Read),
				permissions
					.contains(ResourceSet::WRITE)
					.then_some(Permission::Write),
				permissions
					.contains(ResourceSet::ADMIN)
					.then_some(Permission::Admin),
				None,
				None,
				None,
				None,
				None,
				None,
				None,
			],
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
			],
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
			],
		};
		entries.into_iter().flatten()
	}

	fn process_entry(
		permissions: process::Set,
		permission: process::Permission,
	) -> Option<Permission> {
		permissions
			.contains(process::Set::from_permission(permission))
			.then_some(Permission::Process(permission))
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
		if let Some(s) = s.strip_prefix("object_") {
			let permission = s
				.parse()
				.map_err(|_| tg::error!("invalid grant permission"))?;
			return Ok(Self::Object(permission));
		}
		if let Some(s) = s.strip_prefix("process_") {
			let permission = s
				.parse()
				.map_err(|_| tg::error!("invalid grant permission"))?;
			return Ok(Self::Process(permission));
		}
		match s {
			"admin" => Ok(Self::Admin),
			"read" => Ok(Self::Read),
			"write" => Ok(Self::Write),
			_ => Err(tg::error!("invalid grant permission")),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::{Permission, object, process};

	#[test]
	fn display_from_str_round_trip() {
		let permissions = [
			(Permission::Admin, "admin"),
			(Permission::Read, "read"),
			(Permission::Write, "write"),
			(Permission::Object(object::Permission::Node), "object_node"),
			(
				Permission::Object(object::Permission::Subtree),
				"object_subtree",
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
		];
		for (permission, string) in permissions {
			assert_eq!(permission.to_string(), string);
			assert_eq!(string.parse::<Permission>().unwrap(), permission);
		}
	}

	#[test]
	fn implies() {
		assert!(Permission::Admin.implies(Permission::Read));
		assert!(Permission::Admin.implies(Permission::Write));
		assert!(Permission::Write.implies(Permission::Read));
		assert!(!Permission::Read.implies(Permission::Write));
		assert!(!Permission::Write.implies(Permission::Admin));
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
		assert!(!Permission::Read.implies(Permission::Object(object::Permission::Node)));
		assert!(!Permission::Admin.implies(Permission::Process(process::Permission::Node)));
	}
}
