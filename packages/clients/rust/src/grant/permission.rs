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
