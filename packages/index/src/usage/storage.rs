use tangram_client::{
	authorization::permission::{Set, object, process},
	prelude::*,
};

pub mod put;

#[derive(
	Clone, Debug, Eq, PartialEq, tangram_serialize::Deserialize, tangram_serialize::Serialize,
)]
pub struct Entry {
	#[tangram_serialize(id = 2)]
	pub permissions: tg::authorization::permission::Set,

	// Cleanup recomputes this count; zero also marks an invalidated cache.
	#[tangram_serialize(id = 0)]
	pub reference_count: u64,

	#[tangram_serialize(id = 1)]
	pub touched_at: i64,
}

#[must_use]
pub fn child_permissions(
	permissions: tg::authorization::permission::Set,
) -> tg::authorization::permission::Set {
	match permissions {
		Set::Object(permissions) => Set::Object(if permissions.contains(object::Set::SUBTREE) {
			object::Set::SUBTREE
		} else {
			object::Set::empty()
		}),
		Set::Process(permissions) => {
			let mut output = process::Set::empty();
			for permission in permissions.iter() {
				if permission == permission.to_subtree()
					&& permission != process::Permission::Parent
				{
					output.insert(process::Set::from_permission(permission));
				}
			}
			Set::Process(output)
		},
		_ => permissions,
	}
}

#[must_use]
pub fn object_permissions(
	permissions: tg::authorization::permission::Set,
	kind: crate::process::object::Kind,
) -> tg::authorization::permission::Set {
	let required = match kind {
		crate::process::object::Kind::Command => process::Permission::NodeCommandObjects,
		crate::process::object::Kind::Error => process::Permission::NodeErrorObjects,
		crate::process::object::Kind::Log => process::Permission::NodeLogObjects,
		crate::process::object::Kind::Output => process::Permission::NodeOutputObjects,
	};
	let permitted = permissions
		.iter()
		.any(|permission| permission.implies(tg::authorization::Permission::Process(required)));
	Set::Object(if permitted {
		object::Set::SUBTREE
	} else {
		object::Set::empty()
	})
}

impl Entry {
	#[must_use]
	pub fn stores_node(&self) -> bool {
		let required = match self.permissions {
			tg::authorization::permission::Set::Object(_) => tg::authorization::Permission::Object(
				tg::authorization::permission::object::Permission::Node,
			),
			tg::authorization::permission::Set::Process(_) => {
				tg::authorization::Permission::Process(
					tg::authorization::permission::process::Permission::Node,
				)
			},
			_ => return false,
		};
		self.permissions
			.iter()
			.any(|permission| permission.implies(required))
	}

	pub fn deserialize(bytes: &[u8]) -> tg::Result<Self> {
		tangram_serialize::from_slice(bytes)
			.map_err(|error| tg::error!(!error, "failed to deserialize the storage entry"))
	}

	pub fn serialize(&self) -> tg::Result<Vec<u8>> {
		tangram_serialize::to_vec(self)
			.map_err(|error| tg::error!(!error, "failed to serialize the storage entry"))
	}
}
