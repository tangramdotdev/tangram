use {
	tangram_client::prelude::*,
	tg::authorization::permission::{Set, object, process},
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

#[must_use]
pub fn retained_permissions(stored: Set, retaining: Set) -> Set {
	let mut output = stored.empty_like();
	for stored in stored.iter() {
		for retaining in retaining.iter() {
			// Keep the narrower permission, including permissions implied by a subtree.
			if stored.implies(retaining) {
				output.insert(retaining.into());
			} else if retaining.implies(stored) {
				output.insert(stored.into());
			}
		}
	}
	output
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

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn retained_permissions_preserve_only_shared_access() {
		let objects = [object::Permission::Node, object::Permission::Subtree];
		let processes = process::Set::all().iter().collect::<Vec<_>>();
		let mut cases = objects
			.into_iter()
			.map(|permission| Set::Object(object::Set::from_permission(permission)))
			.collect::<Vec<_>>();
		cases.push(Set::Object(object::Set::empty()));
		cases.extend(
			processes
				.iter()
				.map(|permission| Set::Process(process::Set::from_permission(*permission))),
		);
		cases.push(Set::Process(process::Set::empty()));
		cases.push(Set::Process(process::Set::all()));
		cases.push(Set::Process(
			vec![
				process::Permission::NodeLogObjects,
				process::Permission::SubtreeOutputObjects,
			]
			.into(),
		));
		cases.push(Set::Process(
			vec![
				process::Permission::SubtreeLogObjects,
				process::Permission::NodeOutputObjects,
			]
			.into(),
		));
		let permissions = objects
			.into_iter()
			.map(tg::authorization::Permission::Object)
			.chain(
				processes
					.into_iter()
					.map(tg::authorization::Permission::Process),
			)
			.collect::<Vec<_>>();
		for stored in &cases {
			for retaining in &cases {
				if stored.kind() != retaining.kind() {
					continue;
				}
				let retained = retained_permissions(*stored, *retaining);
				for permission in &permissions {
					let permitted =
						|set: Set| set.iter().any(|granted| granted.implies(*permission));
					assert_eq!(
						permitted(retained),
						permitted(*stored) && permitted(*retaining),
						"stored: {stored:?}, retaining: {retaining:?}, permission: {permission:?}"
					);
				}
			}
		}
	}
}
