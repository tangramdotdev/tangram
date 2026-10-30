use tangram_client::prelude::*;

/// A collection of independent index items, which may execute in any order across multiple transactions.
#[derive(Clone, Debug, Default, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct Arg {
	#[tangram_serialize(default, id = 0, skip_serializing_if = "Vec::is_empty")]
	pub items: Vec<crate::Item>,
}

impl Arg {
	pub fn deserialize(bytes: &[u8]) -> tg::Result<Self> {
		tangram_serialize::from_slice(bytes)
			.map_err(|error| tg::error!(!error, "failed to deserialize the index write arg"))
	}

	pub fn serialize(&self) -> tg::Result<Vec<u8>> {
		tangram_serialize::to_vec(self)
			.map_err(|error| tg::error!(!error, "failed to serialize the index write arg"))
	}

	#[must_use]
	pub fn is_empty(&self) -> bool {
		self.items.is_empty()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn serialization_roundtrip() {
		// Create the batch items.
		let group = tg::group::Id::new();
		let organization = tg::organization::Id::new();
		let sandbox = tg::sandbox::Id::new();
		let tag = tg::tag::Id::new();
		let user = tg::user::Id::new();
		let data = tg::sandbox::get::Output {
			data: tg::sandbox::Data {
				cpu: Some(2.into()),
				creator: Some(tg::Principal::User(user.clone())),
				error: None,
				hostname: Some("host".into()),
				id: sandbox.clone(),
				memory: Some(1024.into()),
				mounts: Vec::new(),
				network: Some(tg::sandbox::Network::Bridge(tg::sandbox::Bridge {
					ports: vec!["127.0.0.1:8080:80/udp".parse().unwrap()],
				})),
				owner: Some(tg::Principal::Root),
				status: tg::sandbox::Status::Destroyed,
				storage: None,
				ttl: Some(std::time::Duration::new(60, 123)),
				usage: Some(tg::sandbox::Usage {
					cpu: tg::sandbox::ResourceUsage {
						consumption: 123,
						reservation: 0,
					},
					memory: tg::sandbox::ResourceUsage {
						consumption: 456,
						reservation: 0,
					},
					storage: tg::sandbox::ResourceUsage::default(),
				}),
			},
			location: None,
			tokens: tg::authorization::Tokens::default(),
		};
		let arg = Arg {
			items: vec![
				crate::Item::DeleteCheckout(tag.clone().into()),
				crate::Item::DeletePermission(crate::permission::delete::Arg {
					creator: Some(tg::Principal::Root),
					permissions: tg::authorization::permission::Set::Group(
						tg::authorization::permission::group::Set::READ,
					),
					resource: group.clone().into(),
					source: crate::permission::Source::Grant,
					subject: tg::authorization::Subject::Root,
				}),
				crate::Item::DeleteGroup(group.clone()),
				crate::Item::DeleteOrganization(organization.clone()),
				crate::Item::DeleteTag(tag),
				crate::Item::DeleteUser(user.clone()),
				crate::Item::PutPermission(crate::permission::put::Arg {
					created_at: 1,
					creator: Some(tg::Principal::Root),
					permissions: tg::authorization::permission::Set::Group(
						tg::authorization::permission::group::Set::READ,
					),
					resource: group.clone().into(),
					source: crate::permission::Source::Grant,
					subject: tg::authorization::Subject::Root,
					time_to_touch: Some(std::time::Duration::new(30, 456)),
					version: None,
				}),
				crate::Item::PutGroupMember(crate::group::member::put::Arg {
					group,
					member: tg::group::Member::User(user.clone()),
				}),
				crate::Item::PutOrganizationMember(crate::organization::member::put::Arg {
					member: tg::organization::Member::User(user),
					organization,
				}),
				crate::Item::PutSandbox(crate::sandbox::put::Arg {
					account: None,
					created_at: 1,
					data: Some(data),
					id: sandbox.clone(),
					location: None,
					permissions: Vec::new(),
					principal: tg::Principal::Sandbox(sandbox.clone()),
					processes: None,
					runner: None,
					touched_at: 2,
				}),
			],
		};

		// Serialize and deserialize the batch.
		let bytes = arg.serialize().unwrap();
		let arg = Arg::deserialize(&bytes).unwrap();

		// Verify the deserialized items.
		assert_eq!(arg.items.len(), 10);
		let crate::Item::PutPermission(permission_arg) = &arg.items[6] else {
			panic!();
		};
		assert_eq!(
			permission_arg.time_to_touch,
			Some(std::time::Duration::new(30, 456))
		);
		assert!(matches!(&arg.items[7], crate::Item::PutGroupMember(_)));
		assert!(matches!(
			&arg.items[8],
			crate::Item::PutOrganizationMember(_)
		));
		let crate::Item::PutSandbox(sandbox_arg) = &arg.items[9] else {
			panic!();
		};
		let data = sandbox_arg.data.as_ref().unwrap();
		assert_eq!(data.data.cpu, Some(2.into()));
		assert_eq!(data.data.ttl, Some(std::time::Duration::new(60, 123)));
		assert_eq!(data.data.usage.as_ref().unwrap().cpu.consumption, 123);
		assert_eq!(data.data.usage.as_ref().unwrap().memory.consumption, 456);
		assert_eq!(data.data.network.as_ref().unwrap().ports().len(), 1);
	}
}
