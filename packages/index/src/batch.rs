use tangram_client::prelude::*;

/// An ordered sequence of mutations that executes in one transaction, which other args may share.
#[derive(Clone, Debug, Default, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct Arg {
	#[tangram_serialize(default, id = 0, skip_serializing_if = "Vec::is_empty")]
	pub items: Vec<Item>,
}

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub enum Item {
	#[tangram_serialize(id = 0)]
	DeleteGrant(crate::grant::delete::Arg),

	#[tangram_serialize(id = 1)]
	DeleteGroup(tg::group::Id),

	#[tangram_serialize(id = 2)]
	DeleteGroupMember(crate::group::member::delete::Arg),

	#[tangram_serialize(id = 3)]
	DeleteOrganization(tg::organization::Id),

	#[tangram_serialize(id = 4)]
	DeleteOrganizationMember(crate::organization::member::delete::Arg),

	#[tangram_serialize(id = 5)]
	DeleteSandbox(tg::sandbox::Id),

	#[tangram_serialize(id = 6)]
	DeleteTag(tg::tag::Id),

	#[tangram_serialize(id = 7)]
	DeleteUser(tg::user::Id),

	#[tangram_serialize(id = 8)]
	PutCacheEntry(crate::cache::put::Arg),

	#[tangram_serialize(id = 9)]
	PutGrant(crate::grant::put::Arg),

	#[tangram_serialize(id = 10)]
	PutGroup(crate::group::put::Arg),

	#[tangram_serialize(id = 11)]
	PutGroupMember(crate::group::member::put::Arg),

	#[tangram_serialize(id = 12)]
	PutObject(crate::object::put::Arg),

	#[tangram_serialize(id = 13)]
	PutOrganization(crate::organization::put::Arg),

	#[tangram_serialize(id = 14)]
	PutOrganizationMember(crate::organization::member::put::Arg),

	#[tangram_serialize(id = 15)]
	PutProcess(crate::process::put::Arg),

	#[tangram_serialize(id = 16)]
	PutRunner(crate::runner::put::Arg),

	#[tangram_serialize(id = 17)]
	PutSandbox(crate::sandbox::put::Arg),

	#[tangram_serialize(id = 18)]
	PutTag(crate::tag::put::Arg),

	#[tangram_serialize(id = 19)]
	PutUser(crate::user::put::Arg),
}

impl Arg {
	pub fn deserialize(bytes: &[u8]) -> tg::Result<Self> {
		tangram_serialize::from_slice(bytes)
			.map_err(|error| tg::error!(!error, "failed to deserialize the index batch arg"))
	}

	pub fn serialize(&self) -> tg::Result<Vec<u8>> {
		tangram_serialize::to_vec(self)
			.map_err(|error| tg::error!(!error, "failed to serialize the index batch arg"))
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
		let group = tg::group::Id::new();
		let organization = tg::organization::Id::new();
		let sandbox = tg::sandbox::Id::new();
		let tag = tg::tag::Id::new();
		let user = tg::user::Id::new();
		let data = tg::sandbox::get::Output {
			cpu: Some(2),
			creator: Some(tg::Principal::User(user.clone())),
			hostname: Some("host".into()),
			id: sandbox.clone(),
			isolation: Some(tg::sandbox::Isolation::Container),
			location: None,
			memory: Some(1024),
			mounts: Vec::new(),
			network: Some(tg::sandbox::Network::Bridge(tg::sandbox::Bridge {
				ports: vec!["127.0.0.1:8080:80/udp".parse().unwrap()],
			})),
			owner: Some(tg::Principal::Root),
			status: tg::sandbox::Status::Started,
			ttl: Some(std::time::Duration::new(60, 123)),
		};
		let arg = Arg {
			items: vec![
				Item::DeleteGrant(crate::grant::delete::Arg {
					creator: Some(tg::Principal::Root),
					expires_at: None,
					permissions: tg::grant::permission::Set::Group(
						tg::grant::permission::group::Set::READ,
					),
					principal: tg::grant::Principal::Root,
					resource: group.clone().into(),
				}),
				Item::DeleteGroup(group.clone()),
				Item::DeleteOrganization(organization.clone()),
				Item::DeleteTag(tag),
				Item::DeleteUser(user.clone()),
				Item::PutGrant(crate::grant::put::Arg {
					created_at: 1,
					creator: Some(tg::Principal::Root),
					expires_at: None,
					permissions: tg::grant::permission::Set::Group(
						tg::grant::permission::group::Set::READ,
					),
					principal: tg::grant::Principal::Root,
					resource: group.clone().into(),
					time_to_touch: Some(std::time::Duration::new(30, 456)),
				}),
				Item::PutGroupMember(crate::group::member::put::Arg {
					group,
					member: tg::group::Member::User(user.clone()),
				}),
				Item::PutOrganizationMember(crate::organization::member::put::Arg {
					member: tg::organization::Member::User(user),
					organization,
				}),
				Item::PutSandbox(crate::sandbox::put::Arg {
					created_at: 1,
					data: Some(data),
					id: sandbox,
					runner: None,
					touched_at: 2,
				}),
			],
		};
		let bytes = arg.serialize().unwrap();
		let arg = Arg::deserialize(&bytes).unwrap();
		assert_eq!(arg.items.len(), 9);
		let Item::PutGrant(grant_arg) = &arg.items[5] else {
			panic!();
		};
		assert_eq!(
			grant_arg.time_to_touch,
			Some(std::time::Duration::new(30, 456))
		);
		assert!(matches!(&arg.items[6], Item::PutGroupMember(_)));
		assert!(matches!(&arg.items[7], Item::PutOrganizationMember(_)));
		let Item::PutSandbox(sandbox_arg) = &arg.items[8] else {
			panic!();
		};
		let data = sandbox_arg.data.as_ref().unwrap();
		assert_eq!(data.cpu, Some(2));
		assert_eq!(data.ttl, Some(std::time::Duration::new(60, 123)));
		assert_eq!(data.network.as_ref().unwrap().ports().len(), 1);
	}
}
