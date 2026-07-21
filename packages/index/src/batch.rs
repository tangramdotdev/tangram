use tangram_client::prelude::*;

#[derive(Clone, Debug, Default, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct Arg {
	#[tangram_serialize(default, id = 0, skip_serializing_if = "Vec::is_empty")]
	pub delete_grants: Vec<crate::grant::delete::Arg>,
	#[tangram_serialize(default, id = 1, skip_serializing_if = "Vec::is_empty")]
	pub delete_group_members: Vec<crate::group::member::delete::Arg>,
	#[tangram_serialize(default, id = 2, skip_serializing_if = "Vec::is_empty")]
	pub delete_groups: Vec<tg::group::Id>,
	#[tangram_serialize(default, id = 3, skip_serializing_if = "Vec::is_empty")]
	pub delete_organization_members: Vec<crate::organization::member::delete::Arg>,
	#[tangram_serialize(default, id = 4, skip_serializing_if = "Vec::is_empty")]
	pub delete_organizations: Vec<tg::organization::Id>,
	#[tangram_serialize(default, id = 5, skip_serializing_if = "Vec::is_empty")]
	pub delete_sandboxes: Vec<tg::sandbox::Id>,
	#[tangram_serialize(default, id = 6, skip_serializing_if = "Vec::is_empty")]
	pub delete_tags: Vec<tg::tag::Id>,
	#[tangram_serialize(default, id = 7, skip_serializing_if = "Vec::is_empty")]
	pub delete_users: Vec<tg::user::Id>,
	#[tangram_serialize(default, id = 8, skip_serializing_if = "Vec::is_empty")]
	pub put_cache_entries: Vec<crate::cache::put::Arg>,
	#[tangram_serialize(default, id = 9, skip_serializing_if = "Vec::is_empty")]
	pub put_grants: Vec<crate::grant::put::Arg>,
	#[tangram_serialize(default, id = 10, skip_serializing_if = "Vec::is_empty")]
	pub put_group_members: Vec<crate::group::member::put::Arg>,
	#[tangram_serialize(default, id = 11, skip_serializing_if = "Vec::is_empty")]
	pub put_groups: Vec<crate::group::put::Arg>,
	#[tangram_serialize(default, id = 12, skip_serializing_if = "Vec::is_empty")]
	pub put_objects: Vec<crate::object::put::Arg>,
	#[tangram_serialize(default, id = 13, skip_serializing_if = "Vec::is_empty")]
	pub put_organization_members: Vec<crate::organization::member::put::Arg>,
	#[tangram_serialize(default, id = 14, skip_serializing_if = "Vec::is_empty")]
	pub put_organizations: Vec<crate::organization::put::Arg>,
	#[tangram_serialize(default, id = 15, skip_serializing_if = "Vec::is_empty")]
	pub put_processes: Vec<crate::process::put::Arg>,
	#[tangram_serialize(default, id = 16, skip_serializing_if = "Vec::is_empty")]
	pub put_runners: Vec<crate::runner::put::Arg>,
	#[tangram_serialize(default, id = 17, skip_serializing_if = "Vec::is_empty")]
	pub put_sandboxes: Vec<crate::sandbox::put::Arg>,
	#[tangram_serialize(default, id = 18, skip_serializing_if = "Vec::is_empty")]
	pub put_tags: Vec<crate::tag::put::Arg>,
	#[tangram_serialize(default, id = 19, skip_serializing_if = "Vec::is_empty")]
	pub put_users: Vec<crate::user::put::Arg>,
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
		self.delete_grants.is_empty()
			&& self.delete_group_members.is_empty()
			&& self.delete_groups.is_empty()
			&& self.delete_organization_members.is_empty()
			&& self.delete_organizations.is_empty()
			&& self.delete_sandboxes.is_empty()
			&& self.delete_tags.is_empty()
			&& self.delete_users.is_empty()
			&& self.put_cache_entries.is_empty()
			&& self.put_grants.is_empty()
			&& self.put_group_members.is_empty()
			&& self.put_groups.is_empty()
			&& self.put_objects.is_empty()
			&& self.put_organization_members.is_empty()
			&& self.put_organizations.is_empty()
			&& self.put_processes.is_empty()
			&& self.put_runners.is_empty()
			&& self.put_sandboxes.is_empty()
			&& self.put_tags.is_empty()
			&& self.put_users.is_empty()
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
			delete_grants: vec![crate::grant::delete::Arg {
				creator: Some(tg::Principal::Root),
				expires_at: None,
				permissions: tg::grant::permission::Set::Group(
					tg::grant::permission::group::Set::READ,
				),
				principal: tg::grant::Principal::Root,
				resource: group.clone().into(),
			}],
			delete_groups: vec![group.clone()],
			delete_organizations: vec![organization.clone()],
			delete_tags: vec![tag],
			delete_users: vec![user.clone()],
			put_grants: vec![crate::grant::put::Arg {
				created_at: 1,
				creator: Some(tg::Principal::Root),
				expires_at: None,
				permissions: tg::grant::permission::Set::Group(
					tg::grant::permission::group::Set::READ,
				),
				principal: tg::grant::Principal::Root,
				resource: group.clone().into(),
				time_to_touch: Some(std::time::Duration::new(30, 456)),
			}],
			put_group_members: vec![crate::group::member::put::Arg {
				group,
				member: tg::group::Member::User(user.clone()),
			}],
			put_organization_members: vec![crate::organization::member::put::Arg {
				member: tg::organization::Member::User(user),
				organization,
			}],
			put_sandboxes: vec![crate::sandbox::put::Arg {
				created_at: 1,
				data: Some(data),
				id: sandbox,
				runner: None,
				touched_at: 2,
			}],
			..Default::default()
		};
		let bytes = arg.serialize().unwrap();
		let arg = Arg::deserialize(&bytes).unwrap();
		assert!(!arg.is_empty());
		assert_eq!(arg.delete_grants.len(), 1);
		assert_eq!(
			arg.put_grants[0].time_to_touch,
			Some(std::time::Duration::new(30, 456))
		);
		assert_eq!(arg.put_group_members.len(), 1);
		assert_eq!(arg.put_organization_members.len(), 1);
		let data = arg.put_sandboxes[0].data.as_ref().unwrap();
		assert_eq!(data.cpu, Some(2));
		assert_eq!(data.ttl, Some(std::time::Duration::new(60, 123)));
		assert_eq!(data.network.as_ref().unwrap().ports().len(), 1);
	}
}
