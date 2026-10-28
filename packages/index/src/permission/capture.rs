use tangram_client::prelude::*;

pub mod enqueue;

/// The permissions, delegations, and capture that grant a resource a principal's authority over a set of roots.
#[derive(Clone, Debug, Default, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct Captures {
	#[tangram_serialize(default, id = 0, skip_serializing_if = "Vec::is_empty")]
	pub delegations: Vec<crate::delegation::put::Arg>,

	#[tangram_serialize(default, id = 1, skip_serializing_if = "Vec::is_empty")]
	pub enqueues: Vec<enqueue::Arg>,

	#[tangram_serialize(default, id = 2, skip_serializing_if = "Vec::is_empty")]
	pub permissions: Vec<crate::permission::put::Arg>,
}

#[derive(Clone, Debug)]
pub struct Entry {
	pub arg: enqueue::Arg,
	pub partition: u64,
}

pub fn validate_permissions(
	resource: &tg::Id,
	permissions: tg::authorization::permission::Set,
) -> tg::Result<()> {
	let allowed = if tg::object::Id::try_from(resource.clone()).is_ok() {
		{
			let mut allowed = tg::authorization::permission::object::Set::NODE;
			allowed.insert(tg::authorization::permission::object::Set::SUBTREE);
			tg::authorization::permission::Set::Object(allowed)
		}
	} else if tg::process::Id::try_from(resource.clone()).is_ok() {
		{
			let mut allowed = tg::authorization::permission::process::Set::all();
			allowed.remove(tg::authorization::permission::process::Set::PARENT);
			tg::authorization::permission::Set::Process(allowed)
		}
	} else {
		return Err(tg::error!(
			"a permission capture resource must be an object or process"
		));
	};
	if !allowed.contains(permissions) {
		return Err(tg::error!(
			"a permission capture permission must be a read permission"
		));
	}
	Ok(())
}

impl Captures {
	#[must_use]
	pub fn is_empty(&self) -> bool {
		self.delegations.is_empty() && self.enqueues.is_empty() && self.permissions.is_empty()
	}

	pub fn extend(&mut self, other: Self) {
		self.delegations.extend(other.delegations);
		self.enqueues.extend(other.enqueues);
		self.permissions.extend(other.permissions);
	}

	/// Convert the captures into independent batch items, for a resource that is already indexed.
	pub fn into_items(self) -> impl Iterator<Item = crate::Item> {
		let permissions = self.permissions.into_iter().map(crate::Item::PutPermission);
		let delegations = self.delegations.into_iter().map(crate::Item::PutDelegation);
		let enqueues = self
			.enqueues
			.into_iter()
			.map(crate::Item::EnqueuePermissionCapture);
		permissions.chain(delegations).chain(enqueues)
	}
}
