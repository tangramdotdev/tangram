use {std::collections::BTreeSet, tangram_client::prelude::*};

pub mod enqueue;

/// A pending capture of a principal's permissions on a resource for a tag or process.
#[derive(
	Clone, Debug, Eq, PartialEq, tangram_serialize::Deserialize, tangram_serialize::Serialize,
)]
pub struct Capture {
	#[tangram_serialize(id = 0)]
	pub expires_at: i64,
	#[tangram_serialize(id = 1)]
	pub principal: tg::Principal,
	#[tangram_serialize(id = 2)]
	pub resource: tg::Referent<tg::Id>,
	#[tangram_serialize(id = 3)]
	pub sources: BTreeSet<tg::authorization::Subject>,
	#[tangram_serialize(id = 4)]
	pub subject: tg::Id,
	#[tangram_serialize(id = 5)]
	pub version: Option<String>,
}

#[derive(Clone, Debug)]
pub struct Entry {
	/// Identifies an enqueue so completion cannot remove a newer notification.
	pub attempt: u128,
	pub partition: u64,
	pub resource: tg::Id,
}

pub fn requested_permissions(resource: &tg::Id) -> tg::Result<tg::authorization::permission::Set> {
	let allowed = if resource.kind().is_object() {
		let mut allowed = tg::authorization::permission::object::Set::NODE;
		allowed.insert(tg::authorization::permission::object::Set::SUBTREE);
		tg::authorization::permission::Set::Object(allowed)
	} else if resource.kind() == tg::id::Kind::Process {
		let mut allowed = tg::authorization::permission::process::Set::all();
		allowed.remove(tg::authorization::permission::process::Set::PARENT);
		tg::authorization::permission::Set::Process(allowed)
	} else {
		return Err(tg::error!(
			"a permission capture resource must be an object or process"
		));
	};

	Ok(allowed)
}

pub fn validate_permissions(
	resource: &tg::Id,
	permissions: tg::authorization::permission::Set,
) -> tg::Result<()> {
	if !requested_permissions(resource)?.contains(permissions) {
		return Err(tg::error!(
			"a permission capture permission must be a read permission"
		));
	}
	Ok(())
}

impl Capture {
	/// Identifies the capture by its contents other than its expiration so that repeated captures extend one record.
	pub fn digest(&self) -> tg::Result<[u8; 32]> {
		let mut hasher = blake3::Hasher::new();
		let principal = tangram_serialize::to_vec(&self.principal);
		let resource = tangram_serialize::to_vec(&self.resource);
		let sources = tangram_serialize::to_vec(&self.sources);
		let subject = tangram_serialize::to_vec(&self.subject);
		let version = tangram_serialize::to_vec(&self.version);
		for bytes in [principal, resource, sources, subject, version] {
			let bytes =
				bytes.map_err(|error| tg::error!(!error, "failed to serialize the capture"))?;
			hasher.update(&bytes);
		}
		let digest = *hasher.finalize().as_bytes();
		Ok(digest)
	}

	pub fn validate(&self) -> tg::Result<()> {
		match self.subject.kind() {
			tg::id::Kind::Process if self.version.is_none() => {},
			tg::id::Kind::Tag if self.version.is_some() => {},
			_ => return Err(tg::error!("invalid permission capture subject or version")),
		}
		if !self.resource.node.kind().is_object()
			&& (self.subject.kind() == tg::id::Kind::Process
				|| self.resource.node.kind() != tg::id::Kind::Process)
		{
			return Err(tg::error!("invalid permission capture resource"));
		}
		Ok(())
	}
}
