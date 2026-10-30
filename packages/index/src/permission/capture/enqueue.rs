use tangram_client::prelude::*;

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct Arg {
	#[tangram_serialize(id = 0)]
	pub id: Vec<u8>,
	#[tangram_serialize(id = 1)]
	pub principal: tg::Principal,
	#[tangram_serialize(id = 2)]
	pub resource: tg::Id,
	#[tangram_serialize(id = 3)]
	pub roots: Vec<tg::Referent<tg::Id>>,
	#[tangram_serialize(id = 4)]
	pub version: Option<String>,
}

impl Arg {
	/// Get the process that grants authority and the other process that receives it, for a capture from one process to another.
	#[must_use]
	pub fn process_grant(&self) -> Option<(tg::process::Id, tg::process::Id)> {
		let tg::Principal::Process(source) = &self.principal else {
			return None;
		};
		let destination = tg::process::Id::try_from(self.resource.clone()).ok()?;
		(*source != destination).then(|| (source.clone(), destination))
	}

	pub fn validate(&self) -> tg::Result<()> {
		if self.id.len() != 16 {
			return Err(tg::error!("invalid permission capture identity"));
		}
		match self.resource.kind() {
			tg::id::Kind::Process if self.version.is_none() => {},
			tg::id::Kind::Tag if self.version.is_some() => {},
			_ => return Err(tg::error!("invalid permission capture resource or version")),
		}
		for root in &self.roots {
			if !root.node.kind().is_object()
				&& (self.resource.kind() == tg::id::Kind::Process
					|| root.node.kind() != tg::id::Kind::Process)
			{
				return Err(tg::error!("invalid permission capture root"));
			}
		}
		Ok(())
	}
}
