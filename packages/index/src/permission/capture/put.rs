use tangram_client::prelude::*;

#[derive(
	Clone, Debug, Eq, PartialEq, tangram_serialize::Deserialize, tangram_serialize::Serialize,
)]
pub struct Arg {
	#[tangram_serialize(id = 0)]
	pub principal: tg::Principal,
	#[tangram_serialize(id = 1)]
	pub resource: tg::Referent<tg::Id>,
	#[tangram_serialize(id = 2)]
	pub subject: tg::Id,
	#[tangram_serialize(id = 3)]
	pub version: Option<String>,
}

impl Arg {
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
