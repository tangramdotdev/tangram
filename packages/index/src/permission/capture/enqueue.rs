use tangram_client::prelude::*;

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct Arg {
	#[tangram_serialize(id = 0)]
	pub resource: tg::Id,
}

impl Arg {
	pub fn validate(&self) -> tg::Result<()> {
		if !self.resource.kind().is_object() && self.resource.kind() != tg::id::Kind::Process {
			return Err(tg::error!("invalid permission capture queue resource"));
		}
		Ok(())
	}
}
