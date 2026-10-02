use tangram_client::prelude::*;

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct Arg {
	#[tangram_serialize(id = 0)]
	pub expires_at: i64,
	#[tangram_serialize(id = 1)]
	pub resource: tg::Id,
	#[tangram_serialize(id = 2)]
	pub source: tg::authorization::Subject,
	#[tangram_serialize(id = 3)]
	pub subject: tg::authorization::Subject,
	#[tangram_serialize(id = 4)]
	pub version: Option<String>,
}

impl Arg {
	pub fn validate(&self) -> tg::Result<()> {
		if self.version.is_some() && !matches!(self.subject, tg::authorization::Subject::Tag(_)) {
			return Err(tg::error!("a versioned delegation must have a tag subject"));
		}
		if tg::object::Id::try_from(self.resource.clone()).is_err()
			&& self.resource.kind() != tg::id::Kind::Process
		{
			return Err(tg::error!("a delegation root must be an object or process"));
		}
		Ok(())
	}
}
