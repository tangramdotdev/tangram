use tangram_client::prelude::*;

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct Arg {
	#[tangram_serialize(id = 0)]
	pub created_at: i64,
	#[tangram_serialize(id = 1)]
	pub creator: Option<tg::Principal>,
	#[tangram_serialize(id = 3)]
	pub permissions: tg::authorization::permission::Set,
	#[tangram_serialize(id = 5)]
	pub resource: tg::Id,
	#[tangram_serialize(id = 2)]
	pub source: super::Source,
	#[tangram_serialize(id = 4)]
	pub subject: tg::authorization::Subject,
	#[tangram_serialize(id = 6)]
	pub time_to_touch: Option<std::time::Duration>,
	#[tangram_serialize(id = 7)]
	pub version: Option<String>,
}

impl Arg {
	pub fn validate(&self) -> tg::Result<()> {
		if self.version.is_some() && !matches!(self.subject, tg::authorization::Subject::Tag(_)) {
			return Err(tg::error!("a versioned permission must have a tag subject"));
		}
		if matches!(self.source, super::Source::Direct { expires_at: None }) {
			match &self.subject {
				tg::authorization::Subject::Process(process) => {
					if self.creator.as_ref() != Some(&tg::Principal::Process(process.clone())) {
						return Err(tg::error!(
							"a non-expiring direct permission must be created by its process"
						));
					}
					if !self.resource.kind().is_object() {
						return Err(tg::error!(
							"a non-expiring process permission must target an object"
						));
					}
				},
				tg::authorization::Subject::Tag(_) => {
					if self.version.is_none() || self.creator.is_some() {
						return Err(tg::error!(
							"a non-expiring tag permission requires a version and no creator"
						));
					}
				},
				_ => {
					return Err(tg::error!(
						"a non-expiring direct permission must have a process or tag subject"
					));
				},
			}
			super::capture::validate_permissions(&self.resource, self.permissions)?;
		}
		Ok(())
	}
}
