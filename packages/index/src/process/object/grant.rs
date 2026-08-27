use tangram_client::prelude::*;

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct Arg {
	#[tangram_serialize(id = 6)]
	pub authorize: crate::authorize::Config,

	#[tangram_serialize(id = 0)]
	pub created_at: i64,

	#[tangram_serialize(id = 1)]
	pub expires_at: Option<i64>,

	#[tangram_serialize(id = 2)]
	pub principal: tg::Principal,

	#[tangram_serialize(id = 3)]
	pub process: tg::process::Id,

	#[tangram_serialize(id = 4)]
	pub roots: Vec<Root>,

	#[tangram_serialize(id = 5)]
	pub time_to_touch: Option<std::time::Duration>,
}

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct Root {
	#[tangram_serialize(id = 0)]
	pub object: tg::object::Id,

	#[tangram_serialize(id = 1)]
	pub permissions: Option<tg::authorization::permission::Set>,
}

impl Arg {
	pub fn validate(&self) -> tg::Result<()> {
		self.authorize.validate()?;
		if self.expires_at.is_none()
			&& self.principal != tg::Principal::Process(self.process.clone())
		{
			return Err(tg::error!(
				"non-expiring process object grants must be authorized by the process"
			));
		}
		if self.roots.iter().any(|root| {
			root.permissions.is_some_and(|permissions| {
				!matches!(permissions, tg::authorization::permission::Set::Object(_))
			})
		}) {
			return Err(tg::error!(
				"process object grant roots must contain object permissions"
			));
		}

		Ok(())
	}
}
