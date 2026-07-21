use {
	crate::duration::{
		deserialize_option as deserialize_duration, serialize_option as serialize_duration,
	},
	tangram_client::prelude::*,
};

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct Arg {
	#[tangram_serialize(id = 0)]
	pub created_at: i64,
	#[tangram_serialize(id = 1)]
	pub creator: Option<tg::Principal>,
	#[tangram_serialize(id = 2)]
	pub expires_at: Option<i64>,
	#[tangram_serialize(id = 3)]
	pub permissions: tg::grant::permission::Set,
	#[tangram_serialize(id = 4)]
	pub principal: tg::grant::Principal,
	#[tangram_serialize(id = 5)]
	pub resource: tg::Id,
	#[tangram_serialize(
		deserialize_with = "deserialize_duration",
		id = 6,
		serialize_with = "serialize_duration"
	)]
	pub time_to_touch: Option<std::time::Duration>,
}
