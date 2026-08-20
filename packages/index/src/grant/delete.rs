use tangram_client::prelude::*;

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
#[allow(clippy::option_option)]
pub struct Arg {
	#[tangram_serialize(id = 0)]
	pub creator: Option<tg::Principal>,
	#[tangram_serialize(id = 1)]
	pub implicit: Option<Option<i64>>,
	#[tangram_serialize(id = 2)]
	pub permissions: tg::authorization::permission::Set,
	#[tangram_serialize(id = 3)]
	pub subject: tg::authorization::Subject,
	#[tangram_serialize(id = 4)]
	pub resource: tg::Id,
}
