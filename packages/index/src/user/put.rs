use tangram_client::prelude::*;

#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct Arg {
	#[tangram_serialize(id = 0)]
	pub id: tg::user::Id,
	#[tangram_serialize(id = 1)]
	pub specifier: tg::Specifier,
}
