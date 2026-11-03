use {crate::prelude::*, tangram_uri::Uri};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Data {
	pub id: tg::remote::Id,

	pub name: String,

	#[serde(default, skip_serializing_if = "tg::Principal::is_root")]
	pub principal: tg::Principal,

	#[serde(default)]
	pub trusted: bool,

	pub url: Uri,
}
