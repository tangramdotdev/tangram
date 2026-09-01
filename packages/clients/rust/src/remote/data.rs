use tangram_uri::Uri;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Data {
	pub name: String,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub token: Option<String>,

	#[serde(default)]
	pub trusted: bool,

	pub url: Uri,
}
