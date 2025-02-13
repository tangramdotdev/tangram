use crate::util::serde::is_false;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Metadata {
	#[serde(default, skip_serializing_if = "is_false")]
	pub commands_complete: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub commands_count: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub commands_depth: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub commands_weight: Option<u64>,

	#[serde(default, skip_serializing_if = "is_false")]
	pub complete: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub count: Option<u64>,

	#[serde(default, skip_serializing_if = "is_false")]
	pub logs_complete: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub logs_count: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub logs_depth: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub logs_weight: Option<u64>,

	#[serde(default, skip_serializing_if = "is_false")]
	pub outputs_complete: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub outputs_count: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub outputs_depth: Option<u64>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub outputs_weight: Option<u64>,
}
