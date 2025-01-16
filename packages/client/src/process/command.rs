use crate as tg;
use std::collections::BTreeMap;

pub struct Output {
	pub process: tg::process::Id,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Command {
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub args: tg::value::data::Array,

	#[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
	pub env: tg::value::data::Map,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub executable: Option<tg::target::data::Executable>,

	pub host: String,
}

impl Command {
	pub async fn run(&self) -> tg::Result<tg::Process> {
		todo!()
	}
}

impl tg::Client {
	pub async fn try_run_command(
		&self,
		command: Command,
	) -> tg::Result<tg::process::command::Output> {
		todo!()
	}
}
