use {serde_with::serde_as, tangram_client::prelude::*, tangram_util::serde::CommaSeparatedString};

pub mod read;
pub mod write;

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde_as(as = "CommaSeparatedString")]
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub streams: Vec<tg::process::stdio::Stream>,
}

fn stdio_uri(id: &tg::process::Id, arg: &Arg) -> tg::Result<String> {
	let query = serde_urlencoded::to_string(arg)
		.map_err(|source| tg::error!(!source, "failed to serialize the arg"))?;
	let uri = if query.is_empty() {
		format!("/processes/{id}/stdio")
	} else {
		format!("/processes/{id}/stdio?{query}")
	};
	Ok(uri)
}
