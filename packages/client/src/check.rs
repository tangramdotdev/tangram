use {
	crate as tg,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub module: tg::module::Data,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remote: Option<String>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub diagnostics: Vec<tg::Diagnostic>,
}

impl tg::Client {
	pub async fn check(&self, arg: Arg) -> tg::Result<tg::check::Output> {
		let method = http::Method::POST;
		let uri = "/check";
		let arg = serde_json::to_string(&arg)
			.map_err(|source| tg::error!(!source, "failed to serialize the arg"))?;
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.bytes(arg)
			.unwrap();
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let error = response.json().await?;
			return Err(error);
		}
		let output = response.json().await?;
		Ok(output)
	}
}
