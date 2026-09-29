use {tangram_client::prelude::*, tangram_uri::Uri};

#[derive(Clone, Debug)]
pub struct Config {
	pub secret_key: String,
	pub url: Uri,
	pub webhook_secret: String,
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Options {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub secret_key: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub url: Option<Uri>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub webhook_secret: Option<String>,
}

impl TryFrom<Options> for Config {
	type Error = tg::Error;

	fn try_from(source: Options) -> tg::Result<Self> {
		let secret_key = required(source.secret_key, "billing.stripe.secret_key")?;
		let url = source
			.url
			.unwrap_or_else(|| "https://api.stripe.com".parse().unwrap());
		let webhook_secret = required(source.webhook_secret, "billing.stripe.webhook_secret")?;
		let target = Self {
			secret_key,
			url,
			webhook_secret,
		};

		Ok(target)
	}
}

fn required<T>(value: Option<T>, field: &'static str) -> tg::Result<T> {
	let value = value.ok_or_else(|| tg::error!(%field, "a required config field is missing"))?;
	Ok(value)
}
