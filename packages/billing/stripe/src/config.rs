use {
	serde_with::{DurationSecondsWithFrac, serde_as},
	std::time::Duration,
	tangram_client::prelude::*,
	tangram_uri::Uri,
};

#[derive(Clone, Debug)]
pub struct Config {
	pub secret_key: String,
	pub url: Uri,
	pub webhook_secret: String,
	pub webhook_tolerance: Duration,
}

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct Options {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub secret_key: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub url: Option<Uri>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub webhook_secret: Option<String>,

	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub webhook_tolerance: Option<Duration>,
}

impl Default for Config {
	fn default() -> Self {
		Self {
			secret_key: String::new(),
			url: "https://api.stripe.com".parse().unwrap(),
			webhook_secret: String::new(),
			webhook_tolerance: Duration::from_mins(5),
		}
	}
}

impl TryFrom<Options> for Config {
	type Error = tg::Error;

	fn try_from(source: Options) -> tg::Result<Self> {
		let secret_key = required(source.secret_key, "billing.stripe.secret_key")?;
		let webhook_secret = required(source.webhook_secret, "billing.stripe.webhook_secret")?;
		let mut target = Self {
			secret_key,
			webhook_secret,
			..Self::default()
		};
		if let Some(value) = source.url {
			target.url = value;
		}
		if let Some(value) = source.webhook_tolerance {
			target.webhook_tolerance = value;
		}

		Ok(target)
	}
}

fn required<T>(value: Option<T>, field: &'static str) -> tg::Result<T> {
	let value = value.ok_or_else(|| tg::error!(%field, "a required config field is missing"))?;
	Ok(value)
}
