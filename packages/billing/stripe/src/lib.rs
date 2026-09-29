use {
	aws_lc_rs::hmac, data_encoding::HEXLOWER, std::collections::BTreeMap,
	tangram_client::prelude::*,
};

#[cfg(test)]
mod tests;

pub use config::Config;

pub mod config;

const WEBHOOK_TOLERANCE: u64 = 300;

#[derive(Clone)]
pub struct Billing {
	client: reqwest::Client,
	secret_key: String,
	url: String,
	webhook_secret: String,
}

#[derive(serde::Deserialize)]
struct Customer {
	#[serde(default)]
	deleted: bool,

	id: String,

	#[serde(default)]
	invoice_settings: InvoiceSettings,
}

#[derive(Default, serde::Deserialize)]
struct InvoiceSettings {
	default_payment_method: Option<String>,
}

#[derive(serde::Deserialize)]
struct PortalSession {
	url: String,
}

#[derive(serde::Deserialize)]
struct Error {
	error: ErrorData,
}

#[derive(serde::Deserialize)]
struct ErrorData {
	message: String,
}

#[derive(serde::Deserialize)]
struct Event {
	data: EventData,
	id: String,

	#[serde(rename = "type")]
	type_: String,
}

#[derive(serde::Deserialize)]
struct EventData {
	object: serde_json::Value,

	#[serde(default)]
	previous_attributes: serde_json::Value,
}

impl Billing {
	#[must_use]
	pub fn new(config: &Config) -> Self {
		Self {
			client: reqwest::Client::new(),
			secret_key: config.secret_key.clone(),
			url: config.url.to_string().trim_end_matches('/').to_owned(),
			webhook_secret: config.webhook_secret.clone(),
		}
	}

	pub async fn create_customer(
		&self,
		arg: tangram_billing::customer::create::Arg,
	) -> tg::Result<String> {
		// Create the parameters.
		let (kind, id) = match arg.account {
			tg::usage::Account::Organization(id) => ("organization", id.to_string()),
			tg::usage::Account::User(id) => ("user", id.to_string()),
		};
		let idempotency_key = format!("tangram-{kind}-{id}");
		let mut params = BTreeMap::new();
		if let Some(email) = arg.email {
			params.insert("email".to_owned(), email);
		}
		params.insert("name".to_owned(), arg.name);
		params.insert(format!("metadata[tangram_{kind}_id]"), id);

		// Create the customer.
		let url = format!("{}/v1/customers", self.url);
		let response = self
			.client
			.post(url)
			.basic_auth(&self.secret_key, Some(""))
			.header("Idempotency-Key", idempotency_key)
			.form(&params)
			.send()
			.await
			.map_err(|error| tg::error!(!error, "failed to send the Stripe request"))?;
		let customer: Customer = Self::parse_response(response).await?;

		Ok(customer.id)
	}

	pub async fn create_management_url(&self, customer: &str) -> tg::Result<String> {
		// Create the parameters.
		let params = BTreeMap::from([
			("customer", customer),
			("flow_data[after_completion][type]", "hosted_confirmation"),
			("flow_data[type]", "payment_method_update"),
		]);

		// Create the portal session.
		let url = format!("{}/v1/billing_portal/sessions", self.url);
		let response = self
			.client
			.post(url)
			.basic_auth(&self.secret_key, Some(""))
			.form(&params)
			.send()
			.await
			.map_err(|error| tg::error!(!error, "failed to send the Stripe request"))?;
		let session: PortalSession = Self::parse_response(response).await?;

		Ok(session.url)
	}

	pub async fn customer_ready(&self, customer: &str) -> tg::Result<bool> {
		let url = format!("{}/v1/customers/{customer}", self.url);
		let response = self
			.client
			.get(url)
			.basic_auth(&self.secret_key, Some(""))
			.send()
			.await
			.map_err(|error| tg::error!(!error, "failed to send the Stripe request"))?;
		let customer: Customer = Self::parse_response(response).await?;
		let default_payment_method = if customer.deleted {
			None
		} else {
			customer.invoice_settings.default_payment_method
		};

		Ok(default_payment_method.is_some())
	}

	pub fn try_parse_webhook(
		&self,
		headers: &http::HeaderMap,
		body: &[u8],
		now: i64,
	) -> tg::Result<Option<tangram_billing::webhook::Event>> {
		let signature = headers
			.get("stripe-signature")
			.and_then(|value| value.to_str().ok())
			.ok_or_else(|| tg::error!("missing the Stripe signature"))?;
		self.verify_webhook_signature(signature, body, now)?;
		let event: Event = serde_json::from_slice(body).map_err(|error| {
			tg::error!(!error, "failed to deserialize the Stripe webhook event")
		})?;
		if !matches!(
			event.type_.as_str(),
			"customer.updated" | "payment_method.attached" | "payment_method.detached"
		) {
			return Ok(None);
		}
		let customer = match event.type_.as_str() {
			"customer.updated" => event.data.object.get("id").and_then(|value| value.as_str()),
			"payment_method.attached" | "payment_method.detached" => event
				.data
				.object
				.get("customer")
				.and_then(|value| value.as_str())
				.or_else(|| {
					event
						.data
						.previous_attributes
						.get("customer")
						.and_then(|value| value.as_str())
				}),
			_ => None,
		}
		.map(str::to_owned);
		let event = tangram_billing::webhook::Event {
			customer,
			id: event.id,
		};

		Ok(Some(event))
	}

	fn verify_webhook_signature(&self, header: &str, payload: &[u8], now: i64) -> tg::Result<()> {
		// Parse the signature header.
		let mut signatures = Vec::new();
		let mut timestamp = None;
		for component in header.split(',') {
			let Some((key, value)) = component.split_once('=') else {
				continue;
			};
			match key {
				"t" if timestamp.is_none() => {
					let parsed = value
						.parse::<i64>()
						.map_err(|error| tg::error!(!error, "invalid Stripe timestamp"))?;
					timestamp = Some((parsed, value));
				},
				"v1" => signatures.push(value),
				_ => (),
			}
		}
		let Some((timestamp, timestamp_string)) = timestamp else {
			return Err(tg::error!("missing the Stripe timestamp"));
		};
		if signatures.is_empty() {
			return Err(tg::error!("missing the Stripe signature"));
		}

		// Validate the timestamp.
		if now.abs_diff(timestamp) > WEBHOOK_TOLERANCE {
			return Err(tg::error!("the Stripe signature has expired"));
		}

		// Verify a signature.
		let mut signed_payload = timestamp_string.as_bytes().to_vec();
		signed_payload.push(b'.');
		signed_payload.extend_from_slice(payload);
		let key = hmac::Key::new(hmac::HMAC_SHA256, self.webhook_secret.as_bytes());
		let valid = signatures.into_iter().any(|signature| {
			let Ok(signature) = HEXLOWER.decode(signature.as_bytes()) else {
				return false;
			};
			hmac::verify(&key, &signed_payload, &signature).is_ok()
		});
		if !valid {
			return Err(tg::error!("invalid Stripe signature"));
		}

		Ok(())
	}

	async fn parse_response<T>(response: reqwest::Response) -> tg::Result<T>
	where
		T: serde::de::DeserializeOwned,
	{
		let status = response.status();
		if !status.is_success() {
			let error = response.json::<Error>().await.map_err(
				|error| tg::error!(!error, %status, "failed to deserialize the Stripe error response"),
			)?;
			return Err(tg::error!(%status, "stripe request failed: {}", error.error.message));
		}
		let output = response
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the Stripe response"))?;

		Ok(output)
	}
}

impl tangram_billing::Billing for Billing {
	async fn create_customer(
		&self,
		arg: tangram_billing::customer::create::Arg,
	) -> tg::Result<String> {
		self.create_customer(arg).await
	}

	async fn create_management_url(&self, customer: &str) -> tg::Result<String> {
		self.create_management_url(customer).await
	}

	async fn customer_ready(&self, customer: &str) -> tg::Result<bool> {
		self.customer_ready(customer).await
	}

	fn try_parse_webhook(
		&self,
		headers: &http::HeaderMap,
		body: &[u8],
		now: i64,
	) -> tg::Result<Option<tangram_billing::webhook::Event>> {
		self.try_parse_webhook(headers, body, now)
	}
}
