use {
	crate::{Session, config},
	aws_lc_rs::hmac,
	data_encoding::HEXLOWER,
	futures::FutureExt as _,
	std::{collections::BTreeMap, ops::ControlFlow},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
	tangram_index::prelude::*,
};

const WEBHOOK_TOLERANCE: u64 = 300;

#[derive(Clone)]
pub(crate) struct Stripe {
	client: reqwest::Client,
	secret_key: String,
	url: String,
	webhook_secret: String,
}

pub(crate) struct CreateCustomerArg {
	pub email: Option<String>,
	pub idempotency_key: String,
	pub metadata: BTreeMap<String, String>,
	pub name: String,
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

#[derive(Clone)]
struct CustomerUpdate {
	customer: String,
	default_payment_method: Option<String>,
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

impl Stripe {
	#[must_use]
	pub fn new(config: &config::Stripe) -> Self {
		Self {
			client: reqwest::Client::new(),
			secret_key: config.secret_key.clone(),
			url: config.url.to_string().trim_end_matches('/').to_owned(),
			webhook_secret: config.webhook_secret.clone(),
		}
	}

	pub async fn create_customer(&self, arg: CreateCustomerArg) -> tg::Result<String> {
		// Create the parameters.
		let mut params = BTreeMap::new();
		if let Some(email) = arg.email {
			params.insert("email".to_owned(), email);
		}
		params.insert("name".to_owned(), arg.name);
		for (key, value) in arg.metadata {
			params.insert(format!("metadata[{key}]"), value);
		}

		// Create the customer.
		let url = format!("{}/v1/customers", self.url);
		let response = self
			.client
			.post(url)
			.basic_auth(&self.secret_key, Some(""))
			.header("Idempotency-Key", arg.idempotency_key)
			.form(&params)
			.send()
			.await
			.map_err(|error| tg::error!(!error, "failed to send the Stripe request"))?;
		let customer: Customer = Self::parse_response(response).await?;

		Ok(customer.id)
	}

	pub async fn create_payment_method_update(&self, customer: &str) -> tg::Result<String> {
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

	async fn get_customer(&self, customer: &str) -> tg::Result<Option<String>> {
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

		Ok(default_payment_method)
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

impl Session {
	pub(crate) async fn handle_stripe_webhook_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		if !self.server.is_primary_region() {
			return self.forward_request_to_primary_region(request).await;
		}

		// Get the signature and body.
		let signature = request
			.headers()
			.get("stripe-signature")
			.and_then(|value| value.to_str().ok())
			.map(str::to_owned);
		let body = request
			.bytes()
			.await
			.map_err(|error| tg::error!(!error, "failed to read the Stripe webhook body"))?;

		// Verify and deserialize the event.
		let stripe = self
			.server
			.billing
			.clone()
			.ok_or_else(|| tg::error!("billing is not configured"))?;
		let Some(signature) = signature else {
			return Ok(http::Response::builder()
				.bad_request()
				.empty()
				.unwrap()
				.boxed_body());
		};
		let now = self.server.clock.unix_timestamp()?;
		if let Err(error) = stripe.verify_webhook_signature(&signature, &body, now) {
			tracing::warn!(%error, "failed to verify the Stripe webhook signature");
			return Ok(http::Response::builder()
				.bad_request()
				.empty()
				.unwrap()
				.boxed_body());
		}
		let event = match serde_json::from_slice(&body) {
			Ok(event) => event,
			Err(error) => {
				tracing::warn!(%error, "failed to deserialize the Stripe webhook event");
				return Ok(http::Response::builder()
					.bad_request()
					.empty()
					.unwrap()
					.boxed_body());
			},
		};

		// Process the event.
		self.process_stripe_webhook(&stripe, event).await?;
		let response = http::Response::builder().ok().empty().unwrap().boxed_body();

		Ok(response)
	}

	async fn process_stripe_webhook(&self, stripe: &Stripe, event: Event) -> tg::Result<()> {
		// Ignore an unsupported event.
		if !matches!(
			event.type_.as_str(),
			"customer.updated" | "payment_method.attached" | "payment_method.detached"
		) {
			return Ok(());
		}

		// Skip a processed event.
		if self.is_stripe_webhook_event_processed(&event.id).await? {
			return Ok(());
		}

		// Reconcile the customer.
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
		let update = if let Some(customer) = customer {
			let default_payment_method = stripe.get_customer(&customer).await?;
			Some(CustomerUpdate {
				customer,
				default_payment_method,
			})
		} else {
			None
		};

		// Store the projection and event.
		let created_at = self.server.clock.unix_timestamp()?;
		let event = event.id;
		let server = self.server.clone();
		let batch = self
			.server
			.database
			.run(|transaction| {
				let event = event.clone();
				let server = server.clone();
				let update = update.clone();
				async move {
					Self::store_stripe_webhook_event_with_transaction(
						transaction,
						&server,
						&event,
						created_at,
						update.as_ref(),
					)
					.await
				}
				.boxed()
			})
			.await?;
		self.server
			.spawn_publish_database_outbox_notification_task();
		self.server.index.batch(batch).await?;

		Ok(())
	}

	async fn store_stripe_webhook_event_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		server: &crate::Server,
		event: &str,
		created_at: i64,
		update: Option<&CustomerUpdate>,
	) -> tg::Result<ControlFlow<tangram_index::batch::Arg, crate::database::Error>> {
		let mut batch = tangram_index::batch::Arg::default();
		if let Some(update) = update {
			#[derive(db::row::Deserialize)]
			struct OrganizationRow {
				#[tangram_database(as = "db::value::FromStr")]
				id: tg::organization::Id,
				#[tangram_database(as = "db::value::FromStr")]
				specifier: tg::Specifier,
			}

			#[derive(db::row::Deserialize)]
			struct UserRow {
				#[tangram_database(as = "db::value::FromStr")]
				id: tg::user::Id,
				#[tangram_database(as = "db::value::FromStr")]
				specifier: tg::Specifier,
			}

			let p = transaction.p();
			let statement = format!(
				"select organizations.id, specifiers.specifier from organizations join specifiers on specifiers.id = organizations.id where organizations.stripe_customer_id = {p}1;"
			);
			let result = transaction
				.query_all_into::<OrganizationRow>(
					statement.into(),
					db::params![update.customer.clone()],
				)
				.await;
			let organizations =
				crate::database::retry!(result, "failed to get the Stripe organizations");
			let statement = format!(
				"select users.id, specifiers.specifier from users join specifiers on specifiers.id = users.id where users.stripe_customer_id = {p}1;"
			);
			let result = transaction
				.query_all_into::<UserRow>(statement.into(), db::params![update.customer.clone()])
				.await;
			let users = crate::database::retry!(result, "failed to get the Stripe users");

			let billing = update.default_payment_method.is_some();
			batch.items.extend(organizations.into_iter().map(|row| {
				tangram_index::batch::Item::PutOrganization(tangram_index::organization::put::Arg {
					billing: Some(billing),
					id: row.id,
					specifier: row.specifier,
				})
			}));
			batch.items.extend(users.into_iter().map(|row| {
				tangram_index::batch::Item::PutUser(tangram_index::user::put::Arg {
					billing: Some(billing),
					id: row.id,
					specifier: row.specifier,
				})
			}));

			for table in ["organizations", "users"] {
				let statement = format!(
					"update {table} set stripe_default_payment_method_id = {p}1 where stripe_customer_id = {p}2;"
				);
				let result = transaction
					.execute(
						statement.into(),
						db::params![
							update.default_payment_method.clone(),
							update.customer.clone()
						],
					)
					.await;
				crate::database::retry!(result, "failed to update the Stripe customer");
			}
		}

		let p = transaction.p();
		let statement = format!(
			"insert into stripe_webhooks (id, created_at) values ({p}1, {p}2) on conflict (id) do nothing;"
		);
		let result = transaction
			.execute(statement.into(), db::params![event, created_at])
			.await;
		crate::database::retry!(result, "failed to record the Stripe webhook event");
		match server
			.enqueue_database_outbox_with_transaction(transaction, &batch)
			.await?
		{
			ControlFlow::Break(()) => (),
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}

		Ok(ControlFlow::Break(batch))
	}

	async fn is_stripe_webhook_event_processed(&self, event: &str) -> tg::Result<bool> {
		let event = event.to_owned();
		let processed = self
			.server
			.database
			.run_with_options(db::ConnectionOptions::default(), |transaction| {
				let event = event.clone();
				async move {
					Self::is_stripe_webhook_event_processed_with_transaction(transaction, &event)
						.await
				}
				.boxed()
			})
			.await?;

		Ok(processed)
	}

	async fn is_stripe_webhook_event_processed_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		event: &str,
	) -> tg::Result<ControlFlow<bool, crate::database::Error>> {
		let p = transaction.p();
		let statement = format!("select id from stripe_webhooks where id = {p}1;");
		let result = transaction
			.query_optional_value_into::<String>(statement.into(), db::params![event])
			.await;
		let processed =
			crate::database::retry!(result, "failed to get the Stripe webhook event").is_some();

		Ok(ControlFlow::Break(processed))
	}
}
