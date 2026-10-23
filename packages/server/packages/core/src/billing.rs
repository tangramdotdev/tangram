use {
	crate::Session, futures::future::BoxFuture, tangram_client::prelude::*,
	tangram_http::body::Boxed as BoxBody,
};

/// The billing component.
pub trait Component: crate::component::Lifecycle {
	fn verify_billing<'a>(
		&'a self,
		session: &'a Session,
		owner: Option<&'a tg::Principal>,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn request<'a>(
		&'a self,
		session: &'a Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>>;
}

#[derive(Clone)]
pub enum Billing {
	Stripe(tangram_billing_stripe::Billing),
}

impl Billing {
	#[must_use]
	pub fn new(config: &crate::config::Billing) -> Self {
		Self::Stripe(tangram_billing_stripe::Billing::new(&config.stripe))
	}
}

impl tangram_billing::Billing for Billing {
	async fn create_customer(
		&self,
		arg: tangram_billing::customer::create::Arg,
	) -> tg::Result<String> {
		match self {
			Self::Stripe(billing) => billing.create_customer(arg).await,
		}
	}

	async fn create_management_url(&self, customer: &str) -> tg::Result<String> {
		match self {
			Self::Stripe(billing) => billing.create_management_url(customer).await,
		}
	}

	async fn customer_ready(&self, customer: &str) -> tg::Result<bool> {
		match self {
			Self::Stripe(billing) => billing.customer_ready(customer).await,
		}
	}

	fn try_parse_webhook(
		&self,
		headers: &http::HeaderMap,
		body: &[u8],
		now: i64,
	) -> tg::Result<Option<tangram_billing::webhook::Event>> {
		match self {
			Self::Stripe(billing) => billing.try_parse_webhook(headers, body, now),
		}
	}
}
