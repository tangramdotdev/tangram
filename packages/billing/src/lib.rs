use tangram_client as tg;

pub mod customer;
pub mod webhook;

pub trait Billing {
	/// Create a customer idempotently for the account so callers can safely retry.
	fn create_customer(
		&self,
		arg: customer::create::Arg,
	) -> impl Future<Output = tg::Result<String>> + Send;

	fn create_management_url(
		&self,
		customer: &str,
	) -> impl Future<Output = tg::Result<String>> + Send;

	fn customer_ready(&self, customer: &str) -> impl Future<Output = tg::Result<bool>> + Send;

	/// Verify and parse a webhook without making network requests. Return `None` for ignored events.
	fn try_parse_webhook(
		&self,
		headers: &http::HeaderMap,
		body: &[u8],
		now: i64,
	) -> tg::Result<Option<webhook::Event>>;
}
