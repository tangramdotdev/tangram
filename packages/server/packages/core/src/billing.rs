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

	fn configured(&self) -> bool;

	fn create_customer(
		&self,
		arg: tangram_billing::customer::create::Arg,
	) -> BoxFuture<'_, tg::Result<String>>;

	fn create_management_url<'a>(&'a self, customer: &'a str) -> BoxFuture<'a, tg::Result<String>>;

	fn request<'a>(
		&'a self,
		session: &'a Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>>;
}
