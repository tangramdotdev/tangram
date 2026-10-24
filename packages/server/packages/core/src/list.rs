use {
	crate::Session, futures::future::BoxFuture, tangram_client::prelude::*,
	tangram_http::body::Boxed as BoxBody,
};

pub mod remote;

pub struct QuerySpecifierEntriesArg<
	'a,
	F = Box<dyn FnOnce(Vec<tg::list::Entry>) -> Vec<tg::list::Entry> + Send + 'a>,
> {
	pub cached: bool,
	pub filter_local: F,
	pub location: Option<&'a tg::location::Arg>,
	pub query: self::remote::Query,
	pub tokens: &'a tg::authorization::Tokens,
	pub ttl: tg::remote::cache::Ttl,
}

/// The list component.
pub trait Component: crate::component::Lifecycle {
	fn query_specifier_entries<'a>(
		&'a self,
		session: &'a Session,
		arg: QuerySpecifierEntriesArg<'a>,
	) -> BoxFuture<'a, tg::Result<Vec<tg::list::Entry>>>;

	fn list<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::list::Arg,
	) -> BoxFuture<'a, tg::Result<tg::list::Output>>;

	fn request<'a>(
		&'a self,
		session: &'a Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>>;

	fn list_inner<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::list::Arg,
	) -> BoxFuture<'a, tg::Result<Vec<tg::list::Entry>>>;

	fn list_local_entries<'a>(
		&'a self,
		session: &'a Session,
	) -> BoxFuture<'a, tg::Result<Vec<tg::list::Entry>>>;
}

#[must_use]
pub fn tokens_for_remote(
	tokens: &tg::authorization::Tokens,
	remote: &str,
) -> tg::authorization::Tokens {
	let mut output = tg::authorization::Tokens::default();
	for (location, tokens) in tokens.iter() {
		let tg::Location::Remote(location) = location else {
			continue;
		};
		if location.name == remote {
			let location = tg::Location::Local(tg::location::Local {
				region: location.region.clone(),
			});
			output.set(location, tokens.clone());
		}
	}

	output
}
