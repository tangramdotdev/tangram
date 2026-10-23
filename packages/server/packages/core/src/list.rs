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

pub struct Kinds {
	pub groups: bool,
	pub organizations: bool,
	pub tags: bool,
	pub users: bool,
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
pub fn entry_kind_enabled(entry: &tg::list::Entry, kinds: &Kinds) -> bool {
	match entry.kind() {
		tg::id::Kind::Group => kinds.groups,
		tg::id::Kind::Organization => kinds.organizations,
		tg::id::Kind::Tag => kinds.tags,
		tg::id::Kind::User => kinds.users,
		_ => false,
	}
}

#[must_use]
pub fn sort_entries(mut data: Vec<tg::list::Entry>, reverse: bool) -> Vec<tg::list::Entry> {
	data.sort_by(|a, b| compare_entries(a, b, reverse));
	data
}

#[must_use]
pub fn compare_entries(
	a: &tg::list::Entry,
	b: &tg::list::Entry,
	reverse: bool,
) -> std::cmp::Ordering {
	let order = tg::list::compare(&a.specifier().to_string(), &b.specifier().to_string());
	let order = if reverse { order.reverse() } else { order };
	order
		.then_with(|| entry_kind(a).cmp(&entry_kind(b)))
		.then_with(|| {
			let order = a.specifier().to_string().cmp(&b.specifier().to_string());
			if reverse { order.reverse() } else { order }
		})
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

#[must_use]
pub fn entry_kind(entry: &tg::list::Entry) -> tg::id::Kind {
	entry.kind()
}
