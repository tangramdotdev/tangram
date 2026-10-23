use {
	crate::Session,
	futures::{future::BoxFuture, stream::BoxStream},
	tangram_client::prelude::*,
	tangram_http::body::Boxed as BoxBody,
};

/// The get component.
pub trait Component: crate::component::Lifecycle {
	fn try_get<'a>(
		&'a self,
		session: &'a Session,
		reference: &'a tg::Reference,
		arg: tg::get::Arg,
	) -> BoxFuture<
		'a,
		tg::Result<BoxStream<'static, tg::Result<tg::progress::Event<Option<tg::get::Output>>>>>,
	>;

	fn request<'a>(
		&'a self,
		session: &'a Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>>;

	fn create_tag_target_token<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::tag::Id,
		target: &'a tg::Id,
		tokens: &'a tg::authorization::Tokens,
	) -> BoxFuture<'a, tg::Result<Option<tg::authorization::Token>>>;

	fn create_tag_target_token_with_permissions<'a>(
		&'a self,
		session: &'a Session,
		target: &'a tg::Id,
		permissions: Vec<tg::authorization::Permission>,
	) -> tg::Result<Option<tg::authorization::Token>>;

	fn match_tags_for_get<'a>(
		&'a self,
		session: &'a Session,
		pattern: &'a tg::specifier::Pattern,
		location: Option<&'a tg::location::Arg>,
		cached: bool,
		length: Option<u64>,
		ttl: tg::remote::cache::Ttl,
	) -> BoxFuture<'a, tg::Result<tg::match_::Output>>;

	fn try_get_with_selector<'a>(
		&'a self,
		session: &'a Session,
		selector: &'a tg::Selector<tg::Id>,
		location: Option<&'a tg::location::Arg>,
		tokens: &'a tg::authorization::Tokens,
		cached: bool,
		ttl: tg::remote::cache::Ttl,
	) -> BoxFuture<'a, tg::Result<Option<tg::get::Output>>>;
}
