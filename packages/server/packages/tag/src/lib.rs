use {
	futures::{FutureExt as _, future::BoxFuture},
	tangram_client as tg,
	tangram_http::body::Boxed as BoxBody,
	tangram_server_core::Session,
};

pub mod batch;
pub mod delete;
pub mod get;
pub mod pull;
pub mod put;

/// The component.
#[derive(Clone, Default)]
pub struct Component {}

impl Component {
	#[must_use]
	pub fn new() -> Self {
		Self::default()
	}

	pub(crate) fn tag_target_to_string(target: &tg::tag::data::Target) -> String {
		match target {
			tg::tag::data::Target::Object(id) => id.to_string(),
			tg::tag::data::Target::Process(id) => id.to_string(),
		}
	}

	pub(crate) async fn request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let method = request.method().clone();
		let path = request.uri().path().to_owned();
		let path_components = path.split('/').skip(1).collect::<Vec<_>>();
		match (method, path_components.as_slice()) {
			(http::Method::POST, ["tags", "batch"]) => {
				self.post_tag_batch_request(session, request).await
			},
			(http::Method::GET, ["tags", path @ ..]) => {
				self.try_get_tag_request(session, request, path).await
			},
			(http::Method::PUT, ["tags"]) => self.put_tag_request(session, request).await,
			(http::Method::DELETE, ["tags"]) => self.delete_tags_request(session, request).await,
			(_, _) => Ok(tangram_server_core::http::not_found()),
		}
	}
}

impl tangram_server_core::component::Lifecycle for Component {}

impl tangram_server_core::tag::Component for Component {
	fn delete_tags<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		arg: tg::tag::delete::Arg,
	) -> BoxFuture<'a, tg::Result<tg::tag::delete::Output>> {
		self.delete_tags(session, arg).boxed()
	}

	fn pull_tag<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		pattern: tg::specifier::Pattern,
		location: Option<tg::location::Arg>,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.pull_tag(session, pattern, location).boxed()
	}

	fn try_get_tag_local<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		id: &'a tg::tag::Id,
		tokens: tg::authorization::Tokens,
	) -> BoxFuture<'a, tg::Result<Option<tg::tag::get::Output>>> {
		self.try_get_tag_local(session, id, tokens).boxed()
	}

	fn try_get_tag_remote<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		id: &'a tg::tag::Id,
		arg: tg::tag::get::Arg,
		remote: tg::location::Remote,
		tokens: tg::authorization::Tokens,
	) -> BoxFuture<'a, tg::Result<Option<tg::tag::get::Output>>> {
		self.try_get_tag_remote(session, id, arg, remote, tokens)
			.boxed()
	}

	fn put_tag<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		arg: tg::tag::put::Arg,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.put_tag(session, arg).boxed()
	}

	fn post_tag_batch<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		arg: tg::tag::batch::Arg,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.post_tag_batch(session, arg).boxed()
	}

	fn try_get_tag<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		tag: &'a tg::tag::Selector,
		arg: tg::tag::get::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::tag::get::Output>>> {
		self.try_get_tag(session, tag, arg).boxed()
	}

	fn request<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>> {
		self.request(session, request).boxed()
	}
}
