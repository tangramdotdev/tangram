use {
	futures::{FutureExt as _, future::BoxFuture},
	tangram_client::prelude::*,
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_server_core::{
		Session,
		list::{Cursor, PageArg, Scope},
	},
};

/// The component.
#[derive(Clone, Default)]
pub struct Component {}

impl Component {
	#[must_use]
	pub fn new() -> Self {
		Self::default()
	}

	#[tracing::instrument(fields(pattern = %arg.pattern), level = "trace", name = "match", skip_all)]
	pub(crate) async fn match_(
		&self,
		session: &Session,
		arg: tg::match_::Arg,
	) -> tg::Result<tg::match_::Output> {
		session.verify_request_with_network_access()?;

		// Parse the pagination arguments.
		let limit = session.parse_page_limit(arg.limit)?;
		let limit = usize::try_from(limit).unwrap();
		let after = arg
			.cursor
			.as_deref()
			.map(tangram_server_core::cursor::deserialize::<Cursor>)
			.transpose()?
			.map(|cursor| match cursor {
				Cursor::V0 { after } => after,
			});

		// Resolve the scope, which is every specifier for an empty pattern, and the children of the pattern's parent otherwise.
		let scope = if arg.pattern.is_empty() {
			Scope::All
		} else {
			Scope::Children(arg.pattern.parent.clone().map(tg::Selector::Specifier))
		};

		// List the page of the matching items.
		let kinds = tangram_server_list_common::Kinds {
			groups: arg.groups,
			organizations: arg.organizations,
			tags: arg.tags,
			users: arg.users,
		};
		let pattern = arg.pattern.clone();
		let page_arg = PageArg {
			after,
			cached: arg.cached,
			filter: Box::new(move |item| {
				pattern.matches_specifier(item.specifier())
					&& tangram_server_list_common::item_kind_enabled(item, &kinds)
			}),
			limit,
			location: arg.location.as_ref(),
			query: tangram_server_core::list::remote::Query::Match(arg.clone()),
			reverse: arg.reverse,
			scope,
			tokens: &arg.tokens,
			ttl: arg.ttl,
		};
		let page = session
			.server
			.components
			.list
			.list_page(session, page_arg)
			.await?;

		// Create the output.
		let cursor = page
			.next
			.map(|after| Cursor::V0 { after })
			.as_ref()
			.map(tangram_server_core::cursor::serialize)
			.transpose()?;
		let output = tg::match_::Output {
			cursor,
			data: page.data,
		};

		Ok(output)
	}

	pub(crate) async fn match_request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the accept header"))?;
		let (arg, _) = request
			.arg()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the arg"))?;
		let arg = arg.unwrap_or_default();
		let output = self.match_(session, arg).await?;
		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::APPLICATION, mime::JSON)) => {
				let content_type = mime::APPLICATION_JSON;
				let body = serde_json::to_vec(&output).unwrap();
				(Some(content_type), BoxBody::with_bytes(body))
			},
			Some((type_, subtype)) => {
				return Err(tg::error!(argument, %type_, %subtype, "invalid accept type"));
			},
		};
		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		let response = response.body(body).unwrap();

		Ok(response)
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
			(http::Method::GET, ["match"]) => self.match_request(session, request).await,
			(_, _) => Ok(tangram_server_core::http::not_found()),
		}
	}
}

impl tangram_server_core::component::Lifecycle for Component {}

impl tangram_server_core::match_::Component for Component {
	fn match_<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		arg: tg::match_::Arg,
	) -> BoxFuture<'a, tg::Result<tg::match_::Output>> {
		self.match_(session, arg).boxed()
	}

	fn request<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>> {
		self.request(session, request).boxed()
	}
}
