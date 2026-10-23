use {
	futures::{FutureExt as _, future::BoxFuture},
	tangram_client::prelude::*,
	tangram_http::body::Boxed as BoxBody,
	tangram_server_core::Session,
};

pub mod billing;
pub mod create;
pub mod delete;
pub mod get;
pub mod members;
pub mod usage;

/// The component.
#[derive(Clone, Default)]
pub struct Component {}

impl Component {
	#[must_use]
	pub fn new() -> Self {
		Self::default()
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
			(http::Method::POST, ["organizations"]) => {
				self.create_organization_request(session, request).await
			},
			(http::Method::GET, ["organizations", organization]) => {
				self.try_get_organization_request(session, request, organization)
					.await
			},
			(http::Method::GET, ["organizations", organization, "usage"]) => {
				self.try_get_organization_usage_request(session, request, organization)
					.await
			},
			(http::Method::DELETE, ["organizations", organization]) => {
				self.try_delete_organization_request(session, request, organization)
					.await
			},
			(http::Method::GET, ["organizations", organization, "members"]) => {
				self.list_organization_members_request(session, request, organization)
					.await
			},
			(http::Method::POST, ["organizations", organization, "members"]) => {
				self.add_organization_member_request(session, request, organization)
					.await
			},
			(http::Method::DELETE, ["organizations", organization, "members", member]) => {
				self.remove_organization_member_request(session, request, organization, member)
					.await
			},
			(http::Method::POST, ["organizations", organization, "billing", "manage"]) => {
				self.manage_organization_billing_request(session, request, organization)
					.await
			},
			(_, _) => Ok(tangram_server_core::http::not_found()),
		}
	}
}

impl tangram_server_core::component::Lifecycle for Component {}

impl tangram_server_core::organization::Component for Component {
	fn create_organization<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		arg: tg::organization::create::Arg,
	) -> BoxFuture<'a, tg::Result<tg::organization::create::Output>> {
		self.create_organization(session, arg).boxed()
	}

	fn try_get_organization<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		organization: &'a tg::organization::Selector,
		arg: tg::organization::get::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::organization::get::Output>>> {
		self.try_get_organization(session, organization, arg)
			.boxed()
	}

	fn try_get_organization_usage<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		organization: &'a tg::organization::Selector,
		arg: tg::usage::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::usage::Output>>> {
		self.try_get_organization_usage(session, organization, arg)
			.boxed()
	}

	fn try_delete_organization<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		organization: &'a tg::organization::Selector,
		arg: tg::organization::delete::Arg,
	) -> BoxFuture<'a, tg::Result<Option<()>>> {
		self.try_delete_organization(session, organization, arg)
			.boxed()
	}

	fn list_organization_members<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		organization: &'a tg::organization::Selector,
		arg: tg::organization::members::list::Arg,
	) -> BoxFuture<'a, tg::Result<tg::organization::members::list::Output>> {
		self.list_organization_members(session, organization, arg)
			.boxed()
	}

	fn add_organization_member<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		organization: &'a tg::organization::Selector,
		arg: tg::organization::members::add::Arg,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.add_organization_member(session, organization, arg)
			.boxed()
	}

	fn remove_organization_member<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		organization: &'a tg::organization::Selector,
		member: &'a tg::organization::Member,
		arg: tg::organization::members::remove::Arg,
	) -> BoxFuture<'a, tg::Result<Option<()>>> {
		self.remove_organization_member(session, organization, member, arg)
			.boxed()
	}

	fn manage_organization_billing<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		organization: &'a tg::organization::Selector,
		arg: tg::organization::billing::manage::Arg,
	) -> BoxFuture<'a, tg::Result<tg::organization::billing::manage::Output>> {
		self.manage_organization_billing(session, organization, arg)
			.boxed()
	}

	fn request<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>> {
		self.request(session, request).boxed()
	}
}
