use {
	crate::Session, futures::future::BoxFuture, std::ops::ControlFlow, tangram_client::prelude::*,
	tangram_http::body::Boxed as BoxBody,
};

/// The organization component.
pub trait Component: crate::component::Lifecycle {
	fn create_organization<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::organization::create::Arg,
	) -> BoxFuture<'a, tg::Result<tg::organization::create::Output>>;

	fn try_get_organization<'a>(
		&'a self,
		session: &'a Session,
		organization: &'a tg::organization::Selector,
		arg: tg::organization::get::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::organization::get::Output>>>;

	fn try_get_organization_usage<'a>(
		&'a self,
		session: &'a Session,
		organization: &'a tg::organization::Selector,
		arg: tg::usage::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::usage::Output>>>;

	fn try_delete_organization<'a>(
		&'a self,
		session: &'a Session,
		organization: &'a tg::organization::Selector,
		arg: tg::organization::delete::Arg,
	) -> BoxFuture<'a, tg::Result<Option<()>>>;

	fn list_organization_members<'a>(
		&'a self,
		session: &'a Session,
		organization: &'a tg::organization::Selector,
		arg: tg::organization::members::list::Arg,
	) -> BoxFuture<'a, tg::Result<tg::organization::members::list::Output>>;

	fn add_organization_member<'a>(
		&'a self,
		session: &'a Session,
		organization: &'a tg::organization::Selector,
		arg: tg::organization::members::add::Arg,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn remove_organization_member<'a>(
		&'a self,
		session: &'a Session,
		organization: &'a tg::organization::Selector,
		member: &'a tg::organization::Member,
		arg: tg::organization::members::remove::Arg,
	) -> BoxFuture<'a, tg::Result<Option<()>>>;

	fn manage_organization_billing<'a>(
		&'a self,
		session: &'a Session,
		organization: &'a tg::organization::Selector,
		arg: tg::organization::billing::manage::Arg,
	) -> BoxFuture<'a, tg::Result<tg::organization::billing::manage::Output>>;

	fn request<'a>(
		&'a self,
		session: &'a Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>>;

	fn try_get_organization_with_transaction<'a>(
		&'a self,
		transaction: &'a crate::database::Transaction<'_>,
		id: &'a tg::organization::Id,
	) -> BoxFuture<
		'a,
		tg::Result<ControlFlow<Option<tg::organization::Data>, crate::database::Error>>,
	>;
}
