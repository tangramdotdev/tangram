use {
	crate::Session, futures::future::BoxFuture, std::ops::ControlFlow, tangram_client::prelude::*,
	tangram_http::body::Boxed as BoxBody,
};

/// The group component.
pub trait Component: crate::component::Lifecycle {
	fn create_group<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::group::create::Arg,
	) -> BoxFuture<'a, tg::Result<tg::group::create::Output>>;

	fn try_get_group<'a>(
		&'a self,
		session: &'a Session,
		group: &'a tg::group::Selector,
		arg: tg::group::get::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::group::get::Output>>>;

	fn try_delete_group<'a>(
		&'a self,
		session: &'a Session,
		group: &'a tg::group::Selector,
		arg: tg::group::delete::Arg,
	) -> BoxFuture<'a, tg::Result<Option<()>>>;

	fn list_group_members<'a>(
		&'a self,
		session: &'a Session,
		group: &'a tg::group::Selector,
		arg: tg::group::members::list::Arg,
	) -> BoxFuture<'a, tg::Result<tg::group::members::list::Output>>;

	fn add_group_member<'a>(
		&'a self,
		session: &'a Session,
		group: &'a tg::group::Selector,
		arg: tg::group::members::add::Arg,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn remove_group_member<'a>(
		&'a self,
		session: &'a Session,
		group: &'a tg::group::Selector,
		member: &'a tg::group::Member,
		arg: tg::group::members::remove::Arg,
	) -> BoxFuture<'a, tg::Result<Option<()>>>;

	fn request<'a>(
		&'a self,
		session: &'a Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>>;

	fn create_parent_groups_with_transaction<'a>(
		&'a self,
		session: &'a Session,
		transaction: &'a crate::database::Transaction<'_>,
		specifier: &'a tg::Specifier,
		batch: &'a mut tangram_index::write::Arg,
	) -> BoxFuture<'a, tg::Result<ControlFlow<Option<tg::Id>, crate::database::Error>>>;

	fn try_get_group_with_transaction<'a>(
		&'a self,
		transaction: &'a crate::database::Transaction<'_>,
		id: &'a tg::group::Id,
	) -> BoxFuture<'a, tg::Result<ControlFlow<Option<tg::group::Data>, crate::database::Error>>>;

	fn resolve_parent_for_specifier_with_transaction<'a>(
		&'a self,
		transaction: &'a crate::database::Transaction<'_>,
		specifier: &'a tg::Specifier,
	) -> BoxFuture<'a, tg::Result<ControlFlow<Option<tg::Id>, crate::database::Error>>>;
}
