use {
	crate::Session, futures::future::BoxFuture, std::ops::ControlFlow, tangram_client::prelude::*,
	tangram_http::body::Boxed as BoxBody,
};

/// The grant component.
pub trait Component: crate::component::Lifecycle {
	fn create_grant<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::grant::create::Arg,
	) -> BoxFuture<'a, tg::Result<tg::grant::create::Output>>;

	fn delete_grant<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::grant::delete::Arg,
	) -> BoxFuture<'a, tg::Result<Option<()>>>;

	fn list_grants<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::grant::list::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::grant::list::Output>>>;

	fn request<'a>(
		&'a self,
		session: &'a Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>>;

	fn create_grant_with_transaction<'a>(
		&'a self,
		session: &'a Session,
		transaction: &'a crate::database::Transaction<'_>,
		arg: tg::grant::create::Arg,
		batch: &'a mut tangram_index::write::Arg,
	) -> BoxFuture<'a, tg::Result<ControlFlow<(tg::grant::Data, bool), crate::database::Error>>>;

	fn delete_grant_with_transaction<'a>(
		&'a self,
		session: &'a Session,
		transaction: &'a crate::database::Transaction<'_>,
		arg: tg::grant::delete::Arg,
		batch: &'a mut tangram_index::write::Arg,
	) -> BoxFuture<'a, tg::Result<ControlFlow<Option<()>, crate::database::Error>>>;

	fn delete_node_grants_batch_with_transaction<'a>(
		&'a self,
		session: &'a Session,
		transaction: &'a crate::database::Transaction<'_>,
		ids: &'a [tg::Id],
		batch: &'a mut tangram_index::write::Arg,
	) -> BoxFuture<'a, tg::Result<ControlFlow<(), crate::database::Error>>>;

	fn delete_node_grants_with_transaction<'a>(
		&'a self,
		session: &'a Session,
		transaction: &'a crate::database::Transaction<'_>,
		id: &'a tg::Id,
		batch: &'a mut tangram_index::write::Arg,
	) -> BoxFuture<'a, tg::Result<ControlFlow<(), crate::database::Error>>>;

	fn list_resource_grants_with_transaction<'a>(
		&'a self,
		transaction: &'a crate::database::Transaction<'_>,
		resource: &'a tg::Id,
		after: Option<&'a (String, String)>,
		limit: Option<u64>,
	) -> BoxFuture<'a, tg::Result<ControlFlow<Vec<tg::grant::Data>, crate::database::Error>>>;

	fn resolve_subject_with_transaction<'a>(
		&'a self,
		transaction: &'a crate::database::Transaction<'_>,
		subject: &'a tg::authorization::subject::Selector,
	) -> BoxFuture<
		'a,
		tg::Result<ControlFlow<Option<tg::authorization::Subject>, crate::database::Error>>,
	>;
}
