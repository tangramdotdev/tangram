use {
	crate::Session, futures::future::BoxFuture, std::ops::ControlFlow, tangram_client::prelude::*,
};

pub struct IndexOutput {
	pub ids: Vec<Option<tg::Id>>,
	pub specifiers: Vec<Option<tg::Specifier>>,
}

/// The node component.
pub trait Component: crate::component::Lifecycle {
	fn collect_named_subtrees_with_transaction<'a>(
		&'a self,
		transaction: &'a crate::database::Transaction<'_>,
		roots: &'a [tg::Id],
		batch_size: usize,
	) -> BoxFuture<'a, tg::Result<ControlFlow<Vec<(tg::Id, tg::Specifier)>, crate::database::Error>>>;

	fn contains_ids_from_index<'a>(
		&'a self,
		session: &'a Session,
		ids: &'a [tg::Id],
	) -> BoxFuture<'a, tg::Result<Vec<bool>>>;

	fn delete_named_nodes_with_transaction<'a>(
		&'a self,
		session: &'a Session,
		transaction: &'a crate::database::Transaction<'_>,
		ids_and_specifiers: &'a [(tg::Id, tg::Specifier)],
		batch: &'a mut tangram_index::write::Arg,
		batch_size: usize,
	) -> BoxFuture<'a, tg::Result<std::ops::ControlFlow<(), crate::database::Error>>>;

	fn pull_ancestors<'a>(
		&'a self,
		session: &'a Session,
		specifier: &'a tg::Specifier,
		pull: tg::node::AncestorsPull,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn try_get_ids_and_ancestors_for_specifiers<'a>(
		&'a self,
		session: &'a Session,
		specifiers: &'a [tg::Specifier],
	) -> BoxFuture<'a, tg::Result<std::collections::BTreeMap<tg::Specifier, Option<tg::Id>>>>;

	fn try_get_ids_for_specifiers_from_index<'a>(
		&'a self,
		session: &'a Session,
		specifiers: &'a [tg::Specifier],
	) -> BoxFuture<'a, tg::Result<Vec<Option<tg::Id>>>>;

	fn try_get_nodes_from_index<'a>(
		&'a self,
		session: &'a Session,
		ids: &'a [tg::Id],
		specifiers: &'a [tg::Specifier],
	) -> BoxFuture<'a, tg::Result<IndexOutput>>;

	fn try_resolve_named_node<'a>(
		&'a self,
		session: &'a Session,
		selector: &'a tg::Selector<tg::Id>,
	) -> BoxFuture<'a, tg::Result<Option<(tg::Id, tg::Specifier)>>>;
}
