use {crate::Session, futures::future::BoxFuture, tangram_client::prelude::*};

mod named;

pub struct IndexOutput {
	pub ids: Vec<Option<tg::Id>>,
	pub specifiers: Vec<Option<tg::Specifier>>,
}

/// The node component.
pub trait Component: crate::component::Lifecycle {
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
		batch: &'a mut tangram_index::batch::Arg,
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

	fn try_get_ids_for_specifiers<'a>(
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

impl Session {
	pub async fn contains_ids_from_index(&self, ids: &[tg::Id]) -> tg::Result<Vec<bool>> {
		self.server
			.components
			.node
			.contains_ids_from_index(self, ids)
			.await
	}

	pub async fn delete_named_nodes_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		ids_and_specifiers: &[(tg::Id, tg::Specifier)],
		batch: &mut tangram_index::batch::Arg,
		batch_size: usize,
	) -> tg::Result<std::ops::ControlFlow<(), crate::database::Error>> {
		self.server
			.components
			.node
			.delete_named_nodes_with_transaction(
				self,
				transaction,
				ids_and_specifiers,
				batch,
				batch_size,
			)
			.await
	}

	pub async fn pull_ancestors(
		&self,
		specifier: &tg::Specifier,
		pull: tg::node::AncestorsPull,
	) -> tg::Result<()> {
		self.server
			.components
			.node
			.pull_ancestors(self, specifier, pull)
			.await
	}

	pub async fn try_get_ids_and_ancestors_for_specifiers(
		&self,
		specifiers: &[tg::Specifier],
	) -> tg::Result<std::collections::BTreeMap<tg::Specifier, Option<tg::Id>>> {
		self.server
			.components
			.node
			.try_get_ids_and_ancestors_for_specifiers(self, specifiers)
			.await
	}

	pub async fn try_get_ids_for_specifiers(
		&self,
		specifiers: &[tg::Specifier],
	) -> tg::Result<std::collections::BTreeMap<tg::Specifier, Option<tg::Id>>> {
		self.server
			.components
			.node
			.try_get_ids_for_specifiers(self, specifiers)
			.await
	}

	pub async fn try_get_ids_for_specifiers_from_index(
		&self,
		specifiers: &[tg::Specifier],
	) -> tg::Result<Vec<Option<tg::Id>>> {
		self.server
			.components
			.node
			.try_get_ids_for_specifiers_from_index(self, specifiers)
			.await
	}

	pub async fn try_get_nodes_from_index(
		&self,
		ids: &[tg::Id],
		specifiers: &[tg::Specifier],
	) -> tg::Result<IndexOutput> {
		self.server
			.components
			.node
			.try_get_nodes_from_index(self, ids, specifiers)
			.await
	}

	pub async fn try_resolve_named_node(
		&self,
		selector: &tg::Selector<tg::Id>,
	) -> tg::Result<Option<(tg::Id, tg::Specifier)>> {
		self.server
			.components
			.node
			.try_resolve_named_node(self, selector)
			.await
	}
}
