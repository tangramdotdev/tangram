use {
	crate::{Session, database::Transaction},
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

#[derive(Clone, Debug)]
pub(crate) struct Node {
	pub id: tg::Id,
	pub kind: tg::id::Kind,
	pub name: String,
	pub parent: Option<tg::Id>,
	pub specifier: tg::Specifier,
}

impl Session {
	pub(crate) async fn create_node_with_transaction(
		transaction: &Transaction<'_>,
		id: &tg::Id,
		kind: tg::id::Kind,
		specifier: &tg::Specifier,
		parent: Option<&tg::Id>,
	) -> tg::Result<Node> {
		if specifier.components().next().is_none() {
			return Err(tg::error!("invalid specifier"));
		}
		let name = specifier.name().to_owned();
		let kind = kind_to_str(kind);
		let p = transaction.p();
		let statement = formatdoc!(
			"
				insert into nodes (id, kind, parent, name, specifier)
				values ({p}1, {p}2, {p}3, {p}4, {p}5);
			"
		);
		let result = transaction
			.execute(
				statement.into(),
				db::params![
					id.to_string(),
					kind,
					parent.map(ToString::to_string),
					name.clone(),
					specifier.to_string()
				],
			)
			.await;
		result.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let node = Node {
			id: id.clone(),
			kind: str_to_kind(kind)?,
			name,
			parent: parent.cloned(),
			specifier: specifier.clone(),
		};
		Ok(node)
	}

	pub(crate) async fn try_get_node_by_selector_with_transaction<I>(
		transaction: &Transaction<'_>,
		selector: &tg::Selector<I>,
	) -> tg::Result<Option<Node>>
	where
		I: Clone + Into<tg::Id>,
	{
		match selector {
			tg::Selector::Id(id) => {
				Self::try_get_node_by_id_with_transaction(transaction, &id.clone().into()).await
			},
			tg::Selector::Specifier(specifier) => {
				Self::try_get_node_by_specifier_with_transaction(transaction, specifier).await
			},
		}
	}

	pub(crate) async fn try_get_node_by_id_with_transaction(
		transaction: &Transaction<'_>,
		id: &tg::Id,
	) -> tg::Result<Option<Node>> {
		Self::try_get_node_with_transaction(transaction, "id", id.to_string()).await
	}

	pub(crate) async fn try_get_node_by_specifier_with_transaction(
		transaction: &Transaction<'_>,
		specifier: &tg::Specifier,
	) -> tg::Result<Option<Node>> {
		Self::try_get_node_with_transaction(transaction, "specifier", specifier.to_string()).await
	}

	async fn try_get_node_with_transaction(
		transaction: &Transaction<'_>,
		column: &str,
		value: String,
	) -> tg::Result<Option<Node>> {
		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			id: tg::Id,
			kind: String,
			name: String,
			#[tangram_database(as = "Option<db::value::FromStr>")]
			parent: Option<tg::Id>,
			#[tangram_database(as = "db::value::FromStr")]
			specifier: tg::Specifier,
		}
		let p = transaction.p();
		let statement = formatdoc!(
			"
				select id, kind, name, parent, specifier
				from nodes
				where {column} = {p}1;
			"
		);
		let row = transaction
			.query_optional_into::<Row>(statement.into(), db::params![value])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		row.map(|row| {
			Ok(Node {
				id: row.id,
				kind: str_to_kind(&row.kind)?,
				name: row.name,
				parent: row.parent,
				specifier: row.specifier,
			})
		})
		.transpose()
	}

	pub(crate) async fn resolve_resource_with_transaction(
		transaction: &Transaction<'_>,
		resource: &tg::grant::Resource,
	) -> tg::Result<Option<tg::Id>> {
		match resource {
			tg::grant::Resource::Id(id) => {
				// Objects and processes are not nodes, so their ids resolve directly.
				if id.kind() == tg::id::Kind::Process
					|| tg::object::Id::try_from(id.clone()).is_ok()
				{
					return Ok(Some(id.clone()));
				}
				Ok(Self::try_get_node_by_id_with_transaction(transaction, id)
					.await?
					.map(|node| node.id))
			},
			tg::grant::Resource::Specifier(specifier) => Ok(
				Self::try_get_node_by_specifier_with_transaction(transaction, specifier)
					.await?
					.map(|node| node.id),
			),
		}
	}

	pub(crate) async fn node_has_children_with_transaction(
		transaction: &Transaction<'_>,
		id: &tg::Id,
	) -> tg::Result<bool> {
		let p = transaction.p();
		let statement = formatdoc!(
			"
				select 1
				from nodes
				where parent = {p}1
				limit 1;
			"
		);
		let row = transaction
			.query_optional(statement.into(), db::params![id.to_string()])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		Ok(row.is_some())
	}
}

pub(crate) fn kind_to_str(kind: tg::id::Kind) -> &'static str {
	match kind {
		tg::id::Kind::User => "user",
		tg::id::Kind::Group => "group",
		tg::id::Kind::Organization => "organization",
		tg::id::Kind::Tag => "tag",
		_ => "unknown",
	}
}

fn str_to_kind(s: &str) -> tg::Result<tg::id::Kind> {
	match s {
		"user" => Ok(tg::id::Kind::User),
		"group" => Ok(tg::id::Kind::Group),
		"organization" => Ok(tg::id::Kind::Organization),
		"tag" => Ok(tg::id::Kind::Tag),
		_ => Err(tg::error!("invalid kind")),
	}
}
