use {
	crate::{Session, database::Transaction},
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

#[derive(Clone, Debug)]
pub(crate) struct Item {
	pub id: tg::Id,
	pub name: String,
	pub parent: Option<tg::Id>,
	pub specifier: tg::Specifier,
}

impl Item {
	#[must_use]
	pub fn kind(&self) -> tg::id::Kind {
		self.id.kind()
	}
}

impl Session {
	pub(crate) async fn create_specifier_with_transaction(
		transaction: &Transaction<'_>,
		id: &tg::Id,
		parent: Option<&tg::Id>,
		specifier: &tg::Specifier,
	) -> tg::Result<Item> {
		if specifier.components().next().is_none() {
			return Err(tg::error!("invalid specifier"));
		}
		let name = specifier.name().to_owned();
		let p = transaction.p();
		let statement = formatdoc!(
			"
				insert into specifiers (id, specifier)
				values ({p}1, {p}2);
			"
		);
		transaction
			.execute(
				statement.into(),
				db::params![id.to_string(), specifier.to_string()],
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let item = Item {
			id: id.clone(),
			name,
			parent: parent.cloned(),
			specifier: specifier.clone(),
		};

		Ok(item)
	}

	pub(crate) async fn try_get_specifier_by_selector_with_transaction<I>(
		transaction: &Transaction<'_>,
		selector: &tg::Selector<I>,
	) -> tg::Result<Option<Item>>
	where
		I: Clone + Into<tg::Id>,
	{
		match selector {
			tg::Selector::Id(id) => {
				Self::try_get_specifier_by_id_with_transaction(transaction, &id.clone().into())
					.await
			},
			tg::Selector::Specifier(specifier) => {
				Self::try_get_specifier_with_transaction(transaction, specifier).await
			},
		}
	}

	pub(crate) async fn try_get_specifier_by_id_with_transaction(
		transaction: &Transaction<'_>,
		id: &tg::Id,
	) -> tg::Result<Option<Item>> {
		Self::try_get_specifier_with_transaction_inner(transaction, "id", id.to_string()).await
	}

	pub(crate) async fn try_get_specifier_with_transaction(
		transaction: &Transaction<'_>,
		specifier: &tg::Specifier,
	) -> tg::Result<Option<Item>> {
		Self::try_get_specifier_with_transaction_inner(
			transaction,
			"specifier",
			specifier.to_string(),
		)
		.await
	}

	async fn try_get_specifier_with_transaction_inner(
		transaction: &Transaction<'_>,
		column: &str,
		value: String,
	) -> tg::Result<Option<Item>> {
		#[derive(db::row::Deserialize)]
		struct SpecifierRow {
			#[tangram_database(as = "db::value::FromStr")]
			id: tg::Id,
			#[tangram_database(as = "db::value::FromStr")]
			specifier: tg::Specifier,
		}

		let p = transaction.p();
		let statement = formatdoc!(
			"
				select id, specifier
				from specifiers
				where {column} = {p}1;
			"
		);
		let row = transaction
			.query_optional_into::<SpecifierRow>(statement.into(), db::params![value])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let Some(row) = row else {
			return Ok(None);
		};

		#[derive(db::row::Deserialize)]
		struct ChildRow {
			name: String,
			#[tangram_database(as = "Option<db::value::FromStr>")]
			parent: Option<tg::Id>,
		}

		#[derive(db::row::Deserialize)]
		struct RootRow {
			name: String,
		}

		let (name, parent) = match row.id.kind() {
			tg::id::Kind::Group | tg::id::Kind::Tag => {
				let table = match row.id.kind() {
					tg::id::Kind::Group => "groups",
					tg::id::Kind::Tag => "tags",
					_ => unreachable!(),
				};
				let statement = format!("select name, parent from {table} where id = {p}1;");
				let Some(row) = transaction
					.query_optional_into::<ChildRow>(
						statement.into(),
						db::params![row.id.to_string()],
					)
					.await
					.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
				else {
					return Ok(None);
				};
				(row.name, row.parent)
			},
			tg::id::Kind::Organization | tg::id::Kind::User => {
				let table = match row.id.kind() {
					tg::id::Kind::Organization => "organizations",
					tg::id::Kind::User => "users",
					_ => unreachable!(),
				};
				let statement = format!("select name from {table} where id = {p}1;");
				let Some(row) = transaction
					.query_optional_into::<RootRow>(
						statement.into(),
						db::params![row.id.to_string()],
					)
					.await
					.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
				else {
					return Ok(None);
				};
				(row.name, None)
			},
			_ => return Ok(None),
		};
		let item = Item {
			id: row.id,
			name,
			parent,
			specifier: row.specifier,
		};

		Ok(Some(item))
	}

	pub(crate) async fn resolve_resource_with_transaction(
		transaction: &Transaction<'_>,
		resource: &tg::grant::Resource,
	) -> tg::Result<Option<tg::Id>> {
		match resource {
			tg::grant::Resource::Id(id) => {
				// Objects, processes, and sandboxes do not have specifiers, so their IDs resolve directly.
				if id.kind() == tg::id::Kind::Process
					|| id.kind() == tg::id::Kind::Sandbox
					|| tg::object::Id::try_from(id.clone()).is_ok()
				{
					return Ok(Some(id.clone()));
				}
				let id = Self::try_get_specifier_by_id_with_transaction(transaction, id)
					.await?
					.map(|item| item.id);

				Ok(id)
			},
			tg::grant::Resource::Specifier(specifier) => {
				let id = Self::try_get_specifier_with_transaction(transaction, specifier)
					.await?
					.map(|item| item.id);

				Ok(id)
			},
		}
	}

	pub(crate) async fn specifier_has_children_with_transaction(
		transaction: &Transaction<'_>,
		id: &tg::Id,
	) -> tg::Result<bool> {
		let p = transaction.p();
		let statement = formatdoc!(
			"
				select 1
				from groups
				where parent = {p}1
				union all
				select 1
				from tags
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
