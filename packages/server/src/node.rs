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

	pub(crate) async fn visible_principals_with_transaction(
		&self,
		transaction: &Transaction<'_>,
	) -> tg::Result<Vec<String>> {
		if matches!(self.context.principal, Some(tg::Principal::Root)) {
			return Ok(Vec::new());
		}
		let mut principals = vec![tg::grant::Principal::Public.to_string()];
		if let Some(tg::Principal::User(user)) = &self.context.principal {
			principals.push(user.to_string());
			principals.extend(
				Self::direct_memberships_with_transaction(transaction, &user.clone().into())
					.await?,
			);
		}
		principals.sort();
		principals.dedup();
		Ok(principals)
	}

	pub(crate) async fn node_is_visible_with_transaction(
		&self,
		transaction: &Transaction<'_>,
		id: &tg::Id,
	) -> tg::Result<bool> {
		if matches!(self.context.principal, Some(tg::Principal::Root)) {
			return Ok(true);
		}
		let principals = self
			.visible_principals_with_transaction(transaction)
			.await?;
		if principals.is_empty() {
			return Ok(false);
		}
		for resource in Self::ancestor_ids_with_transaction(transaction, id).await? {
			for principal in &principals {
				let p = transaction.p();
				let statement = formatdoc!(
					"
						select 1
						from visibility
						where resource = {p}1 and principal = {p}2
						limit 1;
					"
				);
				let row = transaction
					.query_optional(statement.into(), db::params![resource.clone(), principal])
					.await
					.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
				if row.is_some() {
					return Ok(true);
				}
			}
		}
		Ok(false)
	}

	pub(crate) async fn increment_visibility_with_transaction(
		transaction: &Transaction<'_>,
		resource: &tg::Id,
		principal: &str,
	) -> tg::Result<()> {
		for resource in Self::ancestor_ids_with_transaction(transaction, resource).await? {
			let p = transaction.p();
			let statement = formatdoc!(
				"
					insert into visibility (resource, principal, count)
					values ({p}1, {p}2, 1)
					on conflict (resource, principal)
					do update set count = visibility.count + 1;
				"
			);
			let result = transaction
				.execute(statement.into(), db::params![resource, principal])
				.await;
			result.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		}
		Ok(())
	}

	pub(crate) async fn decrement_visibility_with_transaction(
		transaction: &Transaction<'_>,
		resource: &tg::Id,
		principal: &str,
	) -> tg::Result<()> {
		for resource in Self::ancestor_ids_with_transaction(transaction, resource).await? {
			let p = transaction.p();
			let statement = formatdoc!(
				"
					delete from visibility
					where resource = {p}1 and principal = {p}2 and count = 1;
				"
			);
			let result = transaction
				.execute(statement.into(), db::params![resource.clone(), principal])
				.await;
			let deleted =
				result.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
			if deleted == 0 {
				let statement = formatdoc!(
					"
						update visibility
						set count = count - 1
						where resource = {p}1 and principal = {p}2 and count > 1;
					"
				);
				let result = transaction
					.execute(statement.into(), db::params![resource, principal])
					.await;
				result.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
			}
		}
		Ok(())
	}

	async fn ancestor_ids_with_transaction(
		transaction: &Transaction<'_>,
		resource: &tg::Id,
	) -> tg::Result<Vec<String>> {
		let mut ids = Vec::new();
		let mut current = Some(resource.clone());
		while let Some(id) = current {
			let Some(node) = Self::try_get_node_by_id_with_transaction(transaction, &id).await?
			else {
				break;
			};
			ids.push(node.id.to_string());
			current = node.parent;
		}
		Ok(ids)
	}

	async fn direct_memberships_with_transaction(
		transaction: &Transaction<'_>,
		member: &tg::Id,
	) -> tg::Result<Vec<String>> {
		#[derive(db::row::Deserialize)]
		struct Row {
			id: String,
		}
		let p = transaction.p();
		let statement = formatdoc!(
			r#"
				select "group" as id from group_members where member = {p}1
				union
				select organization as id from organization_members where member = {p}1;
			"#
		);
		let rows = transaction
			.query_all_into::<Row>(statement.into(), db::params![member.to_string()])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		Ok(rows.into_iter().map(|row| row.id).collect())
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
