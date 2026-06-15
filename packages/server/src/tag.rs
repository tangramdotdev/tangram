pub mod batch;
pub mod delete;
pub mod get;
pub mod pull;
pub mod put;

use {
	crate::{Session, database::Transaction},
	indoc::formatdoc,
	tangram_client as tg,
	tangram_database::{self as db, prelude::*},
};

impl Session {
	pub(crate) async fn get_tag_data_with_transaction(
		transaction: &Transaction<'_>,
		node: &crate::node::Node,
	) -> tg::Result<tg::tag::Data> {
		#[derive(db::row::Deserialize)]
		struct Row {
			item: String,
			permissions: String,
		}
		let p = transaction.p();
		let statement = formatdoc!(
			"
				select item, permissions
				from tags
				where id = {p}1;
			"
		);
		let row = transaction
			.query_one_into::<Row>(statement.into(), db::params![node.id.to_string()])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let item = Self::parse_tag_item(&row.item)?;
		let permissions = serde_json::from_str(&row.permissions)
			.map_err(|error| tg::error!(!error, "failed to deserialize the permissions"))?;
		Ok(tg::tag::Data {
			id: node.id.clone().try_into()?,
			item,
			name: node.name.clone(),
			parent: node.parent.clone(),
			permissions,
			specifier: node.specifier.clone(),
		})
	}

	pub(crate) fn parse_tag_item(item: &str) -> tg::Result<tg::tag::data::Item> {
		item.parse::<tg::Either<tg::object::Id, tg::process::Id>>()
			.map(Into::into)
			.map_err(|error| tg::error!(!error, "failed to parse the tag item"))
	}

	pub(crate) fn tag_item_to_string(item: &tg::tag::data::Item) -> String {
		match item {
			tg::tag::data::Item::Object(id) => id.to_string(),
			tg::tag::data::Item::Process(id) => id.to_string(),
		}
	}

	/// Compute the permissions the current principal has on a tag item, to be recorded on the tag.
	pub(crate) async fn recorded_tag_permissions(
		&self,
		item: &tg::tag::data::Item,
	) -> tg::Result<Vec<tg::grant::Permission>> {
		let (resource, aspects): (tg::Id, Vec<tg::grant::Permission>) = match item {
			tg::tag::data::Item::Object(id) => (
				id.clone().into(),
				vec![tg::grant::Permission::Object(
					tg::grant::permission::object::Permission::Node,
				)],
			),
			tg::tag::data::Item::Process(id) => (
				id.clone().into(),
				[
					tg::grant::permission::process::Permission::Node,
					tg::grant::permission::process::Permission::NodeCommand,
					tg::grant::permission::process::Permission::NodeError,
					tg::grant::permission::process::Permission::NodeLog,
					tg::grant::permission::process::Permission::NodeOutput,
				]
				.into_iter()
				.map(tg::grant::Permission::Process)
				.collect(),
			),
		};
		// Root is always authorized, so it records the subtree variant of every aspect without consulting the index.
		if matches!(self.context.principal, Some(tg::Principal::Root)) {
			return Ok(aspects
				.into_iter()
				.map(tg::grant::Permission::subtree)
				.collect());
		}
		// For each aspect, record the strongest permission the principal has, trying the subtree variant before the node variant.
		let mut permissions = Vec::new();
		for aspect in aspects {
			for permission in [aspect.subtree(), aspect] {
				let resource = tg::grant::Resource::Id(resource.clone());
				if self.authorize(resource, permission).await? == Some(true) {
					permissions.push(permission);
					break;
				}
			}
		}
		Ok(permissions)
	}
}
