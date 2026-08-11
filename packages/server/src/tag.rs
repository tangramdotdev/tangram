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
		id: &tg::tag::Id,
	) -> tg::Result<tg::tag::Data> {
		#[derive(db::row::Deserialize)]
		struct Row {
			target: String,
			name: String,
			#[tangram_database(as = "Option<db::value::FromStr>")]
			parent: Option<tg::Id>,
			permissions: String,
		}

		let specifier =
			Self::try_get_specifier_for_id_with_transaction(transaction, &id.clone().into())
				.await?
				.ok_or_else(|| tg::error!("failed to find the tag"))?;
		let p = transaction.p();
		let statement = formatdoc!(
			"
				select target, name, parent, permissions
				from tags
				where id = {p}1;
			"
		);
		let row = transaction
			.query_one_into::<Row>(statement.into(), db::params![id.to_string()])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let target = Self::parse_tag_target(&row.target)?;
		let permissions = serde_json::from_str(&row.permissions)
			.map_err(|error| tg::error!(!error, "failed to deserialize the permissions"))?;
		Ok(tg::tag::Data {
			id: id.clone(),
			target,
			name: row.name,
			parent: row.parent,
			permissions,
			specifier,
		})
	}

	pub(crate) fn parse_tag_target(target: &str) -> tg::Result<tg::tag::data::Target> {
		target
			.parse::<tg::Either<tg::object::Id, tg::process::Id>>()
			.map(Into::into)
			.map_err(|error| tg::error!(!error, "failed to parse the tag target"))
	}

	pub(crate) fn tag_target_to_string(target: &tg::tag::data::Target) -> String {
		match target {
			tg::tag::data::Target::Object(id) => id.to_string(),
			tg::tag::data::Target::Process(id) => id.to_string(),
		}
	}

	/// Compute the permissions the current principal has on a tag target, to be recorded on the tag.
	pub(crate) async fn recorded_tag_target_permissions(
		&self,
		target: &tg::tag::data::Target,
	) -> tg::Result<Vec<tg::grant::Permission>> {
		let (resource, aspects): (tg::Id, Vec<tg::grant::Permission>) = match target {
			tg::tag::data::Target::Object(id) => (
				id.clone().into(),
				vec![tg::grant::Permission::Object(
					tg::grant::permission::object::Permission::Node,
				)],
			),
			tg::tag::data::Target::Process(id) => (
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
		if matches!(self.context.principal, tg::Principal::Root) {
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
				if self
					.authorize(resource, permission)
					.await?
					.is_some_and(|permissions| permissions.contains(permission))
				{
					permissions.push(permission);
					break;
				}
			}
		}
		Ok(permissions)
	}
}
