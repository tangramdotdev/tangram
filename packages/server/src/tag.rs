pub mod batch;
pub mod delete;
pub mod get;
pub mod grants;
pub mod pull;
pub mod put;

use {
	indoc::formatdoc,
	tangram_client as tg,
	tangram_database::{self as db, prelude::*},
};

pub(crate) async fn get_tag_data_with_transaction(
	transaction: &crate::database::Transaction<'_>,
	node: &crate::node::Node,
) -> tg::Result<tg::tag::Data> {
	#[derive(db::row::Deserialize)]
	struct Row {
		item: String,
	}
	let p = transaction.p();
	let statement = formatdoc!(
		"
			select item
			from tags
			where id = {p}1;
		"
	);
	let row = transaction
		.query_one_into::<Row>(statement.into(), db::params![node.id.to_string()])
		.await
		.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
	let item = parse_tag_item(&row.item)?;
	Ok(tg::tag::Data {
		id: node.id.clone().try_into()?,
		item,
		name: node.name.clone(),
		parent: node.parent.clone(),
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
