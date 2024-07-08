use crate::Server;
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};

impl Server {
	pub async fn put_tag(&self, tag: &tg::Tag, arg: tg::tag::put::Arg) -> tg::Result<()> {
		// Get a database connection.
		let mut connection = self
			.database
			.connection(db::Priority::Low)
			.await
			.map_err(|source| tg::error!(!source, "failed to get database connection"))?;

		// Begin a transaction.
		let transaction = connection
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

		let mut parent: Option<u64> = None;
		for (i, name) in tag.components().iter().enumerate() {
			// Insert the component.
			let p = transaction.p();
			let statement = formatdoc!(
				"
					insert into tags (name, parent)
					values ({p}1, {p}2)
					on conflict (name, parent) do nothing
					returning id;
				"
			);
			let params = db::params![name, parent];
			let id = transaction
				.query_one_value_into(statement, params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

			if i == tag.components().len() - 1 {
				// If this is the last component, then set the item.
				let p = transaction.p();
				let statement = formatdoc!(
					"
						update tags
						set item = {p}1
						where id = {p}2;
					"
				);
				let item = arg.item.to_string();
				let params = db::params![item, id];
				transaction
					.execute(statement, params)
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			} else {
				// Otherwise, set the parent and continue.
				parent = Some(id);
			}
		}

		// Commit the transaction.
		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		Ok(())
	}
}

impl Server {
	pub(crate) async fn handle_put_tag_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
		tag: &[&str],
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let tag = tag.join("/").parse()?;
		let arg = request.json().await?;
		handle.put_tag(&tag, arg).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
