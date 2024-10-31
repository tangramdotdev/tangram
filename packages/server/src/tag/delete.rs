use crate::Server;
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_http::{outgoing::response::Ext as _, Incoming, Outgoing};

impl Server {
	pub async fn delete_tag(&self, tag: &tg::Tag) -> tg::Result<()> {
		// Get a database connection.
		let mut connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get database connection"))?;

		// Begin a transaction.
		let transaction = connection
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

		// Get the IDs of the components.
		let mut components = Vec::new();
		let mut parent = 0;
		for component in tag.components() {
			// Get the component.
			let p = transaction.p();
			let statement = formatdoc!(
				"
					select id
					from tags
					where name = {p}1 and parent = {p}2;
				"
			);
			let params = db::params![component, parent];
			let id = transaction
				.query_one_value_into(statement, params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			components.push(id);
			parent = id;
		}

		// Delete the components.
		for (i, id) in components.into_iter().rev().enumerate() {
			// If this is the final component, then set its item to null.
			if i == 0 {
				let p = transaction.p();
				let statement = formatdoc!(
					"
						update tags
						set item = null
						where id = {p}1;
					"
				);
				let params = db::params![id];
				transaction
					.execute(statement, params)
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			}

			// If the component no longer has an children, then delete it.
			let p = transaction.p();
			let statement = formatdoc!(
				"
					delete from tags
					where id = {p}1 and (
						select count(*) > 0
						from tags
						where parent = {p}1
					);
				"
			);
			let params = db::params![id];
			transaction
				.execute(statement, params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
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
	pub(crate) async fn handle_delete_tag_request<H>(
		handle: &H,
		_request: http::Request<Incoming>,
		tag: &[&str],
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let tag = tag
			.join("/")
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the tag"))?;
		handle.delete_tag(&tag).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
