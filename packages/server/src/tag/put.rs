use crate::Server;
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};

impl Server {
	pub async fn put_tag(&self, tag: &tg::Tag, arg: tg::tag::put::Arg) -> tg::Result<()> {
		// If the remote arg is set, then forward the request.
		let remote = arg.remote.as_ref();
		if let Some(remote) = remote {
			let remote = self
				.remotes
				.get(remote)
				.ok_or_else(|| tg::error!("failed to find the remote"))?
				.clone();
			let arg = tg::tag::put::Arg {
				remote: None,
				..arg.clone()
			};
			remote.put_tag(tag, arg).await?;
		}

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

		let mut parent = 0;

		for (i, component) in tag.components().iter().enumerate() {
			// Insert the component.
			let p = transaction.p();
			let statement = formatdoc!(
				"
					insert into tags (name, parent)
					values ({p}1, {p}2)
					on conflict (name, parent)
					do update set
						parent = {p}2
					returning id;
				"
			);
			let params = db::params![component, parent];
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
				parent = id;
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
		let tag = tag
			.join("/")
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the tag"))?;
		let arg = request.json().await?;
		handle.put_tag(&tag, arg).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
