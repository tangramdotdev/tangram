use crate::Server;
use indoc::formatdoc;
use num::ToPrimitive;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_either::Either;
use tangram_http::{Incoming, Outgoing, incoming::request::Ext as _, outgoing::response::Ext as _};

impl Server {
	pub async fn list_tags(&self, arg: tg::tag::list::Arg) -> tg::Result<tg::tag::list::Output> {
		// If the remote arg is set, then forward the request.
		let remote = arg.remote.as_ref();
		if let Some(remote) = remote {
			let remote = self
				.remotes
				.get(remote)
				.ok_or_else(|| tg::error!("the remote does not exist"))?
				.clone();
			let arg = tg::tag::list::Arg {
				remote: None,
				..arg
			};
			let output = remote.list_tags(arg).await?;
			return Ok(output);
		}

		// Attempt to list the tags locally.
		let output = self.list_tags_local(arg.clone()).await?;

		// If the output is not empty, then return it.
		if !output.data.is_empty() {
			return Ok(output);
		}

		// Otherwise, try the remotes.
		for remote in &self.remotes {
			let output = remote.list_tags(arg.clone()).await?;
			if !output.data.is_empty() {
				return Ok(output);
			}
		}

		Ok(tg::tag::list::Output { data: vec![] })
	}

	async fn list_tags_local(&self, arg: tg::tag::list::Arg) -> tg::Result<tg::tag::list::Output> {
		// Get a database connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get database connection"))?;

		#[derive(Clone, Debug, serde::Deserialize)]
		struct Row {
			tag: tg::Tag,
			item: Either<tg::build::Id, tg::object::Id>,
		}
		let p = connection.p();
		let prefix = arg
			.pattern
			.as_str()
			.char_indices()
			.find(|(_, c)| !(c.is_alphanumeric() || matches!(c, '.' | '_' | '+' | '-' | '/')))
			.map_or(arg.pattern.as_str().len(), |(i, _)| i);
		let prefix = &arg.pattern.as_str()[..prefix];
		let statement = formatdoc!(
			"
				select tag, item
				from tags
				where tag >= {p}1 and tag < {p}1 || x'ff';
			"
		);
		let params = db::params![prefix];
		let mut rows = connection
			.query_all_into::<Row>(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Filter the rows.
		rows.retain(|row| arg.pattern.matches(&row.tag));

		// Sort the rows.
		rows.sort_by(|a, b| a.tag.cmp(&b.tag));

		// Reverse if requested.
		if arg.reverse {
			rows.reverse();
		}

		// Limit.
		if let Some(length) = arg.length {
			rows.truncate(length.to_usize().unwrap());
		}

		// Create the output.
		let data = rows
			.into_iter()
			.map(|row| tg::tag::get::Output {
				tag: row.tag,
				item: row.item,
			})
			.collect();
		let output = tg::tag::list::Output { data };

		Ok(output)
	}
}

impl Server {
	pub(crate) async fn handle_list_tags_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let arg = request.query_params().transpose()?.unwrap_or_default();
		let output = handle.list_tags(arg).await?;
		let response = http::Response::builder().json(output).unwrap();
		Ok(response)
	}
}
