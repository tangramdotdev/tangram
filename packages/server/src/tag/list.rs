use crate::Server;
use futures::{
	stream::{self, FuturesUnordered},
	StreamExt as _, TryStreamExt,
};
use indoc::formatdoc;
use itertools::Itertools as _;
use num::ToPrimitive;
use std::collections::BTreeMap;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_either::Either;
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};

impl Server {
	pub async fn list_tags(&self, arg: tg::tag::list::Arg) -> tg::Result<tg::tag::list::Output> {
		// If the remote arg is set, then forward the request.
		let remote = arg.remote.as_ref();
		if let Some(remote) = remote {
			let remote = self.get_remote_client(remote.clone()).await?;
			let arg = tg::tag::list::Arg {
				remote: None,
				..arg
			};
			let output = remote.list_tags(arg).await?;
			return Ok(output);
		}

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

		// Drop the database connection.
		drop(connection);

		// Filter the rows.
		rows.retain(|row| arg.pattern.matches(&row.tag));

		// Create the outputs.
		let mut outputs = rows
			.into_iter()
			.map(|row| {
				let tag = row.tag.clone();
				let output = tg::tag::get::Output {
					tag: row.tag,
					item: row.item,
				};
				(tag, output)
			})
			.collect::<BTreeMap<tg::Tag, tg::tag::get::Output>>();

		// Get the outputs from the remotes.
		let remote_outputs = self
			.get_remote_clients()
			.await?
			.into_values()
			.map(|client| {
				let arg = tg::tag::list::Arg {
					remote: None,
					..arg.clone()
				};
				async move { client.list_tags(arg).await }
			})
			.collect::<FuturesUnordered<_>>()
			.map_ok(|output| stream::iter(output.data).map(Ok::<_, tg::Error>))
			.try_flatten()
			.collect::<Vec<_>>()
			.await
			.into_iter()
			.filter_map(Result::ok);
		for output in remote_outputs {
			if !outputs.contains_key(&output.tag) {
				outputs.insert(output.tag.clone(), output);
			}
		}

		// Get the outputs.
		let mut outputs = outputs.into_values().collect_vec();

		// Sort the rows.
		outputs.sort_by(|a, b| a.tag.cmp(&b.tag));

		// Reverse if requested.
		if arg.reverse {
			outputs.reverse();
		}

		// Limit.
		if let Some(length) = arg.length {
			outputs.truncate(length.to_usize().unwrap());
		}

		// Create the output.
		let output = tg::tag::list::Output { data: outputs };

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
