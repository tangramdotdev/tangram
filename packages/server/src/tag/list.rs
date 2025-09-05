use crate::Server;
use futures::{TryStreamExt, stream::FuturesUnordered};
use indoc::formatdoc;
use num::ToPrimitive as _;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_either::Either;
use tangram_http::{Body, request::Ext as _, response::builder::Ext as _};

impl Server {
	pub async fn list_tags(
		&self,
		mut arg: tg::tag::list::Arg,
	) -> tg::Result<tg::tag::list::Output> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = arg.remote.take() {
			let remote = self.get_remote_client(remote.clone()).await?;
			let arg = tg::tag::list::Arg {
				remote: None,
				..arg
			};
			let output = remote.list_tags(arg).await?;
			return Ok(output);
		}

		// List the local tags.
		let mut output = self.list_tags_local(arg.clone()).await?;

		// List the remote tags.
		let remote = self
			.get_remote_clients()
			.await?
			.into_iter()
			.map(|(remote, client)| {
				let arg = arg.clone();
				async move {
					let mut output = client.list_tags(arg).await?;
					for output in &mut output.data {
						output.remote = Some(remote.clone());
					}
					Ok::<_, tg::Error>(output)
				}
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<Vec<_>>()
			.await?;

		output
			.data
			.extend(remote.into_iter().flat_map(|output| output.data));

		Ok(output)
	}

	async fn list_tags_local(&self, arg: tg::tag::list::Arg) -> tg::Result<tg::tag::list::Output> {
		// Get a database connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		#[derive(Clone, Debug, serde::Deserialize)]
		struct Row {
			tag: tg::Tag,
			item: Either<tg::process::Id, tg::object::Id>,
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
			.query_all_into::<db::row::Serde<Row>>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			.into_iter()
			.map(|row| row.0)
			.collect::<Vec<_>>();

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
				remote: None,
			})
			.collect();
		let output = tg::tag::list::Output { data };

		Ok(output)
	}

	pub(crate) async fn handle_list_tags_request<H>(
		handle: &H,
		request: http::Request<Body>,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		let arg = request.query_params().transpose()?.unwrap_or_default();
		let output = handle.list_tags(arg).await?;
		let response = http::Response::builder()
			.json(output)
			.map_err(|source| tg::error!(!source, "failed to serialize the output"))?
			.unwrap();
		Ok(response)
	}
}
