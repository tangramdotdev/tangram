use crate::Server;
use indoc::formatdoc;
use itertools::Itertools;
use num::ToPrimitive;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_either::Either;
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};
use tangram_version::Version;

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
			.connection(db::Priority::Low)
			.await
			.map_err(|source| tg::error!(!source, "failed to get database connection"))?;

		#[derive(Clone, Debug, serde::Deserialize)]
		struct Row {
			id: u64,
			name: String,
			item: Option<Either<tg::build::Id, tg::object::Id>>,
		}
		let mut rows: Vec<Row> = Vec::new();
		let mut prefix = Vec::new();
		for (idx, component) in arg.pattern.components().iter().enumerate() {
			match component {
				tg::tag::pattern::Component::Normal(component) => {
					let p = connection.p();
					let statement = formatdoc!(
						"
							select id, name, item
							from tags
							where name = {p}1 and parent = {p}2
						"
					);
					let parent = rows.first().map_or(0, |row| row.id);
					let params = db::params![component, parent];
					rows = connection
						.query_all_into(statement, params)
						.await
						.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

					// Add the leading components to the tag prefix.
					if idx != arg.pattern.components().len() - 1 {
						prefix.push(component.clone());
					}
				},

				tg::tag::pattern::Component::Version(pattern) => {
					let p = connection.p();
					let statement = formatdoc!(
						"
							select id, name, item
							from tags
							where parent = {p}1
						"
					);
					let parent = rows.first().map_or(0, |row| row.id);
					let params = db::params![parent];
					rows = connection
						.query_all_into::<Row>(statement, params)
						.await
						.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
					rows.retain(|row| {
						let Ok(version) = row.name.parse() else {
							return false;
						};
						pattern.matches(&version)
					});
				},

				tg::tag::pattern::Component::Glob => {
					let p = connection.p();
					let statement = formatdoc!(
						"
							select id, name, item
							from tags
							where parent = {p}1
						"
					);
					let parent = rows.first().map_or(0, |row| row.id);
					let params = db::params![parent];
					rows = connection
						.query_all_into::<Row>(statement, params)
						.await
						.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
				},
			}
		}

		// Sort the result.
		let non_semver = rows
			.iter()
			.filter(|row| row.name.parse::<Version>().is_err())
			.sorted_by_key(|row| &row.name)
			.cloned();
		let semver = rows
			.iter()
			.filter(|row| row.name.parse::<Version>().is_ok())
			.sorted_by(|l, r| {
				let l = l.name.parse::<Version>().unwrap();
				let r = r.name.parse::<Version>().unwrap();
				l.cmp(&r)
			})
			.cloned();
		let rows = non_semver.chain(semver);

		// Create the output.
		let length = arg
			.length
			.map_or(usize::MAX, |length| length.to_usize().unwrap());

		let data = rows
			.take(length)
			.map(|row| {
				let mut components = prefix.clone();
				components.push(tg::tag::Component::new(row.name));
				let tag = tg::Tag::with_components(components);
				let item = row.item;
				tg::tag::get::Output { tag, item }
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
