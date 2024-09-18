use crate::Server;
use indoc::formatdoc;
use num::ToPrimitive;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_either::Either;
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};

impl Server {
	pub async fn list_tags(&self, arg: tg::tag::list::Arg) -> tg::Result<tg::tag::list::Output> {
		// Handle the remote.
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

		// Get a database connection.
		let connection = self
			.database
			.connection(db::Priority::Low)
			.await
			.map_err(|source| tg::error!(!source, "failed to get database connection"))?;

		#[derive(Debug, serde::Deserialize)]
		struct Row {
			id: u64,
			name: String,
			item: Option<Either<tg::build::Id, tg::object::Id>>,
		}
		let mut rows: Vec<Row> = Vec::new();
		let mut prefix = Vec::new();
		for (idx, component) in arg.pattern.components().into_iter().enumerate() {
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

				tg::tag::pattern::Component::Semver(pattern) => {
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

		// If the last component is normal, check if there are any children beneath it.
		if matches!(arg.pattern.components().last(), Some(tg::tag::pattern::Component::Normal(_))) {
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
			let rows_ = connection
				.query_all_into::<Row>(statement, params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			if !rows_.is_empty() {
				prefix.push(tg::tag::Component::new(rows.first().unwrap().name.clone()));
				rows = rows_;
			}
		}

		// Create the output.
		let length = arg
			.length
			.map_or(usize::MAX, |length| length.to_usize().unwrap());

		let data = rows
			.into_iter()
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
