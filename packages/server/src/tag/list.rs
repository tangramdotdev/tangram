use {
	crate::{Context, Database, Server},
	futures::{TryStreamExt as _, stream::FuturesUnordered},
	tangram_client::prelude::*,
	tangram_http::request::Ext as _,
	tracing::Instrument as _,
};

#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;

impl Server {
	#[tracing::instrument(level = "trace", name = "list_tags", skip_all, fields(pattern = %arg.pattern))]
	pub(crate) async fn list_tags_with_context(
		&self,
		context: &Context,
		arg: tg::tag::list::Arg,
	) -> tg::Result<tg::tag::list::Output> {
		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		let mut output = tg::tag::list::Output { data: Vec::new() };

		// List the local tags if requested.
		if Self::local(arg.local, arg.remotes.as_ref()) {
			let local_output = self
				.list_tags_local(arg.clone())
				.instrument(tracing::trace_span!("local"))
				.await
				.map_err(|source| tg::error!(!source, "failed to list local tags"))?;
			output.data.extend(local_output.data);
		}

		// List the remote tags using try_get_tag instead of client.list_tags.
		let remotes = self
			.remotes(arg.local, arg.remotes.clone())
			.instrument(tracing::trace_span!("get_remotes"))
			.await
			.map_err(|source| tg::error!(!source, "failed to get the remotes"))?;
		let remote_outputs = remotes
			.into_iter()
			.map(|remote| {
				let arg = arg.clone();
				let span = tracing::trace_span!("remote", %remote);
				async move {
					self.list_tags_remote(context, &remote, &arg).await.map_err(
						|source| tg::error!(!source, %remote, "failed to list remote tags"),
					)
				}
				.instrument(span)
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<Vec<_>>()
			.await?;

		output
			.data
			.extend(remote_outputs.into_iter().flat_map(|output| output.data));

		// Sort by tag, then by remote (local first for ties).
		if arg.reverse {
			output.data.sort_by(|a, b| match b.tag.cmp(&a.tag) {
				std::cmp::Ordering::Equal => a.remote.cmp(&b.remote),
				other => other,
			});
		} else {
			output.data.sort_by(|a, b| match a.tag.cmp(&b.tag) {
				std::cmp::Ordering::Equal => a.remote.cmp(&b.remote),
				other => other,
			});
		}

		Ok(output)
	}

	/// List tags on a remote by walking the pattern tree using `try_get_tag`.
	async fn list_tags_remote(
		&self,
		context: &Context,
		remote: &str,
		arg: &tg::tag::list::Arg,
	) -> tg::Result<tg::tag::list::Output> {
		// Split the pattern into an exact prefix and a filter suffix.
		let components: Vec<&str> = if arg.pattern.is_empty() {
			Vec::new()
		} else {
			arg.pattern.components().collect()
		};
		let split_index = components
			.iter()
			.position(|c| c.contains(['*', '=', '>', '<', '^']))
			.unwrap_or(components.len());
		let prefix_components = &components[..split_index];
		let suffix_components = &components[split_index..];

		// Build the prefix tag and fetch it with a single try_get_tag call.
		let mut prefix_tag = tg::Tag::empty();
		for component in prefix_components {
			prefix_tag.push(component);
		}
		let get_arg = tg::tag::get::Arg {
			cached: None,
			local: Some(false),
			remotes: Some(vec![remote.to_owned()]),
			ttl: arg.ttl,
		};
		let prefix_output = self
			.try_get_tag_with_context(context, &prefix_tag, get_arg)
			.await
			.map_err(|source| {
				tg::error!(!source, "failed to get the prefix tag from the remote")
			})?;

		// If the prefix is a leaf with no suffix, return it directly.
		if suffix_components.is_empty() {
			if let Some(output) = prefix_output {
				if output.item.is_some() {
					// This is a leaf tag. Return it as a single result.
					return Ok(tg::tag::list::Output {
						data: vec![tg::tag::get::Output {
							children: None,
							item: output.item,
							remote: Some(remote.to_owned()),
							tag: prefix_tag,
						}],
					});
				}
				// This is a branch tag with no suffix. Expand its children.
				if let Some(children) = &output.children {
					let mut data = Vec::new();
					for child in children {
						let mut child_tag = prefix_tag.clone();
						child_tag.push(&child.component);
						if child.item.is_some() {
							data.push(tg::tag::get::Output {
								children: None,
								item: child.item.clone(),
								remote: Some(remote.to_owned()),
								tag: child_tag,
							});
						} else {
							data.push(tg::tag::get::Output {
								children: None,
								item: None,
								remote: Some(remote.to_owned()),
								tag: child_tag,
							});
						}
					}
					if arg.recursive {
						self.expand_remote_branches(context, remote, &mut data)
							.await?;
					}
					return Ok(tg::tag::list::Output { data });
				}
			}
			return Ok(tg::tag::list::Output { data: Vec::new() });
		}

		// The prefix output must exist and have children to continue.
		let Some(prefix_output) = prefix_output else {
			return Ok(tg::tag::list::Output { data: Vec::new() });
		};
		let Some(children) = prefix_output.children else {
			return Ok(tg::tag::list::Output { data: Vec::new() });
		};

		// Walk the suffix components, filtering children at each level.
		let mut current: Vec<(tg::Tag, Option<tg::Either<tg::object::Id, tg::process::Id>>)> =
			children
				.into_iter()
				.filter(|child| tg::tag::pattern::matches(&child.component, suffix_components[0]))
				.map(|child| {
					let mut tag = prefix_tag.clone();
					tag.push(&child.component);
					(tag, child.item)
				})
				.collect();

		// Process remaining suffix components.
		for &suffix in &suffix_components[1..] {
			let mut next = Vec::new();
			for (tag, _item) in &current {
				let get_arg = tg::tag::get::Arg {
					cached: None,
					local: Some(false),
					remotes: Some(vec![remote.to_owned()]),
					ttl: None,
				};
				let output = self
					.try_get_tag_with_context(context, tag, get_arg)
					.await
					.map_err(|source| {
						tg::error!(!source, "failed to get the tag from the remote")
					})?;
				if let Some(output) = output
					&& let Some(children) = output.children
				{
					for child in children {
						if tg::tag::pattern::matches(&child.component, suffix) {
							let mut child_tag = tag.clone();
							child_tag.push(&child.component);
							next.push((child_tag, child.item));
						}
					}
				}
			}
			current = next;
		}

		// Build the output.
		let mut data: Vec<tg::tag::get::Output> = current
			.into_iter()
			.map(|(tag, item)| tg::tag::get::Output {
				children: None,
				item,
				remote: Some(remote.to_owned()),
				tag,
			})
			.collect();

		// Recursively expand branches if requested.
		if arg.recursive {
			self.expand_remote_branches(context, remote, &mut data)
				.await?;
		}

		Ok(tg::tag::list::Output { data })
	}

	/// Recursively expand branch tags by fetching their children from the remote.
	async fn expand_remote_branches(
		&self,
		context: &Context,
		remote: &str,
		data: &mut Vec<tg::tag::get::Output>,
	) -> tg::Result<()> {
		let mut i = 0;
		while i < data.len() {
			if data[i].item.is_none() {
				let tag = data[i].tag.clone();
				let get_arg = tg::tag::get::Arg {
					cached: None,
					local: Some(false),
					remotes: Some(vec![remote.to_owned()]),
					ttl: None,
				};
				let output = self
					.try_get_tag_with_context(context, &tag, get_arg)
					.await
					.map_err(|source| {
						tg::error!(!source, "failed to get the tag from the remote")
					})?;
				if let Some(output) = output
					&& let Some(children) = output.children
				{
					for child in children {
						let mut child_tag = tag.clone();
						child_tag.push(&child.component);
						data.push(tg::tag::get::Output {
							children: None,
							item: child.item,
							remote: Some(remote.to_owned()),
							tag: child_tag,
						});
					}
				}
			}
			i += 1;
		}
		Ok(())
	}

	async fn list_tags_local(&self, arg: tg::tag::list::Arg) -> tg::Result<tg::tag::list::Output> {
		match &self.database {
			#[cfg(feature = "postgres")]
			Database::Postgres(database) => self.list_tags_postgres(database, arg).await,
			#[cfg(feature = "sqlite")]
			Database::Sqlite(database) => self.list_tags_sqlite(database, arg).await,
		}
	}

	pub(crate) async fn handle_list_tags_request(
		&self,
		request: tangram_http::Request,
		context: &Context,
	) -> tg::Result<tangram_http::Response> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;

		// Get the arg.
		let arg = request
			.query_params()
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the query params"))?
			.unwrap_or_default();

		// List the tags.
		let output = self.list_tags_with_context(context, arg).await?;

		// Create the response.
		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::APPLICATION, mime::JSON)) => {
				let content_type = mime::APPLICATION_JSON;
				let body = serde_json::to_vec(&output).unwrap();
				(
					Some(content_type),
					tangram_http::body::Boxed::with_bytes(body),
				)
			},
			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		};

		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		let response = response.body(body).unwrap();
		Ok(response)
	}
}
