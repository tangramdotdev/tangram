use {
	crate::{Session, authentication::Authentication, tag::get_tag_data_with_transaction},
	futures::FutureExt as _,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_index::prelude::*,
};

impl Session {
	pub(crate) async fn delete_tags(
		&self,
		arg: tg::tag::delete::Arg,
	) -> tg::Result<tg::tag::delete::Output> {
		if self
			.context
			.authentication
			.as_ref()
			.is_some_and(Authentication::is_process)
		{
			return Err(tg::error!("unauthorized"));
		}
		let location = self
			.server
			.location(arg.location.as_ref())
			.map_err(|error| tg::error!(!error, "failed to resolve the location"))?;
		match location {
			tg::Location::Local(_) => self.delete_tags_local(arg).await,
			tg::Location::Remote(remote) => self.delete_tags_remote(arg, remote).await,
		}
	}

	async fn delete_tags_local(
		&self,
		arg: tg::tag::delete::Arg,
	) -> tg::Result<tg::tag::delete::Output> {
		let session = self.clone();
		let output = self
			.server
			.database
			.run(|transaction| {
				let arg = arg.clone();
				let session = session.clone();
				async move {
					let deleted = session
						.delete_tags_with_transaction(transaction, &arg)
						.await?;
					Ok::<_, crate::database::Error>(ControlFlow::Break(tg::tag::delete::Output {
						deleted,
					}))
				}
				.boxed()
			})
			.await?;
		let tags = output
			.deleted
			.iter()
			.map(|tag| tag.specifier.clone())
			.collect::<Vec<_>>();
		if !tags.is_empty() {
			self.server
				.index
				.delete_tags(&tags)
				.await
				.map_err(|error| tg::error!(!error, "failed to index the deleted tags"))?;
		}
		Ok(output)
	}

	async fn delete_tags_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		arg: &tg::tag::delete::Arg,
	) -> tg::Result<Vec<tg::tag::Data>> {
		if arg.pattern.is_empty() {
			return Err(tg::error!("cannot delete an empty pattern"));
		}
		if !arg.recursive && arg.pattern.contains_operators() {
			return Err(tg::error!(
				"cannot delete multiple tags without --recursive"
			));
		}
		let tags = self
			.list_tags_to_delete_with_transaction(transaction, &arg.pattern, arg.recursive)
			.await?;
		let p = transaction.p();
		for tag in &tags {
			self.delete_node_grants_with_transaction(transaction, &tag.id.clone().into())
				.await?;
			for statement in [
				format!("delete from tags where id = {p}1;"),
				format!("delete from nodes where id = {p}1;"),
			] {
				transaction
					.execute(statement.into(), db::params![tag.id.to_string()])
					.await
					.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
			}
		}
		Ok(tags)
	}

	async fn list_tags_to_delete_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		pattern: &tg::specifier::Pattern,
		recursive: bool,
	) -> tg::Result<Vec<tg::tag::Data>> {
		if !recursive && !pattern.contains_operators() {
			let specifier = pattern.clone().try_into()?;
			let Some(node) =
				Self::try_get_node_by_specifier_with_transaction(transaction, &specifier).await?
			else {
				return Ok(Vec::new());
			};
			if node.kind != tg::id::Kind::Tag {
				return Ok(Vec::new());
			}
			return Ok(vec![
				get_tag_data_with_transaction(transaction, &node).await?,
			]);
		}
		let output = self
			.list_local(tg::list::Arg {
				groups: false,
				pattern: pattern.clone(),
				recursive,
				tags: true,
				..tg::list::Arg::default()
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to list the tags"))?;
		let mut tags = Vec::new();
		for entry in output.data {
			let tg::list::Entry::Tag { tag, .. } = entry else {
				continue;
			};
			let Some(node) =
				Self::try_get_node_by_specifier_with_transaction(transaction, &tag).await?
			else {
				continue;
			};
			tags.push(get_tag_data_with_transaction(transaction, &node).await?);
		}
		tags.sort_by(|a, b| {
			let a_depth = a.specifier.components().count();
			let b_depth = b.specifier.components().count();
			b_depth
				.cmp(&a_depth)
				.then_with(|| a.specifier.cmp(&b.specifier))
		});
		Ok(tags)
	}

	async fn delete_tags_remote(
		&self,
		mut arg: tg::tag::delete::Arg,
		remote: tg::location::Remote,
	) -> tg::Result<tg::tag::delete::Output> {
		let client = self.get_remote_session(&remote.name).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
		)?;
		arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
		client
			.delete_tags(arg)
			.await
			.map_err(|error| tg::error!(!error, remote = %remote.name, "failed to delete the tags"))
	}

	pub(crate) async fn delete_tags_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;
		let arg = request
			.query_params()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.ok_or_else(|| tg::error!("expected query params"))?;
		let output = self.delete_tags(arg).await?;
		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::APPLICATION, mime::JSON)) => {
				let body = serde_json::to_vec(&output).unwrap();
				(Some(mime::APPLICATION_JSON), BoxBody::with_bytes(body))
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
