use {
	crate::{Session, context::Authentication},
	futures::FutureExt as _,
	indoc::formatdoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
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
		self.server
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
			.await
	}

	async fn delete_tags_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		arg: &tg::tag::delete::Arg,
	) -> tg::Result<Vec<tg::tag::Data>> {
		let Some(node) =
			Self::try_get_node_by_selector_with_transaction(transaction, &arg.tag).await?
		else {
			return Ok(Vec::new());
		};
		if node.kind != tg::id::Kind::Tag {
			return Ok(Vec::new());
		}
		let tag = get_tag_data_with_transaction(transaction, &node).await?;
		let p = transaction.p();
		for statement in [
			format!("delete from tag_grants where tag = {p}1;"),
			format!("delete from grants where resource = {p}1 or principal = {p}1;"),
			format!("delete from visibility where resource = {p}1 or principal = {p}1;"),
			format!("delete from tags where id = {p}1;"),
			format!("delete from nodes where id = {p}1;"),
		] {
			transaction
				.execute(statement.into(), db::params![node.id.to_string()])
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		}
		Ok(vec![tag])
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
		path: &[&str],
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;
		let tag = path.join(":").parse()?;
		let mut arg: tg::tag::delete::Arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;
		arg.tag = tag;
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
		Ok(response.body(body).unwrap())
	}
}

pub(crate) async fn get_tag_data_with_transaction(
	transaction: &crate::database::Transaction<'_>,
	node: &crate::node::Node,
) -> tg::Result<tg::tag::Data> {
	#[derive(db::row::Deserialize)]
	struct Row {
		item: String,
	}
	let p = transaction.p();
	let statement = formatdoc!(
		"
			select item
			from tags
			where id = {p}1;
		"
	);
	let row = transaction
		.query_one_into::<Row>(statement.into(), db::params![node.id.to_string()])
		.await
		.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
	let item = serde_json::from_str(&row.item)
		.map_err(|error| tg::error!(!error, "failed to parse the tag item"))?;
	Ok(tg::tag::Data {
		id: node.id.clone().try_into()?,
		item,
		name: node.name.clone(),
		parent: node.parent.clone(),
		specifier: node.specifier.clone(),
	})
}
