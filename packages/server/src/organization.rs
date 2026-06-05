use {
	crate::{Session, group::organization_member_to_principal, user::parse_selector},
	futures::FutureExt as _,
	indoc::formatdoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
};

impl Session {
	pub(crate) async fn create_organization(
		&self,
		arg: tg::organization::create::Arg,
	) -> tg::Result<tg::organization::create::Output> {
		let session = self.clone();
		self.server
			.database
			.run(|transaction| {
				let arg = arg.clone();
				let session = session.clone();
				async move {
					let organization = session
						.create_organization_with_transaction(transaction, arg)
						.await?;
					Ok::<_, crate::database::Error>(ControlFlow::Break(
						tg::organization::create::Output { organization },
					))
				}
				.boxed()
			})
			.await
	}

	async fn create_organization_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		arg: tg::organization::create::Arg,
	) -> tg::Result<tg::Organization> {
		if arg.specifier.components().count() != 1 {
			return Err(tg::error!("invalid organization specifier"));
		}
		let id = tg::organization::Id::new();
		let node = Self::create_node_with_transaction(
			transaction,
			&id.clone().into(),
			tg::id::Kind::Organization,
			&arg.specifier,
			None,
		)
		.await?;
		let p = transaction.p();
		let statement = formatdoc!(
			"
				insert into organizations (id, name)
				values ({p}1, {p}2);
			"
		);
		transaction
			.execute(
				statement.into(),
				db::params![id.to_string(), node.name.clone()],
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		if let Some(crate::context::Authentication::User(user)) =
			self.context.authentication.as_ref()
		{
			let arg = tg::grant::create::Arg {
				principal: tg::grant::Principal::User(user.id.clone()),
				permission: tg::grant::Permission::Admin,
				resource: tg::grant::Resource::Id(id.clone().into()),
			};
			self.create_grant_with_transaction(transaction, arg).await?;
		}
		organization_from_node(node)
	}

	pub(crate) async fn try_get_organization(
		&self,
		organization: &tg::organization::Selector,
	) -> tg::Result<Option<tg::Organization>> {
		let mut connection = self
			.server
			.database
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let transaction = connection
			.transaction()
			.await
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		let Some(node) =
			Self::try_get_node_by_selector_with_transaction(&transaction, organization).await?
		else {
			return Ok(None);
		};
		if node.kind != tg::id::Kind::Organization
			|| !self
				.node_is_visible_with_transaction(&transaction, &node.id)
				.await?
		{
			return Ok(None);
		}
		organization_from_node(node).map(Some)
	}

	pub(crate) async fn try_delete_organization(
		&self,
		organization: &tg::organization::Selector,
	) -> tg::Result<Option<()>> {
		let session = self.clone();
		self.server
			.database
			.run(|transaction| {
				let organization = organization.clone();
				let session = session.clone();
				async move {
					let output = session
						.delete_organization_with_transaction(transaction, &organization)
						.await?;
					Ok::<_, crate::database::Error>(ControlFlow::Break(output))
				}
				.boxed()
			})
			.await
	}

	async fn delete_organization_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		organization: &tg::organization::Selector,
	) -> tg::Result<Option<()>> {
		let Some(node) =
			Self::try_get_node_by_selector_with_transaction(transaction, organization).await?
		else {
			return Ok(None);
		};
		if node.kind != tg::id::Kind::Organization {
			return Ok(None);
		}
		let p = transaction.p();
		for statement in [
			format!("delete from organization_members where organization = {p}1;"),
			format!("delete from grants where resource = {p}1 or principal = {p}1;"),
			format!("delete from visibility where resource = {p}1 or principal = {p}1;"),
			format!("delete from organizations where id = {p}1;"),
			format!("delete from nodes where id = {p}1;"),
		] {
			transaction
				.execute(statement.into(), db::params![node.id.to_string()])
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		}
		Ok(Some(()))
	}

	pub(crate) async fn list_organization_members(
		&self,
		organization: &tg::organization::Selector,
	) -> tg::Result<tg::organization::members::list::Output> {
		let mut connection = self
			.server
			.database
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let transaction = connection
			.transaction()
			.await
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		let organization =
			Self::try_get_node_by_selector_with_transaction(&transaction, organization)
				.await?
				.ok_or_else(|| tg::error!("failed to find the organization"))?;
		if organization.kind != tg::id::Kind::Organization
			|| !self
				.node_is_visible_with_transaction(&transaction, &organization.id)
				.await?
		{
			return Err(tg::error!("failed to find the organization"));
		}
		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			member: tg::Id,
		}
		let p = transaction.p();
		let statement = formatdoc!(
			"
				select member
				from organization_members
				where organization = {p}1
				order by member;
			"
		);
		let rows = transaction
			.query_all_into::<Row>(statement.into(), db::params![organization.id.to_string()])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let data = rows
			.into_iter()
			.map(|row| row.member.try_into())
			.collect::<tg::Result<_>>()?;
		Ok(tg::organization::members::list::Output { data })
	}

	pub(crate) async fn add_organization_member(
		&self,
		organization: &tg::organization::Selector,
		member: &tg::organization::Member,
	) -> tg::Result<()> {
		let session = self.clone();
		self.server
			.database
			.run(|transaction| {
				let organization = organization.clone();
				let member = member.clone();
				let session = session.clone();
				async move {
					session
						.add_organization_member_with_transaction(
							transaction,
							&organization,
							&member,
						)
						.await?;
					Ok::<_, crate::database::Error>(ControlFlow::Break(()))
				}
				.boxed()
			})
			.await
	}

	async fn add_organization_member_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		organization: &tg::organization::Selector,
		member: &tg::organization::Member,
	) -> tg::Result<()> {
		let organization =
			Self::try_get_node_by_selector_with_transaction(transaction, organization)
				.await?
				.ok_or_else(|| tg::error!("failed to find the organization"))?;
		if organization.kind != tg::id::Kind::Organization {
			return Err(tg::error!("failed to find the organization"));
		}
		let member_id: tg::Id = member.clone().into();
		if Self::try_get_node_by_id_with_transaction(transaction, &member_id)
			.await?
			.is_none()
		{
			return Err(tg::error!("failed to find the member"));
		}
		let p = transaction.p();
		let statement = formatdoc!(
			"
				insert into organization_members (organization, member)
				values ({p}1, {p}2)
				on conflict (organization, member) do nothing;
			"
		);
		transaction
			.execute(
				statement.into(),
				db::params![organization.id.to_string(), member_id.to_string()],
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let arg = tg::grant::create::Arg {
			principal: organization_member_to_principal(member),
			permission: tg::grant::Permission::Read,
			resource: tg::grant::Resource::Id(organization.id),
		};
		self.create_grant_with_transaction(transaction, arg).await?;
		Ok(())
	}

	pub(crate) async fn remove_organization_member(
		&self,
		organization: &tg::organization::Selector,
		member: &tg::organization::Member,
	) -> tg::Result<Option<()>> {
		let session = self.clone();
		self.server
			.database
			.run(|transaction| {
				let organization = organization.clone();
				let member = member.clone();
				let session = session.clone();
				async move {
					let output = session
						.remove_organization_member_with_transaction(
							transaction,
							&organization,
							&member,
						)
						.await?;
					Ok::<_, crate::database::Error>(ControlFlow::Break(output))
				}
				.boxed()
			})
			.await
	}

	async fn remove_organization_member_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		organization: &tg::organization::Selector,
		member: &tg::organization::Member,
	) -> tg::Result<Option<()>> {
		let Some(organization) =
			Self::try_get_node_by_selector_with_transaction(transaction, organization).await?
		else {
			return Ok(None);
		};
		let member_id: tg::Id = member.clone().into();
		let p = transaction.p();
		let statement = formatdoc!(
			"
				delete from organization_members
				where organization = {p}1 and member = {p}2;
			"
		);
		let deleted = transaction
			.execute(
				statement.into(),
				db::params![organization.id.to_string(), member_id.to_string()],
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		if deleted == 0 {
			return Ok(None);
		}
		let arg = tg::grant::delete::Arg {
			principal: organization_member_to_principal(member),
			permission: tg::grant::Permission::Read,
			resource: tg::grant::Resource::Id(organization.id),
		};
		self.delete_grant_with_transaction(transaction, arg).await?;
		Ok(Some(()))
	}

	pub(crate) async fn try_get_organization_grants(
		&self,
		organization: &tg::organization::Selector,
		arg: tg::organization::grants::Arg,
	) -> tg::Result<Option<tg::organization::grants::Output>> {
		let _ = arg;
		let mut connection = self
			.server
			.database
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let transaction = connection
			.transaction()
			.await
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		let Some(node) =
			Self::try_get_node_by_selector_with_transaction(&transaction, organization).await?
		else {
			return Ok(None);
		};
		if node.kind != tg::id::Kind::Organization
			|| !self
				.node_is_visible_with_transaction(&transaction, &node.id)
				.await?
		{
			return Ok(None);
		}
		let data = Self::list_direct_grants_with_transaction(&transaction, &node.id).await?;
		Ok(Some(tg::organization::grants::Output { data }))
	}

	pub(crate) async fn create_organization_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;
		let arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;
		let output = self.create_organization(arg).await?;
		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::APPLICATION, mime::JSON)) => {
				let content_type = mime::APPLICATION_JSON;
				let body = serde_json::to_vec(&output).unwrap();
				(Some(content_type), BoxBody::with_bytes(body))
			},
			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		};
		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		Ok(response.body(body).unwrap().boxed_body())
	}

	pub(crate) async fn try_get_organization_request(
		&self,
		request: http::Request<BoxBody>,
		organization: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;
		let organization = parse_selector::<tg::organization::Id>(organization)?;
		let Some(output) = self.try_get_organization(&organization).await? else {
			let response = http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body();
			return Ok(response);
		};
		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::APPLICATION, mime::JSON)) => {
				let content_type = mime::APPLICATION_JSON;
				let body = serde_json::to_vec(&output).unwrap();
				(Some(content_type), BoxBody::with_bytes(body))
			},
			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		};
		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		Ok(response.body(body).unwrap().boxed_body())
	}

	pub(crate) async fn try_delete_organization_request(
		&self,
		request: http::Request<BoxBody>,
		organization: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let _ = request;
		let organization = parse_selector::<tg::organization::Id>(organization)?;
		let Some(()) = self.try_delete_organization(&organization).await? else {
			let response = http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body();
			return Ok(response);
		};
		let response = http::Response::builder().empty().unwrap().boxed_body();
		Ok(response)
	}

	pub(crate) async fn list_organization_members_request(
		&self,
		request: http::Request<BoxBody>,
		organization: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;
		let organization = parse_selector::<tg::organization::Id>(organization)?;
		let output = self.list_organization_members(&organization).await?;
		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::APPLICATION, mime::JSON)) => {
				let content_type = mime::APPLICATION_JSON;
				let body = serde_json::to_vec(&output).unwrap();
				(Some(content_type), BoxBody::with_bytes(body))
			},
			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		};
		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		Ok(response.body(body).unwrap().boxed_body())
	}

	pub(crate) async fn add_organization_member_request(
		&self,
		request: http::Request<BoxBody>,
		organization: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let arg: tg::organization::members::add::Arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;
		let organization = parse_selector::<tg::organization::Id>(organization)?;
		self.add_organization_member(&organization, &arg.member)
			.await?;
		let response = http::Response::builder().empty().unwrap().boxed_body();
		Ok(response)
	}

	pub(crate) async fn remove_organization_member_request(
		&self,
		request: http::Request<BoxBody>,
		organization: &str,
		member: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let _ = request;
		let organization = parse_selector::<tg::organization::Id>(organization)?;
		let member = member.replace(':', "/").parse()?;
		let Some(()) = self
			.remove_organization_member(&organization, &member)
			.await?
		else {
			let response = http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body();
			return Ok(response);
		};
		let response = http::Response::builder().empty().unwrap().boxed_body();
		Ok(response)
	}

	pub(crate) async fn try_get_organization_grants_request(
		&self,
		request: http::Request<BoxBody>,
		organization: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;
		let arg = request
			.query_params()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.unwrap_or(tg::organization::grants::Arg { location: None });
		let organization = parse_selector::<tg::organization::Id>(organization)?;
		let Some(output) = self.try_get_organization_grants(&organization, arg).await? else {
			let response = http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body();
			return Ok(response);
		};
		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::APPLICATION, mime::JSON)) => {
				let content_type = mime::APPLICATION_JSON;
				let body = serde_json::to_vec(&output).unwrap();
				(Some(content_type), BoxBody::with_bytes(body))
			},
			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		};
		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		Ok(response.body(body).unwrap().boxed_body())
	}
}

fn organization_from_node(node: crate::node::Node) -> tg::Result<tg::Organization> {
	Ok(tg::Organization {
		id: node.id.try_into()?,
		name: node.name,
		specifier: node.specifier,
	})
}
