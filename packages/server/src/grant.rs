use {
	crate::{Session, authentication::Authentication},
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
	pub(crate) async fn create_grant(&self, arg: tg::grant::create::Arg) -> tg::Result<tg::Grant> {
		let session = self.clone();
		self.server
			.database
			.run(|transaction| {
				let arg = arg.clone();
				let session = session.clone();
				async move {
					let grant = session
						.create_grant_with_transaction(transaction, arg)
						.await?;
					Ok::<_, crate::database::Error>(ControlFlow::Break(grant))
				}
				.boxed()
			})
			.await
	}

	pub(crate) async fn delete_grant(&self, arg: tg::grant::delete::Arg) -> tg::Result<Option<()>> {
		let session = self.clone();
		self.server
			.database
			.run(|transaction| {
				let arg = arg.clone();
				let session = session.clone();
				async move {
					let output = session
						.delete_grant_with_transaction(transaction, arg)
						.await?;
					Ok::<_, crate::database::Error>(ControlFlow::Break(output))
				}
				.boxed()
			})
			.await
	}

	pub(crate) async fn create_grant_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		arg: tg::grant::create::Arg,
	) -> tg::Result<tg::Grant> {
		let resource = Self::resolve_resource_with_transaction(transaction, &arg.resource)
			.await?
			.ok_or_else(|| tg::error!("failed to find the resource"))?;
		let principal = Self::resolve_principal_with_transaction(transaction, &arg.principal)
			.await?
			.ok_or_else(|| tg::error!("failed to find the principal"))?;
		let created_at = time::OffsetDateTime::now_utc().unix_timestamp();
		let created_by = match self.context.authentication.as_ref() {
			Some(Authentication::User(user)) => Some(user.id.clone()),
			_ => None,
		};
		let p = transaction.p();
		let statement = formatdoc!(
			"
				insert into grants (resource, principal, permission, created_at, created_by)
				values ({p}1, {p}2, {p}3, {p}4, {p}5)
				on conflict (resource, principal, permission) do nothing;
			"
		);
		let inserted = transaction
			.execute(
				statement.into(),
				db::params![
					resource.to_string(),
					principal.to_string(),
					arg.permission.to_string(),
					created_at,
					created_by.as_ref().map(ToString::to_string)
				],
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		if inserted == 0 {
			#[derive(db::row::Deserialize)]
			struct Row {
				created_at: i64,
				#[tangram_database(as = "Option<db::value::FromStr>")]
				created_by: Option<tg::user::Id>,
			}
			let statement = formatdoc!(
				"
					select created_at, created_by
					from grants
					where resource = {p}1 and principal = {p}2 and permission = {p}3;
				"
			);
			let row = transaction
				.query_one_into::<Row>(
					statement.into(),
					db::params![
						resource.to_string(),
						principal.to_string(),
						arg.permission.to_string()
					],
				)
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
			return Ok(tg::Grant {
				created_at: row.created_at,
				created_by: row.created_by,
				permission: arg.permission,
				principal,
				resource,
			});
		}
		Self::increment_visibility_with_transaction(transaction, &resource, &principal.to_string())
			.await?;
		Ok(tg::Grant {
			created_at,
			created_by,
			permission: arg.permission,
			principal,
			resource,
		})
	}

	pub(crate) async fn delete_grant_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		arg: tg::grant::delete::Arg,
	) -> tg::Result<Option<()>> {
		let Some(resource) =
			Self::resolve_resource_with_transaction(transaction, &arg.resource).await?
		else {
			return Ok(None);
		};
		let Some(principal) =
			Self::resolve_principal_with_transaction(transaction, &arg.principal).await?
		else {
			return Ok(None);
		};
		let p = transaction.p();
		let statement = formatdoc!(
			"
				delete from grants
				where resource = {p}1 and principal = {p}2 and permission = {p}3;
			"
		);
		let deleted = transaction
			.execute(
				statement.into(),
				db::params![
					resource.to_string(),
					principal.to_string(),
					arg.permission.to_string()
				],
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		if deleted == 0 {
			return Ok(None);
		}
		Self::decrement_visibility_with_transaction(transaction, &resource, &principal.to_string())
			.await?;
		Ok(Some(()))
	}

	pub(crate) async fn delete_node_grants_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		id: &tg::Id,
	) -> tg::Result<()> {
		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			permission: tg::grant::Permission,
			#[tangram_database(as = "db::value::FromStr")]
			principal: tg::grant::Principal,
		}
		let p = transaction.p();
		let statement = formatdoc!(
			"
				select permission, principal
				from grants
				where resource = {p}1;
			"
		);
		let rows = transaction
			.query_all_into::<Row>(statement.into(), db::params![id.to_string()])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		for row in rows {
			let arg = tg::grant::delete::Arg {
				permission: row.permission,
				principal: tg::principal::Selector::Principal(row.principal.into()),
				resource: tg::grant::Resource::Id(id.clone()),
			};
			self.delete_grant_with_transaction(transaction, arg).await?;
		}
		for statement in [
			format!("delete from grants where principal = {p}1;"),
			format!("delete from visibility where principal = {p}1;"),
			format!("delete from visibility where resource = {p}1;"),
		] {
			transaction
				.execute(statement.into(), db::params![id.to_string()])
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		}
		Ok(())
	}

	pub(crate) async fn list_direct_grants_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		resource: &tg::Id,
	) -> tg::Result<Vec<tg::Grant>> {
		#[derive(db::row::Deserialize)]
		struct Row {
			created_at: i64,
			#[tangram_database(as = "Option<db::value::FromStr>")]
			created_by: Option<tg::user::Id>,
			#[tangram_database(as = "db::value::FromStr")]
			permission: tg::grant::Permission,
			#[tangram_database(as = "db::value::FromStr")]
			principal: tg::grant::Principal,
		}
		let p = transaction.p();
		let statement = formatdoc!(
			"
				select created_at, created_by, permission, principal
				from grants
				where resource = {p}1
				order by principal, permission;
			"
		);
		let rows = transaction
			.query_all_into::<Row>(statement.into(), db::params![resource.to_string()])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		Ok(rows
			.into_iter()
			.map(|row| tg::Grant {
				created_at: row.created_at,
				created_by: row.created_by,
				permission: row.permission,
				principal: row.principal,
				resource: resource.clone(),
			})
			.collect())
	}

	async fn resolve_principal_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		principal: &tg::principal::Selector,
	) -> tg::Result<Option<tg::grant::Principal>> {
		let principal = match principal {
			tg::principal::Selector::Principal(principal) => match principal {
				tg::Principal::Group(id) => {
					let id = id.clone();
					if Self::try_get_node_by_id_with_transaction(transaction, &id.clone().into())
						.await?
						.is_none()
					{
						return Ok(None);
					}
					tg::grant::Principal::Group(id)
				},
				tg::Principal::Organization(id) => {
					let id = id.clone();
					if Self::try_get_node_by_id_with_transaction(transaction, &id.clone().into())
						.await?
						.is_none()
					{
						return Ok(None);
					}
					tg::grant::Principal::Organization(id)
				},
				tg::Principal::Root => tg::grant::Principal::Root,
				tg::Principal::User(id) => {
					let id = id.clone();
					if Self::try_get_node_by_id_with_transaction(transaction, &id.clone().into())
						.await?
						.is_none()
					{
						return Ok(None);
					}
					tg::grant::Principal::User(id)
				},
			},
			tg::principal::Selector::Specifier(specifier) => {
				let Some(node) =
					Self::try_get_node_by_specifier_with_transaction(transaction, specifier)
						.await?
				else {
					return Ok(None);
				};
				match node.kind {
					tg::id::Kind::Group => tg::grant::Principal::Group(node.id.try_into()?),
					tg::id::Kind::Organization => {
						tg::grant::Principal::Organization(node.id.try_into()?)
					},
					tg::id::Kind::User => tg::grant::Principal::User(node.id.try_into()?),
					_ => return Ok(None),
				}
			},
		};
		Ok(Some(principal))
	}

	pub(crate) async fn create_grant_request(
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
		let output = self.create_grant(arg).await?;
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
		let response = response.body(body).unwrap().boxed_body();
		Ok(response)
	}

	pub(crate) async fn delete_grant_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;
		let Some(()) = self.delete_grant(arg).await? else {
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
}
