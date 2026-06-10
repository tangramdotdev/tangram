use {
	crate::Session,
	futures::FutureExt as _,
	indoc::formatdoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
	tangram_index::prelude::*,
};

impl Session {
	pub(crate) async fn create_grant(&self, arg: tg::grant::create::Arg) -> tg::Result<tg::Grant> {
		let resource = self
			.resolve_resource(&arg.resource)
			.await?
			.ok_or_else(|| tg::error!("failed to find the resource"))?;
		if !self
			.authorize(resource, tg::grant::Permission::Admin)
			.await?
		{
			return Err(tg::error!("unauthorized"));
		}
		let session = self.clone();
		let (grant, batch) = self
			.server
			.database
			.run(|transaction| {
				let arg = arg.clone();
				let session = session.clone();
				async move {
					let mut batch = tangram_index::batch::Arg::default();
					let grant = session
						.create_grant_with_transaction(transaction, arg, &mut batch)
						.await?;
					Ok::<_, crate::database::Error>(ControlFlow::Break((grant, batch)))
				}
				.boxed()
			})
			.await?;
		if !batch.is_empty() {
			self.server
				.index
				.batch(batch)
				.await
				.map_err(|error| tg::error!(!error, "failed to index the grant"))?;
		}
		Ok(grant)
	}

	pub(crate) async fn delete_grant(&self, arg: tg::grant::delete::Arg) -> tg::Result<Option<()>> {
		let Some(resource) = self.resolve_resource(&arg.resource).await? else {
			return Ok(None);
		};
		if !self
			.authorize(resource, tg::grant::Permission::Admin)
			.await?
		{
			return Err(tg::error!("unauthorized"));
		}
		let session = self.clone();
		let (output, batch) = self
			.server
			.database
			.run(|transaction| {
				let arg = arg.clone();
				let session = session.clone();
				async move {
					let mut batch = tangram_index::batch::Arg::default();
					let output = session
						.delete_grant_with_transaction(transaction, arg, &mut batch)
						.await?;
					Ok::<_, crate::database::Error>(ControlFlow::Break((output, batch)))
				}
				.boxed()
			})
			.await?;
		if !batch.is_empty() {
			self.server
				.index
				.batch(batch)
				.await
				.map_err(|error| tg::error!(!error, "failed to index the grant"))?;
		}
		Ok(output)
	}

	pub(crate) async fn create_grant_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		arg: tg::grant::create::Arg,
		batch: &mut tangram_index::batch::Arg,
	) -> tg::Result<tg::Grant> {
		let resource = Self::resolve_resource_with_transaction(transaction, &arg.resource)
			.await?
			.ok_or_else(|| tg::error!("failed to find the resource"))?;
		let principal = Self::resolve_principal_with_transaction(transaction, &arg.principal)
			.await?
			.ok_or_else(|| tg::error!("failed to find the principal"))?;
		let created_at = time::OffsetDateTime::now_utc().unix_timestamp();
		let creator = self.context.principal.clone();
		let p = transaction.p();
		let statement = formatdoc!(
			"
				insert into grants (resource, principal, permission, created_at, creator)
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
					creator.as_ref().map(ToString::to_string)
				],
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		if inserted == 0 {
			#[derive(db::row::Deserialize)]
			struct Row {
				created_at: i64,
				#[tangram_database(as = "Option<db::value::FromStr>")]
				creator: Option<tg::Principal>,
			}
			let statement = formatdoc!(
				"
					select created_at, creator
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
				creator: row.creator,
				permission: arg.permission,
				principal,
				resource,
			});
		}
		Self::increment_visibility_with_transaction(transaction, &resource, &principal.to_string())
			.await?;
		batch.put_grants.push(tangram_index::grant::put::Arg {
			created_at,
			creator: creator.clone(),
			permission: arg.permission,
			principal: principal.clone(),
			resource: resource.clone(),
		});
		Ok(tg::Grant {
			created_at,
			creator,
			permission: arg.permission,
			principal,
			resource,
		})
	}

	pub(crate) async fn delete_grant_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		arg: tg::grant::delete::Arg,
		batch: &mut tangram_index::batch::Arg,
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
		batch.delete_grants.push(tangram_index::grant::delete::Arg {
			permission: arg.permission,
			principal,
			resource,
		});
		Ok(Some(()))
	}

	pub(crate) async fn delete_node_grants_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		id: &tg::Id,
		batch: &mut tangram_index::batch::Arg,
	) -> tg::Result<()> {
		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			permission: tg::grant::Permission,
			#[tangram_database(as = "db::value::FromStr")]
			principal: tg::grant::Principal,
			#[tangram_database(as = "db::value::FromStr")]
			resource: tg::Id,
		}
		let p = transaction.p();
		let statement = formatdoc!(
			"
				select resource, permission, principal
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
				principal: tg::principal::Selector::Principal(row.principal),
				resource: tg::grant::Resource::Id(id.clone()),
			};
			self.delete_grant_with_transaction(transaction, arg, batch)
				.await?;
		}
		let statement = formatdoc!(
			"
				select resource, permission, principal
				from grants
				where principal = {p}1;
			"
		);
		let rows = transaction
			.query_all_into::<Row>(statement.into(), db::params![id.to_string()])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		for row in rows {
			batch.delete_grants.push(tangram_index::grant::delete::Arg {
				permission: row.permission,
				principal: row.principal,
				resource: row.resource,
			});
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
			creator: Option<tg::Principal>,
			#[tangram_database(as = "db::value::FromStr")]
			permission: tg::grant::Permission,
			#[tangram_database(as = "db::value::FromStr")]
			principal: tg::grant::Principal,
		}
		let p = transaction.p();
		let statement = formatdoc!(
			"
				select created_at, creator, permission, principal
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
				creator: row.creator,
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
				tg::grant::Principal::Group(id) => {
					let id = id.clone();
					if Self::try_get_node_by_id_with_transaction(transaction, &id.clone().into())
						.await?
						.is_none()
					{
						return Ok(None);
					}
					tg::grant::Principal::Group(id)
				},
				tg::grant::Principal::Organization(id) => {
					let id = id.clone();
					if Self::try_get_node_by_id_with_transaction(transaction, &id.clone().into())
						.await?
						.is_none()
					{
						return Ok(None);
					}
					tg::grant::Principal::Organization(id)
				},
				tg::grant::Principal::Process(id) => tg::grant::Principal::Process(id.clone()),
				tg::grant::Principal::Public => tg::grant::Principal::Public,
				tg::grant::Principal::Root => tg::grant::Principal::Root,
				tg::grant::Principal::Runner => tg::grant::Principal::Runner,
				tg::grant::Principal::Sandbox(id) => tg::grant::Principal::Sandbox(id.clone()),
				tg::grant::Principal::User(id) => {
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
