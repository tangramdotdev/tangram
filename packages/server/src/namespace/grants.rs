use {
	crate::Session,
	tangram_client::prelude::*,
	tangram_database::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
};

enum NamespaceGrantGrantee {
	User(String),
	Group(String),
	Public,
}

impl Session {
	pub(crate) async fn grant_namespace_permission(
		&self,
		arg: tg::namespace::grant::Arg,
	) -> tg::Result<tg::Grant> {
		if self.context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}
		let grantee = Self::namespace_grant_grantee(arg.user, arg.group, arg.public)?;
		if matches!(grantee, NamespaceGrantGrantee::Public)
			&& arg.permission != tg::Permission::Read
		{
			return Err(tg::error!("public grants may only be read"));
		}
		self.authorize_namespace(&arg.namespace, tg::Permission::Admin)
			.await?;
		let created_by = self
			.context
			.authentication
			.as_ref()
			.and_then(|authentication| authentication.try_unwrap_user_ref().ok())
			.map(|user| user.id.clone());

		let mut connection = self
			.server
			.database
			.write_connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let transaction = connection
			.transaction()
			.await
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		let namespace_id =
			Self::get_or_create_namespace_with_transaction(&transaction, &arg.namespace).await?;
		let grant = match grantee {
			NamespaceGrantGrantee::User(user) => {
				let user = Self::try_get_user_with_transaction(&transaction, &user)
					.await?
					.ok_or_else(|| tg::error!("failed to find the user"))?;
				Self::grant_namespace_to_user_with_transaction(
					&transaction,
					&arg.namespace,
					namespace_id,
					&user.id,
					arg.permission,
					created_by.as_ref(),
				)
				.await?
			},
			NamespaceGrantGrantee::Group(group) => {
				let group = Self::try_get_group_with_transaction(&transaction, &group)
					.await?
					.ok_or_else(|| tg::error!("failed to find the group"))?;
				Self::grant_namespace_to_group_with_transaction(
					&transaction,
					&arg.namespace,
					namespace_id,
					&group.id,
					arg.permission,
					created_by.as_ref(),
				)
				.await?
			},
			NamespaceGrantGrantee::Public => {
				Self::grant_namespace_public_read_with_transaction(
					&transaction,
					&arg.namespace,
					namespace_id,
					created_by.as_ref(),
				)
				.await?
			},
		};
		transaction
			.commit()
			.await
			.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;
		Ok(grant)
	}

	pub(crate) async fn list_namespace_grants(
		&self,
		arg: tg::namespace::grants::Arg,
	) -> tg::Result<Option<tg::namespace::grants::Output>> {
		if self.context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}
		self.authorize_namespace(&arg.namespace, tg::Permission::Admin)
			.await?;

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
		let Some(namespace_id) =
			Self::try_get_namespace_id_with_transaction(&transaction, &arg.namespace).await?
		else {
			return Ok(None);
		};
		let data =
			Self::list_namespace_grants_for_namespace_with_transaction(&transaction, namespace_id)
				.await?;
		Ok(Some(tg::namespace::grants::Output { data }))
	}

	pub(crate) async fn revoke_namespace_permission(
		&self,
		arg: tg::namespace::revoke::Arg,
	) -> tg::Result<Option<()>> {
		if self.context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}
		let grantee = Self::namespace_grant_grantee(arg.user, arg.group, arg.public)?;
		if matches!(grantee, NamespaceGrantGrantee::Public)
			&& arg.permission != tg::Permission::Read
		{
			return Err(tg::error!("public grants may only be read"));
		}
		self.authorize_namespace(&arg.namespace, tg::Permission::Admin)
			.await?;

		let mut connection = self
			.server
			.database
			.write_connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let transaction = connection
			.transaction()
			.await
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		let Some(namespace_id) =
			Self::try_get_namespace_id_with_transaction(&transaction, &arg.namespace).await?
		else {
			return Ok(None);
		};
		let output = match grantee {
			NamespaceGrantGrantee::User(user) => {
				let Some(user) = Self::try_get_user_with_transaction(&transaction, &user).await?
				else {
					return Ok(None);
				};
				Self::revoke_namespace_from_user_with_transaction(
					&transaction,
					namespace_id,
					&user.id,
					arg.permission,
				)
				.await?
			},
			NamespaceGrantGrantee::Group(group) => {
				let Some(group) =
					Self::try_get_group_with_transaction(&transaction, &group).await?
				else {
					return Ok(None);
				};
				Self::revoke_namespace_from_group_with_transaction(
					&transaction,
					namespace_id,
					&group.id,
					arg.permission,
				)
				.await?
			},
			NamespaceGrantGrantee::Public => {
				Self::revoke_public_namespace_read_with_transaction(&transaction, namespace_id)
					.await?
			},
		};
		transaction
			.commit()
			.await
			.map_err(|error| tg::error!(!error, "failed to commit the transaction"))?;
		Ok(output)
	}

	pub(crate) async fn grant_namespace_permission_request(
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
		let output = self
			.grant_namespace_permission(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to grant the namespace permission"))?;
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
		Ok(response.body(body).unwrap())
	}

	pub(crate) async fn list_namespace_grants_request(
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
		let Some(output) = self
			.list_namespace_grants(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to list the namespace grants"))?
		else {
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
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
		Ok(response.body(body).unwrap())
	}

	pub(crate) async fn revoke_namespace_permission_request(
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
		if self
			.revoke_namespace_permission(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to revoke the namespace permission"))?
			.is_none()
		{
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
		}
		match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR)) => (),
			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		}
		let response = http::Response::builder().empty().unwrap().boxed_body();
		Ok(response)
	}

	fn namespace_grant_grantee(
		user: Option<String>,
		group: Option<String>,
		public: bool,
	) -> tg::Result<NamespaceGrantGrantee> {
		match (user, group, public) {
			(Some(user), None, false) => Ok(NamespaceGrantGrantee::User(user)),
			(None, Some(group), false) => Ok(NamespaceGrantGrantee::Group(group)),
			(None, None, true) => Ok(NamespaceGrantGrantee::Public),
			_ => Err(tg::error!("expected exactly one grantee")),
		}
	}
}
