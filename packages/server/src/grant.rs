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

mod token;

impl Session {
	pub(crate) async fn create_grant(&self, arg: tg::grant::create::Arg) -> tg::Result<tg::Grant> {
		let resource = self.resolve_resource(&arg.resource).await?;
		let permissions = Self::normalize_grant_permissions(&resource, arg.permissions.clone())?;
		match &resource {
			id if tg::object::Id::try_from(id.clone()).is_ok()
				|| id.kind() == tg::id::Kind::Process =>
			{
				tangram_index::authorize::validate(id, permissions)?;
				if self
					.authorize(tg::grant::Resource::Id(resource.clone()), permissions)
					.await?
					.is_none_or(|authorized| !authorized.contains(permissions))
				{
					return Err(tg::error!("failed to find the resource"));
				}
			},
			_ => {
				// The resource is not found without read permission, so creating a grant does not reveal whether a resource the actor cannot see exists.
				let permission = Self::read_permission_for_resource(&resource)?;
				if self
					.authorize(tg::grant::Resource::Id(resource.clone()), permission)
					.await?
					.is_none_or(|permissions| !permissions.contains(permission))
				{
					return Err(tg::error!("failed to find the resource"));
				}

				// Creating a grant requires admin permission on the resource.
				let permission = Self::admin_permission_for_resource(&resource)?;
				if self
					.authorize(tg::grant::Resource::Id(resource.clone()), permission)
					.await?
					.is_none_or(|permissions| !permissions.contains(permission))
				{
					return Err(tg::error!("unauthorized"));
				}
			},
		}
		let session = self.clone();
		let (grant, batch) = self
			.server
			.database
			.run(|transaction| {
				let mut arg = arg.clone();
				let session = session.clone();
				async move {
					arg.permissions = tg::Either::Left(permissions);
					let mut batch = tangram_index::batch::Arg::default();
					let (grant, inserted) = session
						.create_grant_with_transaction(transaction, arg, &mut batch)
						.await?;
					if !inserted {
						return Err(tg::error!("the grant already exists").into());
					}
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
		let resource = self.resolve_resource(&arg.resource).await?;
		let permissions = Self::normalize_grant_permissions(&resource, arg.permissions.clone())?;
		match &resource {
			id if tg::object::Id::try_from(id.clone()).is_ok()
				|| id.kind() == tg::id::Kind::Process =>
			{
				// A grant on an object or process may be revoked only by its creator, which is enforced by the creator scoping in the transaction, so being able to read the resource confers no power to revoke another principal's grant.
			},
			_ => {
				// Revoking a grant on a user, group, organization, or tag requires admin permission on the resource.
				let permission = Self::admin_permission_for_resource(&resource)?;
				match self
					.authorize(tg::grant::Resource::Id(resource.clone()), permission)
					.await?
				{
					None => return Ok(None),
					Some(permissions) if permissions.contains(permission) => (),
					Some(_) => return Err(tg::error!("unauthorized")),
				}
			},
		}
		let session = self.clone();
		let (output, batch) = self
			.server
			.database
			.run(|transaction| {
				let mut arg = arg.clone();
				let session = session.clone();
				async move {
					arg.permissions = tg::Either::Left(permissions);
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
	) -> tg::Result<(tg::Grant, bool)> {
		let resource = Self::resolve_resource_with_transaction(transaction, &arg.resource)
			.await?
			.ok_or_else(|| tg::error!("failed to find the resource"))?;
		let permissions = Self::normalize_grant_permissions(&resource, arg.permissions)?;
		tangram_index::authorize::validate(&resource, permissions)?;
		let principal = Self::resolve_principal_with_transaction(transaction, &arg.principal)
			.await?
			.ok_or_else(|| tg::error!("failed to find the principal"))?;
		let created_at = time::OffsetDateTime::now_utc().unix_timestamp();
		if matches!(self.context.principal, tg::Principal::Anonymous) {
			return Err(tg::error!("unauthorized"));
		}
		let creator = self.context.principal.clone();
		let creator_string = creator.to_string();
		let p = transaction.p();
		#[derive(db::row::Deserialize)]
		struct Row {
			created_at: i64,
			#[tangram_database(as = "db::value::FromStr")]
			creator: tg::Principal,
			#[tangram_database(as = "db::value::FromStr")]
			permissions: tg::grant::permission::Set,
		}
		let statement = formatdoc!(
			"
				select created_at, creator, permissions
				from grants
				where resource = {p}1 and principal = {p}2 and creator = {p}3;
			"
		);
		let row = transaction
			.query_optional_into::<Row>(
				statement.into(),
				db::params![
					resource.to_string(),
					principal.to_string(),
					creator_string.clone()
				],
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let (created_at, output_creator, permissions, changed) = if let Some(row) = row {
			let mut updated_permissions = row.permissions;
			updated_permissions.insert(permissions);
			if updated_permissions == row.permissions {
				return Ok((
					tg::Grant {
						created_at: row.created_at,
						creator: Some(row.creator),
						permissions: updated_permissions,
						principal,
						resource,
					},
					false,
				));
			}
			let statement = formatdoc!(
				"
					update grants
					set permissions = {p}3
					where resource = {p}1 and principal = {p}2 and creator = {p}4;
				"
			);
			transaction
				.execute(
					statement.into(),
					db::params![
						resource.to_string(),
						principal.to_string(),
						updated_permissions.to_string(),
						creator_string.clone()
					],
				)
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
			(row.created_at, Some(row.creator), updated_permissions, true)
		} else {
			let statement = formatdoc!(
				"
					insert into grants (resource, principal, permissions, created_at, creator)
					values ({p}1, {p}2, {p}3, {p}4, {p}5);
				"
			);
			transaction
				.execute(
					statement.into(),
					db::params![
						resource.to_string(),
						principal.to_string(),
						permissions.to_string(),
						created_at,
						creator_string
					],
				)
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
			(created_at, Some(creator.clone()), permissions, true)
		};
		batch.put_grants.push(tangram_index::grant::put::Arg {
			created_at,
			creator: output_creator.clone(),
			expires_at: None,
			permissions,
			principal: principal.clone(),
			resource: resource.clone(),
			time_to_touch: None,
		});
		Ok((
			tg::Grant {
				created_at,
				creator: output_creator,
				permissions,
				principal,
				resource,
			},
			changed,
		))
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
		let permissions = Self::normalize_grant_permissions(&resource, arg.permissions)?;
		tangram_index::authorize::validate(&resource, permissions)?;
		let Some(principal) =
			Self::resolve_principal_with_transaction(transaction, &arg.principal).await?
		else {
			return Ok(None);
		};
		if matches!(self.context.principal, tg::Principal::Anonymous) {
			return Err(tg::error!("unauthorized"));
		}
		let creator = self.context.principal.clone();
		let creator_string = creator.to_string();
		let p = transaction.p();
		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			permissions: tg::grant::permission::Set,
		}
		let statement = formatdoc!(
			"
				select permissions
				from grants
				where resource = {p}1 and principal = {p}2 and creator = {p}3;
			"
		);
		let Some(row) = transaction
			.query_optional_into::<Row>(
				statement.into(),
				db::params![
					resource.to_string(),
					principal.to_string(),
					creator_string.clone()
				],
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
		else {
			return Ok(None);
		};
		let mut remaining = row.permissions;
		remaining.remove(permissions);
		if remaining == row.permissions {
			return Ok(None);
		}
		if remaining.is_empty() {
			let statement = formatdoc!(
				"
					delete from grants
					where resource = {p}1 and principal = {p}2 and creator = {p}3;
				"
			);
			transaction
				.execute(
					statement.into(),
					db::params![
						resource.to_string(),
						principal.to_string(),
						creator_string.clone()
					],
				)
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		} else {
			let statement = formatdoc!(
				"
					update grants
					set permissions = {p}3
					where resource = {p}1 and principal = {p}2 and creator = {p}4;
				"
			);
			transaction
				.execute(
					statement.into(),
					db::params![
						resource.to_string(),
						principal.to_string(),
						remaining.to_string(),
						creator_string.clone()
					],
				)
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		}
		let mut deleted = row.permissions.empty_like();
		for permission in row.permissions.iter() {
			if permissions.contains(permission) {
				deleted.insert(permission.into());
			}
		}
		if !deleted.is_empty() {
			batch.delete_grants.push(tangram_index::grant::delete::Arg {
				creator: Some(creator),
				expires_at: None,
				permissions: deleted,
				principal,
				resource,
			});
		}
		Ok(Some(()))
	}

	pub(crate) async fn resolve_resource(
		&self,
		resource: &tg::grant::Resource,
	) -> tg::Result<tg::Id> {
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
		Self::resolve_resource_with_transaction(&transaction, resource)
			.await?
			.ok_or_else(|| tg::error!("failed to find the resource"))
	}

	fn normalize_grant_permissions(
		resource: &tg::Id,
		permissions: tg::Either<tg::grant::permission::Set, String>,
	) -> tg::Result<tg::grant::permission::Set> {
		match permissions {
			tg::Either::Left(permissions) => Ok(permissions),
			tg::Either::Right(permissions) => {
				let kind = tg::grant::resource::Kind::from_id_kind(resource.kind())
					.ok_or_else(|| tg::error!("invalid resource"))?;
				tg::grant::permission::Set::parse_for_kind(kind, &permissions)
			},
		}
	}

	pub(crate) fn read_permission_for_resource(
		resource: &tg::Id,
	) -> tg::Result<tg::grant::Permission> {
		match resource.kind() {
			tg::id::Kind::Group => Ok(tg::grant::Permission::Group(
				tg::grant::permission::group::Permission::Read,
			)),
			tg::id::Kind::Organization => Ok(tg::grant::Permission::Organization(
				tg::grant::permission::organization::Permission::Read,
			)),
			tg::id::Kind::Sandbox => Ok(tg::grant::Permission::Sandbox(
				tg::grant::permission::sandbox::Permission::Read,
			)),
			tg::id::Kind::Tag => Ok(tg::grant::Permission::Tag(
				tg::grant::permission::tag::Permission::Read,
			)),
			tg::id::Kind::User => Ok(tg::grant::Permission::User(
				tg::grant::permission::user::Permission::Read,
			)),
			_ => Err(tg::error!("invalid resource")),
		}
	}

	pub(crate) fn admin_permission_for_resource(
		resource: &tg::Id,
	) -> tg::Result<tg::grant::Permission> {
		match resource.kind() {
			tg::id::Kind::Group => Ok(tg::grant::Permission::Group(
				tg::grant::permission::group::Permission::Admin,
			)),
			tg::id::Kind::Organization => Ok(tg::grant::Permission::Organization(
				tg::grant::permission::organization::Permission::Admin,
			)),
			tg::id::Kind::Sandbox => Ok(tg::grant::Permission::Sandbox(
				tg::grant::permission::sandbox::Permission::Write,
			)),
			tg::id::Kind::Tag => Ok(tg::grant::Permission::Tag(
				tg::grant::permission::tag::Permission::Admin,
			)),
			tg::id::Kind::User => Ok(tg::grant::Permission::User(
				tg::grant::permission::user::Permission::Admin,
			)),
			_ => Err(tg::error!("invalid resource")),
		}
	}

	pub(crate) fn write_permission_for_resource(
		resource: &tg::Id,
	) -> tg::Result<tg::grant::Permission> {
		match resource.kind() {
			tg::id::Kind::Group => Ok(tg::grant::Permission::Group(
				tg::grant::permission::group::Permission::Write,
			)),
			tg::id::Kind::Organization => Ok(tg::grant::Permission::Organization(
				tg::grant::permission::organization::Permission::Write,
			)),
			tg::id::Kind::Sandbox => Ok(tg::grant::Permission::Sandbox(
				tg::grant::permission::sandbox::Permission::Write,
			)),
			tg::id::Kind::Tag => Ok(tg::grant::Permission::Tag(
				tg::grant::permission::tag::Permission::Write,
			)),
			tg::id::Kind::User => Ok(tg::grant::Permission::User(
				tg::grant::permission::user::Permission::Write,
			)),
			_ => Err(tg::error!("invalid resource")),
		}
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
			creator: tg::Principal,
			#[tangram_database(as = "db::value::FromStr")]
			permissions: tg::grant::permission::Set,
			#[tangram_database(as = "db::value::FromStr")]
			principal: tg::grant::Principal,
			#[tangram_database(as = "db::value::FromStr")]
			resource: tg::Id,
		}
		let p = transaction.p();
		let statement = formatdoc!(
			"
				select creator, resource, permissions, principal
				from grants
				where resource = {p}1;
			"
		);
		let rows = transaction
			.query_all_into::<Row>(statement.into(), db::params![id.to_string()])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		for row in rows {
			batch.delete_grants.push(tangram_index::grant::delete::Arg {
				creator: Some(row.creator),
				expires_at: None,
				permissions: row.permissions,
				principal: row.principal,
				resource: row.resource,
			});
		}
		let statement = formatdoc!(
			"
				delete from grants
				where resource = {p}1;
			"
		);
		transaction
			.execute(statement.into(), db::params![id.to_string()])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let statement = formatdoc!(
			"
				select creator, resource, permissions, principal
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
				creator: Some(row.creator),
				expires_at: None,
				permissions: row.permissions,
				principal: row.principal,
				resource: row.resource,
			});
		}
		let statement = formatdoc!(
			"
				delete from grants
				where principal = {p}1;
			"
		);
		transaction
			.execute(statement.into(), db::params![id.to_string()])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		Ok(())
	}

	pub(crate) async fn list_grants(
		&self,
		arg: tg::grant::list::Arg,
	) -> tg::Result<Option<tg::grant::list::Output>> {
		let location = self
			.server
			.location(arg.location.as_ref())
			.map_err(|error| tg::error!(!error, "failed to resolve the location"))?;
		match location {
			tg::Location::Local(_) => self.list_grants_local(arg).await,
			tg::Location::Remote(remote) => self.list_grants_remote(arg, remote).await,
		}
	}

	async fn list_grants_local(
		&self,
		arg: tg::grant::list::Arg,
	) -> tg::Result<Option<tg::grant::list::Output>> {
		match (arg.resource, arg.principal) {
			(Some(resource), None) => self.list_resource_grants_local(resource).await,
			(None, Some(principal)) => self.list_principal_grants_local(principal).await,
			_ => Err(tg::error!(
				"expected exactly one of a resource or a principal"
			)),
		}
	}

	async fn list_resource_grants_local(
		&self,
		resource: tg::grant::Resource,
	) -> tg::Result<Option<tg::grant::list::Output>> {
		// Listing the grants on an object or a process requires the root principal.
		if let tg::grant::Resource::Id(id) = &resource
			&& (id.kind() == tg::id::Kind::Process || tg::object::Id::try_from(id.clone()).is_ok())
		{
			if !matches!(self.context.principal, tg::Principal::Root) {
				return Err(tg::error!("unauthorized"));
			}
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
			let data = Self::list_resource_grants_with_transaction(&transaction, id).await?;
			return Ok(Some(tg::grant::list::Output { data }));
		}
		// Listing the grants on a node requires admin permission, and the node is not found without read permission.
		let id = self.resolve_resource(&resource).await?;
		let read = Self::read_permission_for_resource(&id)?;
		if !self
			.authorize(resource.clone(), read)
			.await?
			.is_some_and(|permissions| permissions.contains(read))
		{
			return Ok(None);
		}
		let admin = Self::admin_permission_for_resource(&id)?;
		if !self
			.authorize(resource.clone(), admin)
			.await?
			.is_some_and(|permissions| permissions.contains(admin))
		{
			return Err(tg::error!("unauthorized"));
		}
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
		let data = Self::list_resource_grants_with_transaction(&transaction, &id).await?;
		Ok(Some(tg::grant::list::Output { data }))
	}

	async fn list_principal_grants_local(
		&self,
		principal: tg::principal::Selector,
	) -> tg::Result<Option<tg::grant::list::Output>> {
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
		let Some(principal) =
			Self::resolve_principal_with_transaction(&transaction, &principal).await?
		else {
			return Ok(None);
		};
		match &principal {
			// Listing the grants held by a user, group, or organization requires admin permission on it, and it is not found without read permission.
			tg::grant::Principal::Group(_)
			| tg::grant::Principal::Organization(_)
			| tg::grant::Principal::User(_) => {
				let id: tg::Id = match &principal {
					tg::grant::Principal::Group(id) => id.clone().into(),
					tg::grant::Principal::Organization(id) => id.clone().into(),
					tg::grant::Principal::User(id) => id.clone().into(),
					_ => unreachable!(),
				};
				let read = Self::read_permission_for_resource(&id)?;
				if !self
					.authorize(tg::grant::Resource::Id(id.clone()), read)
					.await?
					.is_some_and(|permissions| permissions.contains(read))
				{
					return Ok(None);
				}
				let admin = Self::admin_permission_for_resource(&id)?;
				if !self
					.authorize(tg::grant::Resource::Id(id), admin)
					.await?
					.is_some_and(|permissions| permissions.contains(admin))
				{
					return Err(tg::error!("unauthorized"));
				}
			},
			// Listing the grants held by any other principal requires the root principal.
			tg::grant::Principal::Process(_)
			| tg::grant::Principal::Public
			| tg::grant::Principal::Root
			| tg::grant::Principal::Runner
			| tg::grant::Principal::Sandbox(_) => {
				if !matches!(self.context.principal, tg::Principal::Root) {
					return Err(tg::error!("unauthorized"));
				}
			},
		}
		let data = Self::list_principal_grants_with_transaction(&transaction, &principal).await?;
		Ok(Some(tg::grant::list::Output { data }))
	}

	async fn list_grants_remote(
		&self,
		mut arg: tg::grant::list::Arg,
		remote: tg::location::Remote,
	) -> tg::Result<Option<tg::grant::list::Output>> {
		let client = self.get_remote_session(&remote.name).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
		)?;
		arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
		client
			.list_grants(arg)
			.await
			.map_err(|error| tg::error!(!error, remote = %remote.name, "failed to list the grants"))
	}

	pub(crate) async fn list_grants_request(
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
			.unwrap_or_default();
		let Some(output) = self.list_grants(arg).await? else {
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
		let response = response.body(body).unwrap().boxed_body();
		Ok(response)
	}

	pub(crate) async fn list_resource_grants_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		resource: &tg::Id,
	) -> tg::Result<Vec<tg::Grant>> {
		#[derive(db::row::Deserialize)]
		struct Row {
			created_at: i64,
			#[tangram_database(as = "db::value::FromStr")]
			creator: tg::Principal,
			#[tangram_database(as = "db::value::FromStr")]
			permissions: tg::grant::permission::Set,
			#[tangram_database(as = "db::value::FromStr")]
			principal: tg::grant::Principal,
		}
		let p = transaction.p();
		let statement = formatdoc!(
			"
				select created_at, creator, permissions, principal
				from grants
				where resource = {p}1
				order by principal, creator, permissions;
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
				creator: Some(row.creator),
				permissions: row.permissions,
				principal: row.principal,
				resource: resource.clone(),
			})
			.collect())
	}

	async fn list_principal_grants_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		principal: &tg::grant::Principal,
	) -> tg::Result<Vec<tg::Grant>> {
		#[derive(db::row::Deserialize)]
		struct Row {
			created_at: i64,
			#[tangram_database(as = "db::value::FromStr")]
			creator: tg::Principal,
			#[tangram_database(as = "db::value::FromStr")]
			permissions: tg::grant::permission::Set,
			#[tangram_database(as = "db::value::FromStr")]
			resource: tg::Id,
		}
		let p = transaction.p();
		let statement = formatdoc!(
			"
				select created_at, creator, permissions, resource
				from grants
				where principal = {p}1
				order by resource, creator, permissions;
			"
		);
		let rows = transaction
			.query_all_into::<Row>(statement.into(), db::params![principal.to_string()])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		Ok(rows
			.into_iter()
			.map(|row| tg::Grant {
				created_at: row.created_at,
				creator: Some(row.creator),
				permissions: row.permissions,
				principal: principal.clone(),
				resource: row.resource,
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
