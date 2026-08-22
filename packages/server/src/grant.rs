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
};

impl Session {
	pub(crate) async fn create_grant(&self, arg: tg::grant::create::Arg) -> tg::Result<tg::Grant> {
		if !self.server.is_primary_region() {
			return self.create_grant_primary_region(arg).await;
		}
		let resource = self.resolve_resource(&arg.resource.node).await?;
		let permissions = Self::normalize_grant_permissions(&resource, arg.permissions.clone())?;
		let authorization_resource = tg::Referent::with_node_and_tokens(
			tg::Selector::Id(resource.clone()),
			arg.resource.options.tokens.clone(),
		);
		match &resource {
			id if tg::object::Id::try_from(id.clone()).is_ok()
				|| id.kind() == tg::id::Kind::Process =>
			{
				tangram_index::authorize::validate(id, permissions)?;
				if self
					.authorize(authorization_resource.clone(), permissions)
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
					.authorize(authorization_resource.clone(), permission)
					.await?
					.is_none_or(|permissions| !permissions.contains(permission))
				{
					return Err(tg::error!("failed to find the resource"));
				}

				// Creating a grant requires admin permission on the resource.
				let permission = Self::admin_permission_for_resource(&resource)?;
				if self
					.authorize(authorization_resource, permission)
					.await?
					.is_none_or(|permissions| !permissions.contains(permission))
				{
					return Err(tg::error!("unauthorized"));
				}
			},
		}
		let session = self.clone();
		let grant = self
			.server
			.database
			.run(|transaction| {
				let arg = arg.clone();
				let session = session.clone();
				async move {
					session
						.create_grant_local_with_transaction(transaction, arg, permissions)
						.await
				}
				.boxed()
			})
			.await?;
		self.server
			.spawn_publish_database_outbox_notification_task();
		Ok(grant)
	}

	async fn create_grant_primary_region(
		&self,
		arg: tg::grant::create::Arg,
	) -> tg::Result<tg::Grant> {
		let client = self
			.get_primary_region_session()
			.await
			.map_err(|error| tg::error!(!error, "failed to get the primary region session"))?;
		let grant = client.create_grant(arg).await.map_err(|error| {
			tg::error!(!error, "failed to create the grant in the primary region")
		})?;

		Ok(grant)
	}

	async fn create_grant_local_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		mut arg: tg::grant::create::Arg,
		permissions: tg::authorization::permission::Set,
	) -> tg::Result<ControlFlow<tg::Grant, crate::database::Error>> {
		arg.permissions = tg::Either::Left(permissions);
		let mut batch = tangram_index::batch::Arg::default();
		let (grant, inserted) = match self
			.create_grant_with_transaction(transaction, arg, &mut batch)
			.await?
		{
			ControlFlow::Break(output) => output,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		if !inserted {
			return Err(tg::error!("the grant already exists"));
		}
		match self
			.server
			.enqueue_database_outbox_with_transaction(transaction, &batch)
			.await?
		{
			ControlFlow::Break(()) => (),
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}

		Ok(ControlFlow::Break(grant))
	}

	pub(crate) async fn delete_grant(&self, arg: tg::grant::delete::Arg) -> tg::Result<Option<()>> {
		if !self.server.is_primary_region() {
			return self.delete_grant_primary_region(arg).await;
		}
		let resource = self.resolve_resource(&arg.resource.node).await?;
		let permissions = Self::normalize_grant_permissions(&resource, arg.permissions.clone())?;
		let authorization_resource = tg::Referent::with_node_and_tokens(
			tg::Selector::Id(resource.clone()),
			arg.resource.options.tokens.clone(),
		);
		match &resource {
			id if tg::object::Id::try_from(id.clone()).is_ok()
				|| id.kind() == tg::id::Kind::Process =>
			{
				// A grant on an object or process may be revoked only by its creator, which is enforced by the creator scoping in the transaction, so being able to read the resource confers no power to revoke another subject's grant.
			},
			_ => {
				// Revoking a grant on a user, group, organization, or tag requires admin permission on the resource.
				let permission = Self::admin_permission_for_resource(&resource)?;
				match self.authorize(authorization_resource, permission).await? {
					None => return Ok(None),
					Some(permissions) if permissions.contains(permission) => (),
					Some(_) => return Err(tg::error!("unauthorized")),
				}
			},
		}
		let session = self.clone();
		let output = self
			.server
			.database
			.run(|transaction| {
				let arg = arg.clone();
				let session = session.clone();
				async move {
					session
						.delete_grant_local_with_transaction(transaction, arg, permissions)
						.await
				}
				.boxed()
			})
			.await?;
		self.server
			.spawn_publish_database_outbox_notification_task();
		Ok(output)
	}

	async fn delete_grant_primary_region(
		&self,
		arg: tg::grant::delete::Arg,
	) -> tg::Result<Option<()>> {
		let client = self
			.get_primary_region_session()
			.await
			.map_err(|error| tg::error!(!error, "failed to get the primary region session"))?;
		let output = client.delete_grant(arg).await.map_err(|error| {
			tg::error!(!error, "failed to delete the grant in the primary region")
		})?;

		Ok(output)
	}

	async fn delete_grant_local_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		mut arg: tg::grant::delete::Arg,
		permissions: tg::authorization::permission::Set,
	) -> tg::Result<ControlFlow<Option<()>, crate::database::Error>> {
		arg.permissions = tg::Either::Left(permissions);
		let mut batch = tangram_index::batch::Arg::default();
		let output = match self
			.delete_grant_with_transaction(transaction, arg, &mut batch)
			.await?
		{
			ControlFlow::Break(output) => output,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		match self
			.server
			.enqueue_database_outbox_with_transaction(transaction, &batch)
			.await?
		{
			ControlFlow::Break(()) => (),
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}

		Ok(ControlFlow::Break(output))
	}

	pub(crate) async fn create_grant_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		arg: tg::grant::create::Arg,
		batch: &mut tangram_index::batch::Arg,
	) -> tg::Result<ControlFlow<(tg::Grant, bool), crate::database::Error>> {
		let resource =
			match Self::resolve_resource_with_transaction(transaction, &arg.resource.node).await? {
				ControlFlow::Break(resource) => resource,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			}
			.ok_or_else(|| tg::error!("failed to find the resource"))?;
		let permissions = Self::normalize_grant_permissions(&resource, arg.permissions)?;
		tangram_index::authorize::validate(&resource, permissions)?;
		let subject =
			match Self::resolve_subject_with_transaction(transaction, &arg.subject).await? {
				ControlFlow::Break(subject) => subject,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			}
			.ok_or_else(|| tg::error!("failed to find the subject"))?;
		let created_at = self.server.clock.unix_timestamp()?;
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
			permissions: tg::authorization::permission::Set,
		}
		let statement = formatdoc!(
			"
				select created_at, creator, permissions
				from grants
				where resource = {p}1 and subject = {p}2 and creator = {p}3;
			"
		);
		let result = transaction
			.query_optional_into::<Row>(
				statement.into(),
				db::params![
					resource.to_string(),
					subject.to_string(),
					creator_string.clone()
				],
			)
			.await;
		let row = crate::database::retry!(result, "failed to execute the statement");
		let (created_at, output_creator, permissions, changed) = if let Some(row) = row {
			let mut updated_permissions = row.permissions;
			updated_permissions.insert(permissions);
			if updated_permissions == row.permissions {
				return Ok(ControlFlow::Break((
					tg::Grant {
						created_at: row.created_at,
						creator: Some(row.creator),
						permissions: updated_permissions,
						subject,
						resource,
					},
					false,
				)));
			}
			let statement = formatdoc!(
				"
					update grants
					set permissions = {p}3
					where resource = {p}1 and subject = {p}2 and creator = {p}4;
				"
			);
			let result = transaction
				.execute(
					statement.into(),
					db::params![
						resource.to_string(),
						subject.to_string(),
						updated_permissions.to_string(),
						creator_string.clone()
					],
				)
				.await;
			crate::database::retry!(result, "failed to execute the statement");
			(row.created_at, Some(row.creator), updated_permissions, true)
		} else {
			let statement = formatdoc!(
				"
					insert into grants (resource, subject, permissions, created_at, creator)
					values ({p}1, {p}2, {p}3, {p}4, {p}5);
				"
			);
			let result = transaction
				.execute(
					statement.into(),
					db::params![
						resource.to_string(),
						subject.to_string(),
						permissions.to_string(),
						created_at,
						creator_string
					],
				)
				.await;
			crate::database::retry!(result, "failed to execute the statement");
			(created_at, Some(creator.clone()), permissions, true)
		};
		batch.items.push(tangram_index::batch::Item::PutGrant(
			tangram_index::grant::put::Arg {
				created_at,
				creator: output_creator.clone(),
				expires_at: None,
				permissions,
				subject: subject.clone(),
				resource: resource.clone(),
				time_to_touch: None,
			},
		));
		Ok(ControlFlow::Break((
			tg::Grant {
				created_at,
				creator: output_creator,
				permissions,
				subject,
				resource,
			},
			changed,
		)))
	}

	pub(crate) async fn delete_grant_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		arg: tg::grant::delete::Arg,
		batch: &mut tangram_index::batch::Arg,
	) -> tg::Result<ControlFlow<Option<()>, crate::database::Error>> {
		let resource =
			match Self::resolve_resource_with_transaction(transaction, &arg.resource.node).await? {
				ControlFlow::Break(resource) => resource,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			};
		let Some(resource) = resource else {
			return Ok(ControlFlow::Break(None));
		};
		let permissions = Self::normalize_grant_permissions(&resource, arg.permissions)?;
		tangram_index::authorize::validate(&resource, permissions)?;
		let subject =
			match Self::resolve_subject_with_transaction(transaction, &arg.subject).await? {
				ControlFlow::Break(subject) => subject,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			};
		let Some(subject) = subject else {
			return Ok(ControlFlow::Break(None));
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
			permissions: tg::authorization::permission::Set,
		}
		let statement = formatdoc!(
			"
				select permissions
				from grants
				where resource = {p}1 and subject = {p}2 and creator = {p}3;
			"
		);
		let result = transaction
			.query_optional_into::<Row>(
				statement.into(),
				db::params![
					resource.to_string(),
					subject.to_string(),
					creator_string.clone()
				],
			)
			.await;
		let row = crate::database::retry!(result, "failed to execute the statement");
		let Some(row) = row else {
			return Ok(ControlFlow::Break(None));
		};
		let mut remaining = row.permissions;
		remaining.remove(permissions);
		if remaining == row.permissions {
			return Ok(ControlFlow::Break(None));
		}
		if remaining.is_empty() {
			let statement = formatdoc!(
				"
					delete from grants
					where resource = {p}1 and subject = {p}2 and creator = {p}3;
				"
			);
			let result = transaction
				.execute(
					statement.into(),
					db::params![
						resource.to_string(),
						subject.to_string(),
						creator_string.clone()
					],
				)
				.await;
			crate::database::retry!(result, "failed to execute the statement");
		} else {
			let statement = formatdoc!(
				"
					update grants
					set permissions = {p}3
					where resource = {p}1 and subject = {p}2 and creator = {p}4;
				"
			);
			let result = transaction
				.execute(
					statement.into(),
					db::params![
						resource.to_string(),
						subject.to_string(),
						remaining.to_string(),
						creator_string.clone()
					],
				)
				.await;
			crate::database::retry!(result, "failed to execute the statement");
		}
		let mut deleted = row.permissions.empty_like();
		for permission in row.permissions.iter() {
			if permissions.contains(permission) {
				deleted.insert(permission.into());
			}
		}
		if !deleted.is_empty() {
			batch.items.push(tangram_index::batch::Item::DeleteGrant(
				tangram_index::grant::delete::Arg {
					creator: Some(creator),
					expires_at: None,
					permissions: deleted,
					subject,
					resource,
				},
			));
		}
		Ok(ControlFlow::Break(Some(())))
	}

	pub(crate) async fn resolve_resource(
		&self,
		resource: &tg::Selector<tg::Id>,
	) -> tg::Result<tg::Id> {
		let resource = resource.clone();
		let resource = self
			.server
			.database
			.run_with_options(db::ConnectionOptions::default(), |transaction| {
				let resource = resource.clone();
				async move { Self::resolve_resource_with_transaction(transaction, &resource).await }
					.boxed()
			})
			.await?
			.ok_or_else(|| tg::error!("failed to find the resource"))?;

		Ok(resource)
	}

	async fn resolve_resource_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		resource: &tg::Selector<tg::Id>,
	) -> tg::Result<ControlFlow<Option<tg::Id>, crate::database::Error>> {
		match resource {
			tg::Selector::Id(id) => {
				// Objects, processes, and sandboxes do not have specifiers, so their IDs resolve directly.
				if id.kind() == tg::id::Kind::Process
					|| id.kind() == tg::id::Kind::Sandbox
					|| tg::object::Id::try_from(id.clone()).is_ok()
				{
					return Ok(ControlFlow::Break(Some(id.clone())));
				}
				let specifier =
					match Self::try_get_specifier_for_id_with_transaction(transaction, id).await? {
						ControlFlow::Break(specifier) => specifier,
						ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
					};
				let id = specifier.map(|_| id.clone());

				Ok(ControlFlow::Break(id))
			},
			tg::Selector::Specifier(specifier) => {
				Self::try_get_id_for_specifier_with_transaction(transaction, specifier).await
			},
		}
	}

	fn normalize_grant_permissions(
		resource: &tg::Id,
		permissions: tg::Either<tg::authorization::permission::Set, String>,
	) -> tg::Result<tg::authorization::permission::Set> {
		match permissions {
			tg::Either::Left(permissions) => Ok(permissions),
			tg::Either::Right(permissions) => {
				let kind = tg::authorization::ResourceKind::from_id_kind(resource.kind())
					.ok_or_else(|| tg::error!("invalid resource"))?;
				tg::authorization::permission::Set::parse_for_kind(kind, &permissions)
			},
		}
	}

	pub(crate) fn read_permission_for_resource(
		resource: &tg::Id,
	) -> tg::Result<tg::authorization::Permission> {
		match resource.kind() {
			tg::id::Kind::Group => Ok(tg::authorization::Permission::Group(
				tg::authorization::permission::group::Permission::Read,
			)),
			tg::id::Kind::Organization => Ok(tg::authorization::Permission::Organization(
				tg::authorization::permission::organization::Permission::Read,
			)),
			tg::id::Kind::Process => Ok(tg::authorization::Permission::Process(
				tg::authorization::permission::process::Permission::Read,
			)),
			tg::id::Kind::Sandbox => Ok(tg::authorization::Permission::Sandbox(
				tg::authorization::permission::sandbox::Permission::Read,
			)),
			tg::id::Kind::Tag => Ok(tg::authorization::Permission::Tag(
				tg::authorization::permission::tag::Permission::Read,
			)),
			tg::id::Kind::User => Ok(tg::authorization::Permission::User(
				tg::authorization::permission::user::Permission::Read,
			)),
			_ => Err(tg::error!("invalid resource")),
		}
	}

	pub(crate) fn admin_permission_for_resource(
		resource: &tg::Id,
	) -> tg::Result<tg::authorization::Permission> {
		match resource.kind() {
			tg::id::Kind::Group => Ok(tg::authorization::Permission::Group(
				tg::authorization::permission::group::Permission::Admin,
			)),
			tg::id::Kind::Organization => Ok(tg::authorization::Permission::Organization(
				tg::authorization::permission::organization::Permission::Admin,
			)),
			tg::id::Kind::Sandbox => Ok(tg::authorization::Permission::Sandbox(
				tg::authorization::permission::sandbox::Permission::Write,
			)),
			tg::id::Kind::Tag => Ok(tg::authorization::Permission::Tag(
				tg::authorization::permission::tag::Permission::Admin,
			)),
			tg::id::Kind::User => Ok(tg::authorization::Permission::User(
				tg::authorization::permission::user::Permission::Admin,
			)),
			_ => Err(tg::error!("invalid resource")),
		}
	}

	pub(crate) fn write_permission_for_resource(
		resource: &tg::Id,
	) -> tg::Result<tg::authorization::Permission> {
		match resource.kind() {
			tg::id::Kind::Group => Ok(tg::authorization::Permission::Group(
				tg::authorization::permission::group::Permission::Write,
			)),
			tg::id::Kind::Organization => Ok(tg::authorization::Permission::Organization(
				tg::authorization::permission::organization::Permission::Write,
			)),
			tg::id::Kind::Process => Ok(tg::authorization::Permission::Process(
				tg::authorization::permission::process::Permission::Write,
			)),
			tg::id::Kind::Sandbox => Ok(tg::authorization::Permission::Sandbox(
				tg::authorization::permission::sandbox::Permission::Write,
			)),
			tg::id::Kind::Tag => Ok(tg::authorization::Permission::Tag(
				tg::authorization::permission::tag::Permission::Write,
			)),
			tg::id::Kind::User => Ok(tg::authorization::Permission::User(
				tg::authorization::permission::user::Permission::Write,
			)),
			_ => Err(tg::error!("invalid resource")),
		}
	}

	pub(crate) async fn delete_node_grants_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		id: &tg::Id,
		batch: &mut tangram_index::batch::Arg,
	) -> tg::Result<ControlFlow<(), crate::database::Error>> {
		match self
			.delete_node_grants_batch_with_transaction(transaction, std::slice::from_ref(id), batch)
			.await?
		{
			ControlFlow::Break(()) => (),
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}

		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn delete_node_grants_batch_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		ids: &[tg::Id],
		batch: &mut tangram_index::batch::Arg,
	) -> tg::Result<ControlFlow<(), crate::database::Error>> {
		if ids.is_empty() {
			return Ok(ControlFlow::Break(()));
		}

		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			creator: tg::Principal,
			#[tangram_database(as = "db::value::FromStr")]
			permissions: tg::authorization::permission::Set,
			#[tangram_database(as = "db::value::FromStr")]
			subject: tg::authorization::Subject,
			#[tangram_database(as = "db::value::FromStr")]
			resource: tg::Id,
		}
		let p = transaction.p();
		let placeholders = (1..=ids.len())
			.map(|index| format!("{p}{index}"))
			.collect::<Vec<_>>()
			.join(", ");
		let params = ids
			.iter()
			.map(ToString::to_string)
			.map(db::Value::from)
			.collect::<Vec<_>>();
		let statement = formatdoc!(
			"
				select creator, resource, permissions, subject
				from grants
				where creator in ({placeholders})
					or resource in ({placeholders})
					or subject in ({placeholders});
			"
		);
		let result = transaction
			.query_all_into::<Row>(statement.into(), params.clone())
			.await;
		let rows = crate::database::retry!(result, "failed to execute the statement");
		for row in rows {
			batch.items.push(tangram_index::batch::Item::DeleteGrant(
				tangram_index::grant::delete::Arg {
					creator: Some(row.creator),
					expires_at: None,
					permissions: row.permissions,
					subject: row.subject,
					resource: row.resource,
				},
			));
		}
		let statement = formatdoc!(
			"
				delete from grants
				where creator in ({placeholders})
					or resource in ({placeholders})
					or subject in ({placeholders});
			"
		);
		let result = transaction.execute(statement.into(), params).await;
		crate::database::retry!(result, "failed to execute the statement");

		Ok(ControlFlow::Break(()))
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
		match (arg.resource, arg.subject) {
			(Some(resource), None) => self.list_resource_grants_local(resource).await,
			(None, Some(subject)) => self.list_subject_grants_local(subject).await,
			_ => Err(tg::error!(
				"expected exactly one of a resource or a subject"
			)),
		}
	}

	async fn list_resource_grants_local(
		&self,
		resource: tg::Selector<tg::Id>,
	) -> tg::Result<Option<tg::grant::list::Output>> {
		// Listing the grants on an object or a process requires the root principal.
		if let tg::Selector::Id(id) = &resource
			&& (id.kind() == tg::id::Kind::Process || tg::object::Id::try_from(id.clone()).is_ok())
		{
			if !matches!(self.context.principal, tg::Principal::Root) {
				return Err(tg::error!("unauthorized"));
			}
			let data = self.list_resource_grants(id).await?;
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
		let data = self.list_resource_grants(&id).await?;
		Ok(Some(tg::grant::list::Output { data }))
	}

	async fn list_resource_grants(&self, resource: &tg::Id) -> tg::Result<Vec<tg::Grant>> {
		let resource = resource.clone();
		self.server
			.database
			.run_with_options(db::ConnectionOptions::default(), |transaction| {
				let resource = resource.clone();
				async move {
					Self::list_resource_grants_with_transaction(transaction, &resource).await
				}
				.boxed()
			})
			.await
	}

	async fn list_subject_grants_local(
		&self,
		subject: tg::authorization::subject::Selector,
	) -> tg::Result<Option<tg::grant::list::Output>> {
		// Resolve the subject.
		let subject = self.try_resolve_subject(&subject).await?;
		let Some(subject) = subject else {
			return Ok(None);
		};

		// Authorize the subject.
		match &subject {
			// Listing the grants held by a user, group, or organization requires admin permission on it, and it is not found without read permission.
			tg::authorization::Subject::Group(_)
			| tg::authorization::Subject::Organization(_)
			| tg::authorization::Subject::User(_) => {
				let id: tg::Id = match &subject {
					tg::authorization::Subject::Group(id) => id.clone().into(),
					tg::authorization::Subject::Organization(id) => id.clone().into(),
					tg::authorization::Subject::User(id) => id.clone().into(),
					_ => unreachable!(),
				};
				let read = Self::read_permission_for_resource(&id)?;
				if !self
					.authorize(tg::Selector::Id(id.clone()), read)
					.await?
					.is_some_and(|permissions| permissions.contains(read))
				{
					return Ok(None);
				}
				let admin = Self::admin_permission_for_resource(&id)?;
				if !self
					.authorize(tg::Selector::Id(id), admin)
					.await?
					.is_some_and(|permissions| permissions.contains(admin))
				{
					return Err(tg::error!("unauthorized"));
				}
			},
			// Listing the grants held by any other subject requires the root principal.
			tg::authorization::Subject::Process(_)
			| tg::authorization::Subject::Public
			| tg::authorization::Subject::Root
			| tg::authorization::Subject::Runner(_)
			| tg::authorization::Subject::Sandbox(_) => {
				if !matches!(self.context.principal, tg::Principal::Root) {
					return Err(tg::error!("unauthorized"));
				}
			},
		}

		// List the grants.
		let data = self.list_subject_grants(&subject).await?;

		Ok(Some(tg::grant::list::Output { data }))
	}

	async fn try_resolve_subject(
		&self,
		subject: &tg::authorization::subject::Selector,
	) -> tg::Result<Option<tg::authorization::Subject>> {
		let subject = subject.clone();
		self.server
			.database
			.run_with_options(db::ConnectionOptions::default(), |transaction| {
				let subject = subject.clone();
				async move { Self::resolve_subject_with_transaction(transaction, &subject).await }
					.boxed()
			})
			.await
	}

	async fn list_subject_grants(
		&self,
		subject: &tg::authorization::Subject,
	) -> tg::Result<Vec<tg::Grant>> {
		let subject = subject.clone();
		self.server
			.database
			.run_with_options(db::ConnectionOptions::default(), |transaction| {
				let subject = subject.clone();
				async move {
					Self::list_subject_grants_with_transaction(transaction, &subject).await
				}
				.boxed()
			})
			.await
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
	) -> tg::Result<ControlFlow<Vec<tg::Grant>, crate::database::Error>> {
		#[derive(db::row::Deserialize)]
		struct Row {
			created_at: i64,
			#[tangram_database(as = "db::value::FromStr")]
			creator: tg::Principal,
			#[tangram_database(as = "db::value::FromStr")]
			permissions: tg::authorization::permission::Set,
			#[tangram_database(as = "db::value::FromStr")]
			subject: tg::authorization::Subject,
		}
		let p = transaction.p();
		let statement = formatdoc!(
			"
				select created_at, creator, permissions, subject
				from grants
				where resource = {p}1
				order by subject, creator, permissions;
			"
		);
		let result = transaction
			.query_all_into::<Row>(statement.into(), db::params![resource.to_string()])
			.await;
		let rows = crate::database::retry!(result, "failed to execute the statement");
		let grants = rows
			.into_iter()
			.map(|row| tg::Grant {
				created_at: row.created_at,
				creator: Some(row.creator),
				permissions: row.permissions,
				subject: row.subject,
				resource: resource.clone(),
			})
			.collect();

		Ok(ControlFlow::Break(grants))
	}

	async fn list_subject_grants_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		subject: &tg::authorization::Subject,
	) -> tg::Result<ControlFlow<Vec<tg::Grant>, crate::database::Error>> {
		#[derive(db::row::Deserialize)]
		struct Row {
			created_at: i64,
			#[tangram_database(as = "db::value::FromStr")]
			creator: tg::Principal,
			#[tangram_database(as = "db::value::FromStr")]
			permissions: tg::authorization::permission::Set,
			#[tangram_database(as = "db::value::FromStr")]
			resource: tg::Id,
		}
		let p = transaction.p();
		let statement = formatdoc!(
			"
				select created_at, creator, permissions, resource
				from grants
				where subject = {p}1
				order by resource, creator, permissions;
			"
		);
		let result = transaction
			.query_all_into::<Row>(statement.into(), db::params![subject.to_string()])
			.await;
		let rows = crate::database::retry!(result, "failed to execute the statement");
		let grants = rows
			.into_iter()
			.map(|row| tg::Grant {
				created_at: row.created_at,
				creator: Some(row.creator),
				permissions: row.permissions,
				subject: subject.clone(),
				resource: row.resource,
			})
			.collect();

		Ok(ControlFlow::Break(grants))
	}

	pub(crate) async fn resolve_subject_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		subject: &tg::authorization::subject::Selector,
	) -> tg::Result<ControlFlow<Option<tg::authorization::Subject>, crate::database::Error>> {
		let subject = match subject {
			tg::authorization::subject::Selector::Subject(subject) => match subject {
				tg::authorization::Subject::Group(id) => {
					let id = id.clone();
					let specifier = match Self::try_get_specifier_for_id_with_transaction(
						transaction,
						&id.clone().into(),
					)
					.await?
					{
						ControlFlow::Break(specifier) => specifier,
						ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
					};
					if specifier.is_none() {
						return Ok(ControlFlow::Break(None));
					}
					tg::authorization::Subject::Group(id)
				},
				tg::authorization::Subject::Organization(id) => {
					let id = id.clone();
					let specifier = match Self::try_get_specifier_for_id_with_transaction(
						transaction,
						&id.clone().into(),
					)
					.await?
					{
						ControlFlow::Break(specifier) => specifier,
						ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
					};
					if specifier.is_none() {
						return Ok(ControlFlow::Break(None));
					}
					tg::authorization::Subject::Organization(id)
				},
				tg::authorization::Subject::Process(id) => {
					tg::authorization::Subject::Process(id.clone())
				},
				tg::authorization::Subject::Public => tg::authorization::Subject::Public,
				tg::authorization::Subject::Root => tg::authorization::Subject::Root,
				tg::authorization::Subject::Runner(id) => {
					tg::authorization::Subject::Runner(id.clone())
				},
				tg::authorization::Subject::Sandbox(id) => {
					tg::authorization::Subject::Sandbox(id.clone())
				},
				tg::authorization::Subject::User(id) => {
					let id = id.clone();
					let specifier = match Self::try_get_specifier_for_id_with_transaction(
						transaction,
						&id.clone().into(),
					)
					.await?
					{
						ControlFlow::Break(specifier) => specifier,
						ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
					};
					if specifier.is_none() {
						return Ok(ControlFlow::Break(None));
					}
					tg::authorization::Subject::User(id)
				},
			},
			tg::authorization::subject::Selector::Specifier(specifier) => {
				let id =
					match Self::try_get_id_for_specifier_with_transaction(transaction, specifier)
						.await?
					{
						ControlFlow::Break(id) => id,
						ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
					};
				let Some(id) = id else {
					return Ok(ControlFlow::Break(None));
				};
				match id.kind() {
					tg::id::Kind::Group => tg::authorization::Subject::Group(id.try_into()?),
					tg::id::Kind::Organization => {
						tg::authorization::Subject::Organization(id.try_into()?)
					},
					tg::id::Kind::User => tg::authorization::Subject::User(id.try_into()?),
					_ => return Ok(ControlFlow::Break(None)),
				}
			},
		};

		Ok(ControlFlow::Break(Some(subject)))
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
