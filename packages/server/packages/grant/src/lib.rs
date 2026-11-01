use {
	futures::{FutureExt as _, future::BoxFuture},
	indoc::formatdoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
	tangram_server_core::Session,
};

/// The component.
#[derive(Clone, Default)]
pub struct Component {}

#[derive(Clone, serde::Deserialize, serde::Serialize)]
#[serde(tag = "version")]
enum Cursor {
	V0 { after: tg::grant::Id },
}

impl Component {
	#[must_use]
	pub fn new() -> Self {
		Self::default()
	}

	pub(crate) async fn list_resource_grants_with_transaction(
		transaction: &tangram_server_core::database::Transaction<'_>,
		resource: &tg::Id,
		after: Option<&tg::grant::Id>,
		limit: Option<u64>,
	) -> tg::Result<ControlFlow<Vec<tg::grant::Data>, tangram_server_core::database::Error>> {
		#[derive(db::row::Deserialize)]
		struct Row {
			created_at: i64,

			#[tangram_database(as = "Option<db::value::FromStr>")]
			creator: Option<tg::Principal>,

			#[tangram_database(as = "db::value::FromStr")]
			id: tg::grant::Id,

			#[tangram_database(as = "db::value::FromStr")]
			permissions: tg::authorization::permission::Set,

			#[tangram_database(as = "db::value::FromStr")]
			subject: tg::authorization::Subject,
		}
		let p = transaction.p();

		// The empty string sorts before every grant ID for the first page.
		let after = after.map(ToString::to_string).unwrap_or_default();
		let limit = limit.map_or(i64::MAX, |limit| i64::try_from(limit).unwrap());
		let statement = formatdoc!(
			"
				select created_at, creator, id, permissions, subject
				from grants
				where resource = {p}1 and id > {p}2
				order by id
				limit {p}3;
			"
		);
		let result = transaction
			.query_all_into::<Row>(
				statement.into(),
				db::params![resource.to_string(), after, limit],
			)
			.await;
		let rows = tangram_server_core::database::retry!(result, "failed to execute the statement");
		let grants = rows
			.into_iter()
			.map(|row| tg::grant::Data {
				created_at: row.created_at,
				creator: row.creator.unwrap_or_default(),
				id: row.id,
				permissions: row.permissions,
				resource: resource.clone(),
				subject: row.subject,
			})
			.collect();

		Ok(ControlFlow::Break(grants))
	}

	pub(crate) async fn resolve_subject_with_transaction(
		transaction: &tangram_server_core::database::Transaction<'_>,
		subject: &tg::authorization::subject::Selector,
	) -> tg::Result<
		ControlFlow<Option<tg::authorization::Subject>, tangram_server_core::database::Error>,
	> {
		let subject = match subject {
			tg::authorization::subject::Selector::Subject(subject) => match subject {
				tg::authorization::Subject::Group(id) => {
					let id = id.clone();
					let specifier = match tangram_server_node_common::specifier::try_get_specifier_for_id_with_transaction(
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
					let specifier = match tangram_server_node_common::specifier::try_get_specifier_for_id_with_transaction(
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
				tg::authorization::Subject::Sync(id) => {
					tg::authorization::Subject::Sync(id.clone())
				},
				tg::authorization::Subject::Tag(id) => tg::authorization::Subject::Tag(id.clone()),
				tg::authorization::Subject::User(id) => {
					let id = id.clone();
					let specifier = match tangram_server_node_common::specifier::try_get_specifier_for_id_with_transaction(
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
					match tangram_server_node_common::specifier::try_get_id_for_specifier_with_transaction(
						transaction,
						specifier,
					)
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
}

impl Component {
	pub(crate) async fn grant(
		&self,
		session: &Session,
		arg: tg::grant::grant::Arg,
	) -> tg::Result<tg::grant::grant::Output> {
		if !session.server.is_primary_region() {
			return self.grant_primary_region(session, arg).await;
		}
		let resource = self.resolve_resource(session, &arg.resource.node).await?;
		let permissions = Self::normalize_grant_permissions(&resource, arg.permissions.clone())?;
		let authorization_resource = tg::Referent::with_node_and_tokens(
			tg::Selector::Id(resource.clone()),
			arg.resource.options.tokens.clone(),
		);
		match &resource {
			id if tg::object::Id::try_from(id.clone()).is_ok()
				|| matches!(
					id.kind(),
					tg::id::Kind::Process | tg::id::Kind::Sandbox | tg::id::Kind::Sync
				) =>
			{
				tangram_index::verify::validate(id, permissions)?;
				if !session
					.authorize(authorization_resource.clone(), permissions)
					.await?
					.check_exhaustion()?
					.permissions
					.contains(permissions)
				{
					return Err(tg::error!("failed to find the resource"));
				}
			},
			_ => {
				// The resource is not found without read permission, so creating a grant does not reveal whether a resource the actor cannot see exists.
				let permission =
					tangram_server_grant_common::read_permission_for_resource(&resource)?;
				if !session
					.authorize(authorization_resource.clone(), permission)
					.await?
					.check_exhaustion()?
					.permissions
					.contains(permission)
				{
					return Err(tg::error!("failed to find the resource"));
				}

				// Creating a grant requires admin permission on the resource.
				let permission =
					tangram_server_grant_common::admin_permission_for_resource(&resource)?;
				if !session
					.authorize(authorization_resource, permission)
					.await?
					.check_exhaustion()?
					.permissions
					.contains(permission)
				{
					return Err(tg::error!("unauthorized"));
				}
			},
		}
		let component = self.clone();
		let session = session.clone();
		let grant = session
			.server
			.database
			.run(|transaction| {
				let arg = arg.clone();
				let component = component.clone();
				let session = session.clone();
				async move {
					component
						.grant_local_with_transaction(&session, transaction, arg, permissions)
						.await
				}
				.boxed()
			})
			.await?;
		session
			.server
			.components
			.indexer
			.spawn_publish_database_index_queue_notification_task(&session.server);
		Ok(tg::grant::grant::Output { data: grant })
	}

	async fn grant_primary_region(
		&self,
		session: &Session,
		arg: tg::grant::grant::Arg,
	) -> tg::Result<tg::grant::grant::Output> {
		let client = session
			.get_primary_region_session()
			.await
			.map_err(|error| tg::error!(!error, "failed to get the primary region session"))?;
		let grant = client.grant(arg).await.map_err(|error| {
			tg::error!(!error, "failed to create the grant in the primary region")
		})?;

		Ok(grant)
	}

	async fn grant_local_with_transaction(
		&self,
		session: &Session,
		transaction: &tangram_server_core::database::Transaction<'_>,
		mut arg: tg::grant::grant::Arg,
		permissions: tg::authorization::permission::Set,
	) -> tg::Result<ControlFlow<tg::grant::Data, tangram_server_core::database::Error>> {
		arg.permissions = tg::Either::Left(permissions);
		let mut batch = tangram_index::batch::Arg::default();
		let (grant, inserted) = match self
			.grant_with_transaction(session, transaction, arg, &mut batch)
			.await?
		{
			ControlFlow::Break(output) => output,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		if !inserted {
			return Err(tg::error!("the grant already exists"));
		}
		match session
			.server
			.components
			.indexer
			.enqueue_database_index_queue_with_transaction(&session.server, transaction, &batch)
			.await?
		{
			ControlFlow::Break(()) => (),
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}

		Ok(ControlFlow::Break(grant))
	}

	pub(crate) async fn revoke(
		&self,
		session: &Session,
		arg: tg::grant::revoke::Arg,
	) -> tg::Result<()> {
		if !session.server.is_primary_region() {
			return self.revoke_primary_region(session, arg).await;
		}
		let resource = self.resolve_resource(session, &arg.resource.node).await?;
		let permissions = Self::normalize_grant_permissions(&resource, arg.permissions.clone())?;
		let authorization_resource = tg::Referent::with_node_and_tokens(
			tg::Selector::Id(resource.clone()),
			arg.resource.options.tokens.clone(),
		);
		match &resource {
			id if tg::object::Id::try_from(id.clone()).is_ok()
				|| matches!(
					id.kind(),
					tg::id::Kind::Process | tg::id::Kind::Sandbox | tg::id::Kind::Sync
				) =>
			{
				// A grant on an object, process, sandbox, or sync may be revoked only by its creator, which is enforced by the creator scoping in the transaction, so being able to read the resource confers no power to revoke another subject's grant.
			},
			_ => {
				// Revoking a grant on a user, group, organization, or tag requires admin permission on the resource.
				let permission =
					tangram_server_grant_common::admin_permission_for_resource(&resource)?;
				session
					.authorize(authorization_resource, permission)
					.await?
					.into_result()?;
			},
		}
		let component = self.clone();
		let session = session.clone();
		let output = session
			.server
			.database
			.run(|transaction| {
				let arg = arg.clone();
				let component = component.clone();
				let session = session.clone();
				async move {
					component
						.revoke_local_with_transaction(&session, transaction, arg, permissions)
						.await
				}
				.boxed()
			})
			.await?;
		session
			.server
			.components
			.indexer
			.spawn_publish_database_index_queue_notification_task(&session.server);
		output.ok_or_else(|| tg::error!(missing, "failed to find the grant"))?;

		Ok(())
	}

	async fn revoke_primary_region(
		&self,
		session: &Session,
		arg: tg::grant::revoke::Arg,
	) -> tg::Result<()> {
		let client = session
			.get_primary_region_session()
			.await
			.map_err(|error| tg::error!(!error, "failed to get the primary region session"))?;
		client.revoke(arg).await.map_err(|error| {
			tg::error!(!error, "failed to revoke the grant in the primary region")
		})?;

		Ok(())
	}

	async fn revoke_local_with_transaction(
		&self,
		session: &Session,
		transaction: &tangram_server_core::database::Transaction<'_>,
		mut arg: tg::grant::revoke::Arg,
		permissions: tg::authorization::permission::Set,
	) -> tg::Result<ControlFlow<Option<()>, tangram_server_core::database::Error>> {
		arg.permissions = tg::Either::Left(permissions);
		let mut batch = tangram_index::batch::Arg::default();
		let output = match self
			.revoke_with_transaction(session, transaction, arg, &mut batch)
			.await?
		{
			ControlFlow::Break(output) => output,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		match session
			.server
			.components
			.indexer
			.enqueue_database_index_queue_with_transaction(&session.server, transaction, &batch)
			.await?
		{
			ControlFlow::Break(()) => (),
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}

		Ok(ControlFlow::Break(output))
	}

	pub async fn grant_with_transaction(
		&self,
		session: &Session,
		transaction: &tangram_server_core::database::Transaction<'_>,
		arg: tg::grant::grant::Arg,
		batch: &mut tangram_index::batch::Arg,
	) -> tg::Result<ControlFlow<(tg::grant::Data, bool), tangram_server_core::database::Error>> {
		let resource =
			match Self::resolve_resource_with_transaction(transaction, &arg.resource.node).await? {
				ControlFlow::Break(resource) => resource,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			}
			.ok_or_else(|| tg::error!("failed to find the resource"))?;
		let permissions = Self::normalize_grant_permissions(&resource, arg.permissions)?;
		tangram_index::verify::validate(&resource, permissions)?;
		let subject =
			match Self::resolve_subject_with_transaction(transaction, &arg.subject).await? {
				ControlFlow::Break(subject) => subject,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			}
			.ok_or_else(|| tg::error!("failed to find the subject"))?;
		if matches!(subject, tg::authorization::Subject::Tag(_)) {
			return Err(tg::error!("a tag cannot be a grant subject"));
		}
		let created_at = session.server.clock.unix_timestamp();
		if matches!(session.context.principal, tg::Principal::Anonymous) {
			return Err(tg::error!("unauthorized"));
		}

		// Store the root creator as null, and compare creators with the null creator as the empty string.
		let creator = session.context.principal.clone();
		let creator_value = (!creator.is_root()).then(|| creator.to_string());
		let creator_key = creator_key(&creator);

		// Get the existing grant.
		let p = transaction.p();
		#[derive(db::row::Deserialize)]
		struct Row {
			created_at: i64,

			#[tangram_database(as = "db::value::FromStr")]
			id: tg::grant::Id,

			#[tangram_database(as = "db::value::FromStr")]
			permissions: tg::authorization::permission::Set,
		}
		let statement = formatdoc!(
			"
				select created_at, id, permissions
				from grants
				where resource = {p}1 and subject = {p}2 and coalesce(creator, '') = {p}3;
			"
		);
		let result = transaction
			.query_optional_into::<Row>(
				statement.into(),
				db::params![
					resource.to_string(),
					subject.to_string(),
					creator_key.clone()
				],
			)
			.await;
		let row = tangram_server_core::database::retry!(result, "failed to execute the statement");

		// Update the existing grant, keeping its ID, or insert a new grant.
		let (created_at, id, permissions, changed) = if let Some(row) = row {
			let mut updated_permissions = row.permissions;
			updated_permissions.insert(permissions);
			if updated_permissions == row.permissions {
				let data = tg::grant::Data {
					created_at: row.created_at,
					creator,
					id: row.id,
					permissions: updated_permissions,
					resource,
					subject,
				};
				return Ok(ControlFlow::Break((data, false)));
			}
			let statement = formatdoc!(
				"
					update grants
					set permissions = {p}2
					where id = {p}1;
				"
			);
			let result = transaction
				.execute(
					statement.into(),
					db::params![row.id.to_string(), updated_permissions.to_string()],
				)
				.await;
			tangram_server_core::database::retry!(result, "failed to execute the statement");
			(row.created_at, row.id, updated_permissions, true)
		} else {
			let id = tg::grant::Id::new();
			let statement = formatdoc!(
				"
					insert into grants (id, resource, subject, permissions, created_at, creator)
					values ({p}1, {p}2, {p}3, {p}4, {p}5, {p}6);
				"
			);
			let result = transaction
				.execute(
					statement.into(),
					db::params![
						id.to_string(),
						resource.to_string(),
						subject.to_string(),
						permissions.to_string(),
						created_at,
						creator_value
					],
				)
				.await;
			tangram_server_core::database::retry!(result, "failed to execute the statement");
			(created_at, id, permissions, true)
		};

		// Put the permission in the index.
		batch.items.push(tangram_index::batch::Item::PutPermission(
			tangram_index::permission::put::Arg {
				created_at,
				creator: Some(creator.clone()),
				permissions,
				resource: resource.clone(),
				source: tangram_index::permission::Source::Grant,
				subject: subject.clone(),
				time_to_touch: None,
				version: None,
			},
		));
		let data = tg::grant::Data {
			created_at,
			creator,
			id,
			permissions,
			resource,
			subject,
		};

		Ok(ControlFlow::Break((data, changed)))
	}

	pub(crate) async fn revoke_with_transaction(
		&self,
		session: &Session,
		transaction: &tangram_server_core::database::Transaction<'_>,
		arg: tg::grant::revoke::Arg,
		batch: &mut tangram_index::batch::Arg,
	) -> tg::Result<ControlFlow<Option<()>, tangram_server_core::database::Error>> {
		let resource =
			match Self::resolve_resource_with_transaction(transaction, &arg.resource.node).await? {
				ControlFlow::Break(resource) => resource,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			};
		let Some(resource) = resource else {
			return Ok(ControlFlow::Break(None));
		};
		let permissions = Self::normalize_grant_permissions(&resource, arg.permissions)?;
		tangram_index::verify::validate(&resource, permissions)?;
		let subject =
			match Self::resolve_subject_with_transaction(transaction, &arg.subject).await? {
				ControlFlow::Break(subject) => subject,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			};
		let Some(subject) = subject else {
			return Ok(ControlFlow::Break(None));
		};
		if matches!(session.context.principal, tg::Principal::Anonymous) {
			return Err(tg::error!("unauthorized"));
		}
		let creator = session.context.principal.clone();
		let creator_key = creator_key(&creator);
		let p = transaction.p();
		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			id: tg::grant::Id,

			#[tangram_database(as = "db::value::FromStr")]
			permissions: tg::authorization::permission::Set,
		}
		let statement = formatdoc!(
			"
				select id, permissions
				from grants
				where resource = {p}1 and subject = {p}2 and coalesce(creator, '') = {p}3;
			"
		);
		let result = transaction
			.query_optional_into::<Row>(
				statement.into(),
				db::params![resource.to_string(), subject.to_string(), creator_key],
			)
			.await;
		let row = tangram_server_core::database::retry!(result, "failed to execute the statement");
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
					where id = {p}1;
				"
			);
			let result = transaction
				.execute(statement.into(), db::params![row.id.to_string()])
				.await;
			tangram_server_core::database::retry!(result, "failed to execute the statement");
		} else {
			let statement = formatdoc!(
				"
					update grants
					set permissions = {p}2
					where id = {p}1;
				"
			);
			let result = transaction
				.execute(
					statement.into(),
					db::params![row.id.to_string(), remaining.to_string()],
				)
				.await;
			tangram_server_core::database::retry!(result, "failed to execute the statement");
		}
		let mut deleted = row.permissions.empty_like();
		for permission in row.permissions.iter() {
			if permissions.contains(permission) {
				deleted.insert(permission.into());
			}
		}
		if !deleted.is_empty() {
			batch
				.items
				.push(tangram_index::batch::Item::DeletePermission(
					tangram_index::permission::delete::Arg {
						creator: Some(creator),
						permissions: deleted,
						resource,
						source: tangram_index::permission::Source::Grant,
						subject,
					},
				));
		}
		Ok(ControlFlow::Break(Some(())))
	}

	pub(crate) async fn try_get_grant(
		&self,
		session: &Session,
		grant: &tg::grant::Id,
		mut arg: tg::grant::get::Arg,
	) -> tg::Result<Option<tg::grant::get::Output>> {
		let location = session
			.server
			.location(arg.location.as_ref())
			.map_err(|error| tg::error!(!error, "failed to resolve the location"))?;
		match location {
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) if Some(region.as_str()) != session.server.config.region.as_deref() => {
				let client = session.get_region_session(&region).await.map_err(
					|error| tg::error!(!error, region = %region, "failed to get the region client"),
				)?;
				arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
				client.try_get_grant(grant, arg).await
			},
			tg::Location::Local(_) => self.try_get_grant_local(session, grant).await,
			tg::Location::Remote(remote) => {
				let client = session
					.server
					.components
					.remote
					.get_remote_session(session, &remote.name)
					.await
					.map_err(
						|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
					)?;
				arg.location = Some(
					tg::Location::Local(tg::location::Local {
						region: remote.region.clone(),
					})
					.into(),
				);
				let location = tg::Location::Remote(remote);
				let output = client.try_get_grant(grant, arg).await?.map(|mut output| {
					output.location = Some(location);
					output
				});
				Ok(output)
			},
		}
	}

	async fn try_get_grant_local(
		&self,
		session: &Session,
		grant: &tg::grant::Id,
	) -> tg::Result<Option<tg::grant::get::Output>> {
		// Get the grant.
		let Some(data) = self.try_get_grant_data(session, grant).await? else {
			return Ok(None);
		};

		// Return the grant only to a session that may list it, or to its creator.
		let authorized = data.creator == session.context.principal
			|| self.is_grant_listable(session, &data).await?;
		if !authorized {
			return Ok(None);
		}

		// Create the output.
		let location = tg::Location::Local(tg::location::Local {
			region: session.server.config.region.clone(),
		});
		let output = tg::grant::get::Output {
			data,
			location: Some(location),
		};

		Ok(Some(output))
	}

	async fn is_grant_listable(
		&self,
		session: &Session,
		data: &tg::grant::Data,
	) -> tg::Result<bool> {
		// The root principal may list every grant.
		if matches!(session.context.principal, tg::Principal::Root) {
			return Ok(true);
		}

		// An admin of a user, group, organization, or tag resource may list the grants on it.
		let node = !(tg::object::Id::try_from(data.resource.clone()).is_ok()
			|| matches!(
				data.resource.kind(),
				tg::id::Kind::Process | tg::id::Kind::Sandbox | tg::id::Kind::Sync
			));
		if node {
			let permission =
				tangram_server_grant_common::admin_permission_for_resource(&data.resource)?;
			let authorized = session
				.authorize(tg::Selector::Id(data.resource.clone()), permission)
				.await?
				.check_exhaustion()?
				.permissions
				.contains(permission);
			if authorized {
				return Ok(true);
			}
		}

		// An admin of a user, group, or organization subject may list the grants it holds.
		let subject: tg::Id = match &data.subject {
			tg::authorization::Subject::Group(id) => id.clone().into(),
			tg::authorization::Subject::Organization(id) => id.clone().into(),
			tg::authorization::Subject::User(id) => id.clone().into(),
			tg::authorization::Subject::Process(_)
			| tg::authorization::Subject::Public
			| tg::authorization::Subject::Root
			| tg::authorization::Subject::Runner(_)
			| tg::authorization::Subject::Sandbox(_)
			| tg::authorization::Subject::Sync(_)
			| tg::authorization::Subject::Tag(_) => return Ok(false),
		};
		let permission = tangram_server_grant_common::admin_permission_for_resource(&subject)?;
		let authorized = session
			.authorize(tg::Selector::Id(subject), permission)
			.await?
			.check_exhaustion()?
			.permissions
			.contains(permission);

		Ok(authorized)
	}

	async fn try_get_grant_data(
		&self,
		session: &Session,
		grant: &tg::grant::Id,
	) -> tg::Result<Option<tg::grant::Data>> {
		let grant = grant.clone();
		session
			.server
			.database
			.run_with_options(db::ConnectionOptions::default(), |transaction| {
				let grant = grant.clone();
				async move { Self::try_get_grant_data_with_transaction(transaction, &grant).await }
					.boxed()
			})
			.await
	}

	async fn try_get_grant_data_with_transaction(
		transaction: &tangram_server_core::database::Transaction<'_>,
		grant: &tg::grant::Id,
	) -> tg::Result<ControlFlow<Option<tg::grant::Data>, tangram_server_core::database::Error>> {
		#[derive(db::row::Deserialize)]
		struct Row {
			created_at: i64,

			#[tangram_database(as = "Option<db::value::FromStr>")]
			creator: Option<tg::Principal>,

			#[tangram_database(as = "db::value::FromStr")]
			permissions: tg::authorization::permission::Set,

			#[tangram_database(as = "db::value::FromStr")]
			resource: tg::Id,

			#[tangram_database(as = "db::value::FromStr")]
			subject: tg::authorization::Subject,
		}
		let p = transaction.p();
		let statement = formatdoc!(
			"
				select created_at, creator, permissions, resource, subject
				from grants
				where id = {p}1;
			"
		);
		let result = transaction
			.query_optional_into::<Row>(statement.into(), db::params![grant.to_string()])
			.await;
		let row = tangram_server_core::database::retry!(result, "failed to execute the statement");
		let data = row.map(|row| tg::grant::Data {
			created_at: row.created_at,
			creator: row.creator.unwrap_or_default(),
			id: grant.clone(),
			permissions: row.permissions,
			resource: row.resource,
			subject: row.subject,
		});

		Ok(ControlFlow::Break(data))
	}

	pub(crate) async fn try_delete_grant(
		&self,
		session: &Session,
		grant: &tg::grant::Id,
		mut arg: tg::grant::delete::Arg,
	) -> tg::Result<Option<()>> {
		let location = session
			.server
			.location(arg.location.as_ref())
			.map_err(|error| tg::error!(!error, "failed to resolve the location"))?;
		match location {
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) if Some(region.as_str()) != session.server.config.region.as_deref() => {
				let client = session.get_region_session(&region).await.map_err(
					|error| tg::error!(!error, region = %region, "failed to get the region client"),
				)?;
				arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
				client.try_delete_grant(grant, arg).await
			},
			tg::Location::Local(_) => {
				arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
				self.try_delete_grant_local(session, grant, arg).await
			},
			tg::Location::Remote(remote) => {
				let client = session
					.server
					.components
					.remote
					.get_remote_session(session, &remote.name)
					.await
					.map_err(
						|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
					)?;
				arg.location = Some(
					tg::Location::Local(tg::location::Local {
						region: remote.region,
					})
					.into(),
				);
				client.try_delete_grant(grant, arg).await
			},
		}
	}

	async fn try_delete_grant_local(
		&self,
		session: &Session,
		grant: &tg::grant::Id,
		arg: tg::grant::delete::Arg,
	) -> tg::Result<Option<()>> {
		if !session.server.is_primary_region() {
			return self
				.try_delete_grant_primary_region(session, grant, arg)
				.await;
		}

		// Delete the grant, which only its creator may delete.
		if matches!(session.context.principal, tg::Principal::Anonymous) {
			return Err(tg::error!("unauthorized"));
		}
		let creator = session.context.principal.clone();
		let output = session
			.server
			.database
			.run(|transaction| {
				let creator = creator.clone();
				let grant = grant.clone();
				let server = session.server.clone();
				async move {
					Self::try_delete_grant_with_transaction(&server, transaction, &grant, &creator)
						.await
				}
				.boxed()
			})
			.await?;
		session
			.server
			.components
			.indexer
			.spawn_publish_database_index_queue_notification_task(&session.server);

		Ok(output)
	}

	async fn try_delete_grant_with_transaction(
		server: &tangram_server_core::Server,
		transaction: &tangram_server_core::database::Transaction<'_>,
		grant: &tg::grant::Id,
		creator: &tg::Principal,
	) -> tg::Result<ControlFlow<Option<()>, tangram_server_core::database::Error>> {
		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			permissions: tg::authorization::permission::Set,

			#[tangram_database(as = "db::value::FromStr")]
			resource: tg::Id,

			#[tangram_database(as = "db::value::FromStr")]
			subject: tg::authorization::Subject,
		}

		// Delete the grant.
		let p = transaction.p();
		let statement = formatdoc!(
			"
				delete from grants
				where id = {p}1 and coalesce(creator, '') = {p}2
				returning permissions, resource, subject;
			"
		);
		let result = transaction
			.query_optional_into::<Row>(
				statement.into(),
				db::params![grant.to_string(), creator_key(creator)],
			)
			.await;
		let row = tangram_server_core::database::retry!(result, "failed to execute the statement");
		let Some(row) = row else {
			return Ok(ControlFlow::Break(None));
		};

		// Delete the permissions from the index.
		let mut batch = tangram_index::batch::Arg::default();
		batch
			.items
			.push(tangram_index::batch::Item::DeletePermission(
				tangram_index::permission::delete::Arg {
					creator: Some(creator.clone()),
					permissions: row.permissions,
					resource: row.resource,
					source: tangram_index::permission::Source::Grant,
					subject: row.subject,
				},
			));
		match server
			.components
			.indexer
			.enqueue_database_index_queue_with_transaction(server, transaction, &batch)
			.await?
		{
			ControlFlow::Break(()) => (),
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}

		Ok(ControlFlow::Break(Some(())))
	}

	async fn try_delete_grant_primary_region(
		&self,
		session: &Session,
		grant: &tg::grant::Id,
		arg: tg::grant::delete::Arg,
	) -> tg::Result<Option<()>> {
		let client = session
			.get_primary_region_session()
			.await
			.map_err(|error| tg::error!(!error, "failed to get the primary region session"))?;
		let output = client.try_delete_grant(grant, arg).await.map_err(|error| {
			tg::error!(!error, "failed to delete the grant in the primary region")
		})?;

		Ok(output)
	}

	pub(crate) async fn resolve_resource(
		&self,
		session: &Session,
		resource: &tg::Selector<tg::Id>,
	) -> tg::Result<tg::Id> {
		let resource = resource.clone();
		let resource = session
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
		transaction: &tangram_server_core::database::Transaction<'_>,
		resource: &tg::Selector<tg::Id>,
	) -> tg::Result<ControlFlow<Option<tg::Id>, tangram_server_core::database::Error>> {
		match resource {
			tg::Selector::Id(id) => {
				// Objects, processes, sandboxes, and syncs do not have specifiers, so their IDs resolve directly.
				if id.kind() == tg::id::Kind::Process
					|| id.kind() == tg::id::Kind::Sandbox
					|| id.kind() == tg::id::Kind::Sync
					|| tg::object::Id::try_from(id.clone()).is_ok()
				{
					return Ok(ControlFlow::Break(Some(id.clone())));
				}
				let specifier =
					match tangram_server_node_common::specifier::try_get_specifier_for_id_with_transaction(
						transaction,
						id,
					)
					.await?
					{
						ControlFlow::Break(specifier) => specifier,
						ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
					};
				let id = specifier.map(|_| id.clone());

				Ok(ControlFlow::Break(id))
			},
			tg::Selector::Specifier(specifier) => {
				tangram_server_node_common::specifier::try_get_id_for_specifier_with_transaction(
					transaction,
					specifier,
				)
				.await
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

	pub async fn delete_node_grants_with_transaction(
		&self,
		transaction: &tangram_server_core::database::Transaction<'_>,
		id: &tg::Id,
		batch: &mut tangram_index::batch::Arg,
	) -> tg::Result<ControlFlow<(), tangram_server_core::database::Error>> {
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
		transaction: &tangram_server_core::database::Transaction<'_>,
		ids: &[tg::Id],
		batch: &mut tangram_index::batch::Arg,
	) -> tg::Result<ControlFlow<(), tangram_server_core::database::Error>> {
		if ids.is_empty() {
			return Ok(ControlFlow::Break(()));
		}

		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "Option<db::value::FromStr>")]
			creator: Option<tg::Principal>,

			#[tangram_database(as = "db::value::FromStr")]
			permissions: tg::authorization::permission::Set,

			#[tangram_database(as = "db::value::FromStr")]
			resource: tg::Id,

			#[tangram_database(as = "db::value::FromStr")]
			subject: tg::authorization::Subject,
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
		let rows = tangram_server_core::database::retry!(result, "failed to execute the statement");
		for row in rows {
			batch
				.items
				.push(tangram_index::batch::Item::DeletePermission(
					tangram_index::permission::delete::Arg {
						creator: Some(row.creator.unwrap_or_default()),
						permissions: row.permissions,
						resource: row.resource,
						source: tangram_index::permission::Source::Grant,
						subject: row.subject,
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
		tangram_server_core::database::retry!(result, "failed to execute the statement");

		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn list_grants(
		&self,
		session: &Session,
		arg: tg::grant::list::Arg,
	) -> tg::Result<Option<tg::grant::list::Output>> {
		let location = session
			.server
			.location(arg.location.as_ref())
			.map_err(|error| tg::error!(!error, "failed to resolve the location"))?;
		match location {
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) if Some(region.as_str()) != session.server.config.region.as_deref() => {
				self.list_grants_region(session, arg, region).await
			},
			tg::Location::Local(_) => self.list_grants_local(session, arg).await,
			tg::Location::Remote(remote) => self.list_grants_remote(session, arg, remote).await,
		}
	}

	async fn list_grants_local(
		&self,
		session: &Session,
		arg: tg::grant::list::Arg,
	) -> tg::Result<Option<tg::grant::list::Output>> {
		// Parse the pagination arguments.
		let limit = session.parse_page_limit(arg.limit)?;
		let cursor = arg
			.cursor
			.as_deref()
			.map(tangram_server_core::cursor::deserialize::<Cursor>)
			.transpose()?;
		let after = cursor.map(|cursor| match cursor {
			Cursor::V0 { after } => after,
		});

		// List the grants for the requested scope.
		let expand = arg.expand;
		match (arg.resource, arg.subject) {
			(Some(resource), None) => {
				self.list_resource_grants_local(session, resource, after.as_ref(), limit, expand)
					.await
			},
			(None, Some(subject)) => {
				self.list_subject_grants_local(session, subject, after.as_ref(), limit, expand)
					.await
			},
			_ => Err(tg::error!(
				"expected exactly one of a resource or a subject"
			)),
		}
	}

	async fn list_resource_grants_local(
		&self,
		session: &Session,
		resource: tg::Referent<tg::Selector<tg::Id>>,
		after: Option<&tg::grant::Id>,
		limit: u64,
		expand: bool,
	) -> tg::Result<Option<tg::grant::list::Output>> {
		// Listing the grants on an object, process, sandbox, or sync requires the root principal.
		if let tg::Selector::Id(id) = &resource.node
			&& (matches!(
				id.kind(),
				tg::id::Kind::Process | tg::id::Kind::Sandbox | tg::id::Kind::Sync
			) || tg::object::Id::try_from(id.clone()).is_ok())
		{
			if !matches!(session.context.principal, tg::Principal::Root) {
				return Err(tg::error!("unauthorized"));
			}
			let output = self
				.list_resource_grants(session, id, after, limit, expand)
				.await?;
			return Ok(Some(output));
		}
		// Listing the grants on a node requires admin permission, and the node is not found without read permission.
		let id = self.resolve_resource(session, &resource.node).await?;
		let read = tangram_server_grant_common::read_permission_for_resource(&id)?;
		if !session
			.authorize(resource.clone(), read)
			.await?
			.check_exhaustion()?
			.permissions
			.contains(read)
		{
			return Ok(None);
		}
		let admin = tangram_server_grant_common::admin_permission_for_resource(&id)?;
		if !session
			.authorize(resource.clone(), admin)
			.await?
			.check_exhaustion()?
			.permissions
			.contains(admin)
		{
			return Err(tg::error!("unauthorized"));
		}
		let output = self
			.list_resource_grants(session, &id, after, limit, expand)
			.await?;
		Ok(Some(output))
	}

	async fn list_resource_grants(
		&self,
		session: &Session,
		resource: &tg::Id,
		after: Option<&tg::grant::Id>,
		limit: u64,
		expand: bool,
	) -> tg::Result<tg::grant::list::Output> {
		// Read an extra row to determine whether another page exists.
		let mut data = session
			.server
			.database
			.run_with_options(db::ConnectionOptions::default(), |transaction| {
				let after = after.cloned();
				let resource = resource.clone();
				async move {
					Self::list_resource_grants_with_transaction(
						transaction,
						&resource,
						after.as_ref(),
						Some(limit + 1),
					)
					.await
				}
				.boxed()
			})
			.await?;

		// Create the continuation cursor.
		let cursor = if u64::try_from(data.len()).unwrap() > limit {
			data.pop();
			let cursor = Cursor::V0 {
				after: data.last().unwrap().id.clone(),
			};
			Some(tangram_server_core::cursor::serialize(&cursor)?)
		} else {
			None
		};
		let location = tg::Location::Local(tg::location::Local {
			region: session.server.config.region.clone(),
		});
		let data = data
			.into_iter()
			.map(|data| {
				let output = tg::grant::get::Output {
					data,
					location: Some(location.clone()),
				};
				item(output, expand)
			})
			.collect();
		let output = tg::grant::list::Output { cursor, data };

		Ok(output)
	}

	async fn list_subject_grants_local(
		&self,
		session: &Session,
		subject: tg::authorization::subject::Selector,
		after: Option<&tg::grant::Id>,
		limit: u64,
		expand: bool,
	) -> tg::Result<Option<tg::grant::list::Output>> {
		// Resolve the subject.
		let subject = self.try_resolve_subject(session, &subject).await?;
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
				let read = tangram_server_grant_common::read_permission_for_resource(&id)?;
				if !session
					.authorize(tg::Selector::Id(id.clone()), read)
					.await?
					.check_exhaustion()?
					.permissions
					.contains(read)
				{
					return Ok(None);
				}
				let admin = tangram_server_grant_common::admin_permission_for_resource(&id)?;
				if !session
					.authorize(tg::Selector::Id(id), admin)
					.await?
					.check_exhaustion()?
					.permissions
					.contains(admin)
				{
					return Err(tg::error!("unauthorized"));
				}
			},
			// Listing the grants held by any other subject requires the root principal.
			tg::authorization::Subject::Process(_)
			| tg::authorization::Subject::Public
			| tg::authorization::Subject::Root
			| tg::authorization::Subject::Runner(_)
			| tg::authorization::Subject::Sandbox(_)
			| tg::authorization::Subject::Sync(_)
			| tg::authorization::Subject::Tag(_) => {
				if !matches!(session.context.principal, tg::Principal::Root) {
					return Err(tg::error!("unauthorized"));
				}
			},
		}

		// List the grants.
		let output = self
			.list_subject_grants(session, &subject, after, limit, expand)
			.await?;

		Ok(Some(output))
	}

	async fn try_resolve_subject(
		&self,
		session: &Session,
		subject: &tg::authorization::subject::Selector,
	) -> tg::Result<Option<tg::authorization::Subject>> {
		let subject = subject.clone();
		session
			.server
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
		session: &Session,
		subject: &tg::authorization::Subject,
		after: Option<&tg::grant::Id>,
		limit: u64,
		expand: bool,
	) -> tg::Result<tg::grant::list::Output> {
		// Read an extra row to determine whether another page exists.
		let mut data = session
			.server
			.database
			.run_with_options(db::ConnectionOptions::default(), |transaction| {
				let after = after.cloned();
				let subject = subject.clone();
				async move {
					Self::list_subject_grants_with_transaction(
						transaction,
						&subject,
						after.as_ref(),
						Some(limit + 1),
					)
					.await
				}
				.boxed()
			})
			.await?;

		// Create the continuation cursor.
		let cursor = if u64::try_from(data.len()).unwrap() > limit {
			data.pop();
			let cursor = Cursor::V0 {
				after: data.last().unwrap().id.clone(),
			};
			Some(tangram_server_core::cursor::serialize(&cursor)?)
		} else {
			None
		};
		let location = tg::Location::Local(tg::location::Local {
			region: session.server.config.region.clone(),
		});
		let data = data
			.into_iter()
			.map(|data| {
				let output = tg::grant::get::Output {
					data,
					location: Some(location.clone()),
				};
				item(output, expand)
			})
			.collect();
		let output = tg::grant::list::Output { cursor, data };

		Ok(output)
	}

	async fn list_subject_grants_with_transaction(
		transaction: &tangram_server_core::database::Transaction<'_>,
		subject: &tg::authorization::Subject,
		after: Option<&tg::grant::Id>,
		limit: Option<u64>,
	) -> tg::Result<ControlFlow<Vec<tg::grant::Data>, tangram_server_core::database::Error>> {
		#[derive(db::row::Deserialize)]
		struct Row {
			created_at: i64,

			#[tangram_database(as = "Option<db::value::FromStr>")]
			creator: Option<tg::Principal>,

			#[tangram_database(as = "db::value::FromStr")]
			id: tg::grant::Id,

			#[tangram_database(as = "db::value::FromStr")]
			permissions: tg::authorization::permission::Set,

			#[tangram_database(as = "db::value::FromStr")]
			resource: tg::Id,
		}
		let p = transaction.p();

		// The empty string sorts before every grant ID for the first page.
		let after = after.map(ToString::to_string).unwrap_or_default();
		let limit = limit.map_or(i64::MAX, |limit| i64::try_from(limit).unwrap());
		let statement = formatdoc!(
			"
				select created_at, creator, id, permissions, resource
				from grants
				where subject = {p}1 and id > {p}2
				order by id
				limit {p}3;
			"
		);
		let result = transaction
			.query_all_into::<Row>(
				statement.into(),
				db::params![subject.to_string(), after, limit],
			)
			.await;
		let rows = tangram_server_core::database::retry!(result, "failed to execute the statement");
		let grants = rows
			.into_iter()
			.map(|row| tg::grant::Data {
				created_at: row.created_at,
				creator: row.creator.unwrap_or_default(),
				id: row.id,
				permissions: row.permissions,
				resource: row.resource,
				subject: subject.clone(),
			})
			.collect();

		Ok(ControlFlow::Break(grants))
	}

	async fn list_grants_region(
		&self,
		session: &Session,
		mut arg: tg::grant::list::Arg,
		region: String,
	) -> tg::Result<Option<tg::grant::list::Output>> {
		let client = session.get_region_session(&region).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to get the region client"),
		)?;
		arg.location = Some(
			tg::Location::Local(tg::location::Local {
				region: Some(region.clone()),
			})
			.into(),
		);
		client
			.list_grants(arg)
			.await
			.map_err(|error| tg::error!(!error, region = %region, "failed to list the grants"))
	}

	async fn list_grants_remote(
		&self,
		session: &Session,
		mut arg: tg::grant::list::Arg,
		remote: tg::location::Remote,
	) -> tg::Result<Option<tg::grant::list::Output>> {
		let client = session
			.server
			.components
			.remote
			.get_remote_session(session, &remote.name)
			.await
			.map_err(
				|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
			)?;
		if let Some(resource) = &mut arg.resource {
			let location = tg::Location::Remote(tg::location::Remote {
				name: remote.name.clone(),
				region: None,
			});
			resource.options.tokens = resource.options.tokens.for_location(&location);
		}
		arg.location = Some(
			tg::Location::Local(tg::location::Local {
				region: remote.region.clone(),
			})
			.into(),
		);
		let output = client.list_grants(arg).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to list the grants"),
		)?;

		// Set the location of each listed grant to the remote.
		let Some(mut output) = output else {
			return Ok(None);
		};
		let location = tg::Location::Remote(remote.clone());
		let trusted = client.trusted();
		for item in &mut output.data {
			match item {
				tg::Either::Left(output) => {
					output.location = Some(location.clone());
				},
				tg::Either::Right(referent) => {
					session
						.server
						.components
						.authorization
						.update_tokens_and_location(
							session,
							&mut referent.options.tokens,
							Some(&mut referent.options.location),
							&location,
							trusted,
						)?;
				},
			}
		}

		Ok(Some(output))
	}

	pub(crate) async fn list_grants_request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the accept header"))?;
		let (arg, _) = request
			.arg()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the arg"))?;
		let arg = arg.unwrap_or_default();
		let Some(output) = self.list_grants(session, arg).await? else {
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
				return Err(tg::error!(argument, %type_, %subtype, "invalid accept type"));
			},
		};
		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		let response = response.body(body).unwrap().boxed_body();
		Ok(response)
	}

	pub(crate) async fn grant_request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the accept header"))?;
		let arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;
		let output = self.grant(session, arg).await?;
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
				return Err(tg::error!(argument, %type_, %subtype, "invalid accept type"));
			},
		};
		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		let response = response.body(body).unwrap().boxed_body();
		Ok(response)
	}

	pub(crate) async fn revoke_request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;
		self.revoke(session, arg).await?;
		let response = http::Response::builder().empty().unwrap().boxed_body();

		Ok(response)
	}

	pub(crate) async fn try_get_grant_request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
		grant: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the accept header"))?;
		let (arg, _) = request
			.arg()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the arg"))?;
		let arg = arg.unwrap_or_default();
		let grant = grant
			.parse()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the grant ID"))?;
		let Some(output) = self.try_get_grant(session, &grant, arg).await? else {
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
				return Err(tg::error!(argument, %type_, %subtype, "invalid accept type"));
			},
		};
		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		let response = response.body(body).unwrap().boxed_body();
		Ok(response)
	}

	pub(crate) async fn try_delete_grant_request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
		grant: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let (arg, _) = request
			.arg()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the arg"))?;
		let arg = arg.unwrap_or_default();
		let grant = grant
			.parse()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the grant ID"))?;
		let Some(()) = self.try_delete_grant(session, &grant, arg).await? else {
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

	pub(crate) async fn request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let method = request.method().clone();
		let path = request.uri().path().to_owned();
		let path_components = path.split('/').skip(1).collect::<Vec<_>>();
		match (method, path_components.as_slice()) {
			(http::Method::POST, ["grant"]) => self.grant_request(session, request).await,
			(http::Method::GET, ["grants"]) => self.list_grants_request(session, request).await,
			(http::Method::DELETE, ["grants", grant]) => {
				self.try_delete_grant_request(session, request, grant).await
			},
			(http::Method::GET, ["grants", grant]) => {
				self.try_get_grant_request(session, request, grant).await
			},
			(http::Method::POST, ["revoke"]) => self.revoke_request(session, request).await,
			(_, _) => Ok(tangram_server_core::http::not_found()),
		}
	}
}

impl tangram_server_core::component::Lifecycle for Component {}

impl tangram_server_core::grant::Component for Component {
	fn grant_with_transaction<'a>(
		&'a self,
		session: &'a Session,
		transaction: &'a tangram_server_core::database::Transaction<'_>,
		arg: tg::grant::grant::Arg,
		batch: &'a mut tangram_index::batch::Arg,
	) -> BoxFuture<
		'a,
		tg::Result<ControlFlow<(tg::grant::Data, bool), tangram_server_core::database::Error>>,
	> {
		self.grant_with_transaction(session, transaction, arg, batch)
			.boxed()
	}

	fn revoke_with_transaction<'a>(
		&'a self,
		session: &'a Session,
		transaction: &'a tangram_server_core::database::Transaction<'_>,
		arg: tg::grant::revoke::Arg,
		batch: &'a mut tangram_index::batch::Arg,
	) -> BoxFuture<'a, tg::Result<ControlFlow<Option<()>, tangram_server_core::database::Error>>> {
		self.revoke_with_transaction(session, transaction, arg, batch)
			.boxed()
	}

	fn delete_node_grants_batch_with_transaction<'a>(
		&'a self,
		_session: &'a Session,
		transaction: &'a tangram_server_core::database::Transaction<'_>,
		ids: &'a [tg::Id],
		batch: &'a mut tangram_index::batch::Arg,
	) -> BoxFuture<'a, tg::Result<ControlFlow<(), tangram_server_core::database::Error>>> {
		self.delete_node_grants_batch_with_transaction(transaction, ids, batch)
			.boxed()
	}

	fn delete_node_grants_with_transaction<'a>(
		&'a self,
		_session: &'a Session,
		transaction: &'a tangram_server_core::database::Transaction<'_>,
		id: &'a tg::Id,
		batch: &'a mut tangram_index::batch::Arg,
	) -> BoxFuture<'a, tg::Result<ControlFlow<(), tangram_server_core::database::Error>>> {
		self.delete_node_grants_with_transaction(transaction, id, batch)
			.boxed()
	}

	fn grant<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::grant::grant::Arg,
	) -> BoxFuture<'a, tg::Result<tg::grant::grant::Output>> {
		self.grant(session, arg).boxed()
	}

	fn revoke<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::grant::revoke::Arg,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.revoke(session, arg).boxed()
	}

	fn list_grants<'a>(
		&'a self,
		session: &'a Session,
		arg: tg::grant::list::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::grant::list::Output>>> {
		self.list_grants(session, arg).boxed()
	}

	fn try_delete_grant<'a>(
		&'a self,
		session: &'a Session,
		grant: &'a tg::grant::Id,
		arg: tg::grant::delete::Arg,
	) -> BoxFuture<'a, tg::Result<Option<()>>> {
		self.try_delete_grant(session, grant, arg).boxed()
	}

	fn try_get_grant<'a>(
		&'a self,
		session: &'a Session,
		grant: &'a tg::grant::Id,
		arg: tg::grant::get::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::grant::get::Output>>> {
		self.try_get_grant(session, grant, arg).boxed()
	}

	fn request<'a>(
		&'a self,
		session: &'a Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>> {
		self.request(session, request).boxed()
	}

	fn list_resource_grants_with_transaction<'a>(
		&'a self,
		transaction: &'a tangram_server_core::database::Transaction<'_>,
		resource: &'a tg::Id,
		after: Option<&'a tg::grant::Id>,
		limit: Option<u64>,
	) -> BoxFuture<
		'a,
		tg::Result<ControlFlow<Vec<tg::grant::Data>, tangram_server_core::database::Error>>,
	> {
		Self::list_resource_grants_with_transaction(transaction, resource, after, limit).boxed()
	}

	fn resolve_subject_with_transaction<'a>(
		&'a self,
		transaction: &'a tangram_server_core::database::Transaction<'_>,
		subject: &'a tg::authorization::subject::Selector,
	) -> BoxFuture<
		'a,
		tg::Result<
			ControlFlow<Option<tg::authorization::Subject>, tangram_server_core::database::Error>,
		>,
	> {
		Self::resolve_subject_with_transaction(transaction, subject).boxed()
	}
}

fn item(output: tg::grant::get::Output, expand: bool) -> tg::grant::list::Item {
	if expand {
		return tg::Either::Left(output);
	}
	let options = tg::referent::Options {
		location: output.location,
		..Default::default()
	};
	let referent = tg::Referent::new(output.data.id, options);
	tg::Either::Right(referent)
}

fn creator_key(creator: &tg::Principal) -> String {
	if creator.is_root() {
		String::new()
	} else {
		creator.to_string()
	}
}
