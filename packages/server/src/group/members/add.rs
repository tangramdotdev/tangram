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
	pub(crate) async fn add_group_member(
		&self,
		group: &tg::group::Selector,
		arg: tg::group::members::add::Arg,
	) -> tg::Result<()> {
		let location = self
			.server
			.location(arg.location.as_ref())
			.map_err(|error| tg::error!(!error, "failed to resolve the location"))?;
		match location {
			tg::Location::Local(_) if !self.server.is_primary_region() => {
				self.add_group_member_primary_region(group, arg).await
			},
			tg::Location::Local(_) => self.add_group_member_local(group, &arg.member).await,
			tg::Location::Remote(remote) => self.add_group_member_remote(group, arg, remote).await,
		}
	}

	async fn add_group_member_local(
		&self,
		group: &tg::group::Selector,
		member: &tg::group::Member,
	) -> tg::Result<()> {
		let permission = tg::authorization::Permission::Group(
			tg::authorization::permission::group::Permission::Admin,
		);
		match self.authorize(group.clone(), permission).await? {
			None => return Err(tg::error!("failed to find the group")),
			Some(permissions) if permissions.contains(permission) => (),
			Some(_) => return Err(tg::error!("unauthorized")),
		}
		let session = self.clone();
		self.server
			.database
			.run(|transaction| {
				let group = group.clone();
				let member = member.clone();
				let session = session.clone();
				async move {
					session
						.add_group_member_local_with_transaction(transaction, &group, &member)
						.await
				}
				.boxed()
			})
			.await?;
		self.server
			.spawn_publish_database_outbox_notification_task();
		Ok(())
	}

	async fn add_group_member_local_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		group: &tg::group::Selector,
		member: &tg::group::Member,
	) -> tg::Result<ControlFlow<(), crate::database::Error>> {
		let mut batch = tangram_index::batch::Arg::default();
		match self
			.add_group_member_with_transaction(transaction, group, member, &mut batch)
			.await?
		{
			ControlFlow::Break(()) => (),
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}
		match self
			.server
			.enqueue_database_outbox_with_transaction(transaction, &batch)
			.await?
		{
			ControlFlow::Break(()) => (),
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}

		Ok(ControlFlow::Break(()))
	}

	async fn add_group_member_primary_region(
		&self,
		group: &tg::group::Selector,
		mut arg: tg::group::members::add::Arg,
	) -> tg::Result<()> {
		let client = self
			.get_primary_region_session()
			.await
			.map_err(|error| tg::error!(!error, "failed to get the primary region session"))?;
		arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
		client.add_group_member(group, arg).await.map_err(|error| {
			tg::error!(
				!error,
				"failed to add the group member in the primary region"
			)
		})?;

		Ok(())
	}

	async fn add_group_member_remote(
		&self,
		group: &tg::group::Selector,
		mut arg: tg::group::members::add::Arg,
		remote: tg::location::Remote,
	) -> tg::Result<()> {
		let client = self.get_remote_session(&remote.name).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
		)?;
		arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
		client.add_group_member(group, arg).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to add the group member"),
		)?;
		self.invalidate_remote_cache(&remote.name).await;

		Ok(())
	}

	pub(crate) async fn add_group_member_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		group: &tg::group::Selector,
		member: &tg::group::Member,
		batch: &mut tangram_index::batch::Arg,
	) -> tg::Result<ControlFlow<(), crate::database::Error>> {
		let id = match group {
			tg::Selector::Id(id) => Some(id.clone()),
			tg::Selector::Specifier(specifier) => {
				let id =
					match Self::try_get_id_for_specifier_with_transaction(transaction, specifier)
						.await?
					{
						ControlFlow::Break(id) => id,
						ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
					};
				id.and_then(|id| id.try_into().ok())
			},
		}
		.ok_or_else(|| tg::error!("failed to find the group"))?;
		let group = match Self::try_get_group_with_transaction(transaction, &id).await? {
			ControlFlow::Break(group) => group,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		if group.is_none() {
			return Err(tg::error!("failed to find the group"));
		}
		let group_id: tg::Id = id.into();
		let member_id: tg::Id = member.clone().into();
		let specifier =
			match Self::try_get_specifier_for_id_with_transaction(transaction, &member_id).await? {
				ControlFlow::Break(specifier) => specifier,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			};
		if specifier.is_none() {
			return Err(tg::error!("failed to find the member"));
		}
		if matches!(member, tg::group::Member::Group(_)) {
			let contains = match Self::group_contains_group_with_transaction(
				transaction,
				&member_id,
				&group_id,
			)
			.await?
			{
				ControlFlow::Break(contains) => contains,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			};
			if contains {
				return Err(tg::error!("membership cycle"));
			}
		}
		let p = transaction.p();
		let statement = formatdoc!(
			r#"
				insert into group_members ("group", member)
				values ({p}1, {p}2)
				on conflict ("group", member) do nothing;
			"#
		);
		let result = transaction
			.execute(
				statement.into(),
				db::params![group_id.to_string(), member_id.to_string()],
			)
			.await;
		let inserted = crate::database::retry!(result, "failed to execute the statement");
		if inserted == 0 {
			return Err(tg::error!("the member is already in the group"));
		}
		batch.items.push(tangram_index::batch::Item::PutGroupMember(
			tangram_index::group::member::put::Arg {
				group: group_id.clone().try_into()?,
				member: member.clone(),
			},
		));
		let subject = match member {
			tg::group::Member::Group(id) => tg::authorization::Subject::Group(id.clone()),
			tg::group::Member::User(id) => tg::authorization::Subject::User(id.clone()),
		};
		let arg = tg::grant::create::Arg {
			permissions: tg::Either::Left(
				tg::authorization::Permission::Group(
					tg::authorization::permission::group::Permission::Write,
				)
				.into(),
			),
			resource: tg::Referent::with_node(tg::Selector::Id(group_id)),
			subject: subject.into(),
		};
		match self
			.create_grant_with_transaction(transaction, arg, batch)
			.await?
		{
			ControlFlow::Break(_) => (),
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}

		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn add_group_member_request(
		&self,
		request: http::Request<BoxBody>,
		group: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let arg: tg::group::members::add::Arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;
		let group = group.replace(':', "/").parse()?;
		self.add_group_member(&group, arg).await?;
		let response = http::Response::builder().empty().unwrap().boxed_body();
		Ok(response)
	}
}
