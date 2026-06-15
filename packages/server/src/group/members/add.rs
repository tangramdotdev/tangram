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
			tg::Location::Local(_) => self.add_group_member_local(group, &arg.member).await,
			tg::Location::Remote(remote) => self.add_group_member_remote(group, arg, remote).await,
		}
	}

	async fn add_group_member_local(
		&self,
		group: &tg::group::Selector,
		member: &tg::group::Member,
	) -> tg::Result<()> {
		match self
			.authorize(group.clone().into(), tg::grant::Permission::Admin)
			.await?
		{
			None => return Err(tg::error!("failed to find the group")),
			Some(false) => return Err(tg::error!("unauthorized")),
			Some(true) => (),
		}
		let session = self.clone();
		let batch = self
			.server
			.database
			.run(|transaction| {
				let group = group.clone();
				let member = member.clone();
				let session = session.clone();
				async move {
					let mut batch = tangram_index::batch::Arg::default();
					session
						.add_group_member_with_transaction(transaction, &group, &member, &mut batch)
						.await?;
					Ok::<_, crate::database::Error>(ControlFlow::Break(batch))
				}
				.boxed()
			})
			.await?;
		if !batch.is_empty() {
			self.server
				.index
				.batch(batch)
				.await
				.map_err(|error| tg::error!(!error, "failed to index the group member"))?;
		}
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
		)
	}

	pub(crate) async fn add_group_member_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		group: &tg::group::Selector,
		member: &tg::group::Member,
		batch: &mut tangram_index::batch::Arg,
	) -> tg::Result<()> {
		let group = Self::try_get_node_by_selector_with_transaction(transaction, group)
			.await?
			.ok_or_else(|| tg::error!("failed to find the group"))?;
		if group.kind != tg::id::Kind::Group {
			return Err(tg::error!("failed to find the group"));
		}
		let member_id: tg::Id = member.clone().into();
		if Self::try_get_node_by_id_with_transaction(transaction, &member_id)
			.await?
			.is_none()
		{
			return Err(tg::error!("failed to find the member"));
		}
		if matches!(member, tg::group::Member::Group(_))
			&& Self::group_contains_group_with_transaction(transaction, &member_id, &group.id)
				.await?
		{
			return Err(tg::error!("membership cycle"));
		}
		let p = transaction.p();
		let statement = formatdoc!(
			r#"
				insert into group_members ("group", member)
				values ({p}1, {p}2)
				on conflict ("group", member) do nothing;
			"#
		);
		let inserted = transaction
			.execute(
				statement.into(),
				db::params![group.id.to_string(), member_id.to_string()],
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		if inserted == 0 {
			return Err(tg::error!("the member is already in the group"));
		}
		batch
			.put_group_members
			.push(tangram_index::group::member::put::Arg {
				group: group.id.clone().try_into()?,
				member: member.clone(),
			});
		let principal = match member {
			tg::group::Member::Group(id) => tg::grant::Principal::Group(id.clone()),
			tg::group::Member::User(id) => tg::grant::Principal::User(id.clone()),
		};
		let arg = tg::grant::create::Arg {
			principal: principal.into(),
			permission: tg::grant::Permission::Write,
			resource: tg::grant::Resource::Id(group.id.clone()),
		};
		self.create_grant_with_transaction(transaction, arg, batch)
			.await?;
		Ok(())
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
