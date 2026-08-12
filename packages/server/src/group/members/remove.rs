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
	pub(crate) async fn remove_group_member(
		&self,
		group: &tg::group::Selector,
		member: &tg::group::Member,
		arg: tg::group::members::remove::Arg,
	) -> tg::Result<Option<()>> {
		let location = self
			.server
			.location(arg.location.as_ref())
			.map_err(|error| tg::error!(!error, "failed to resolve the location"))?;
		match location {
			tg::Location::Local(_) => self.remove_group_member_local(group, member).await,
			tg::Location::Remote(remote) => {
				self.remove_group_member_remote(group, member, arg, remote)
					.await
			},
		}
	}

	async fn remove_group_member_local(
		&self,
		group: &tg::group::Selector,
		member: &tg::group::Member,
	) -> tg::Result<Option<()>> {
		let permission = tg::authorization::Permission::Group(
			tg::authorization::permission::group::Permission::Admin,
		);
		match self.authorize(group.clone(), permission).await? {
			None => return Ok(None),
			Some(permissions) if permissions.contains(permission) => (),
			Some(_) => return Err(tg::error!("unauthorized")),
		}
		let session = self.clone();
		let output = self
			.server
			.database
			.run(|transaction| {
				let group = group.clone();
				let member = member.clone();
				let session = session.clone();
				async move {
					let mut batch = tangram_index::batch::Arg::default();
					let output = session
						.remove_group_member_with_transaction(
							transaction,
							&group,
							&member,
							&mut batch,
						)
						.await?;
					session
						.server
						.enqueue_database_outbox_with_transaction(transaction, &batch)
						.await?;
					Ok::<_, crate::database::Error>(ControlFlow::Break(output))
				}
				.boxed()
			})
			.await?;
		Ok(output)
	}

	async fn remove_group_member_remote(
		&self,
		group: &tg::group::Selector,
		member: &tg::group::Member,
		mut arg: tg::group::members::remove::Arg,
		remote: tg::location::Remote,
	) -> tg::Result<Option<()>> {
		let client = self.get_remote_session(&remote.name).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
		)?;
		arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
		let output = client
			.remove_group_member(group, member, arg)
			.await
			.map_err(
				|error| tg::error!(!error, remote = %remote.name, "failed to remove the group member"),
			)?;
		self.invalidate_remote_cache(&remote.name).await;

		Ok(output)
	}

	async fn remove_group_member_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		group: &tg::group::Selector,
		member: &tg::group::Member,
		batch: &mut tangram_index::batch::Arg,
	) -> tg::Result<Option<()>> {
		let id = match group {
			tg::Selector::Id(id) => Some(id.clone()),
			tg::Selector::Specifier(specifier) => {
				Self::try_get_id_for_specifier_with_transaction(transaction, specifier)
					.await?
					.and_then(|id| id.try_into().ok())
			},
		};
		let Some(id) = id else {
			return Ok(None);
		};
		if Self::try_get_group_with_transaction(transaction, &id)
			.await?
			.is_none()
		{
			return Ok(None);
		}
		let group_id: tg::Id = id.into();
		let member_id: tg::Id = member.clone().into();
		let p = transaction.p();
		let statement = formatdoc!(
			r#"
				delete from group_members
				where "group" = {p}1 and member = {p}2;
			"#
		);
		let deleted = transaction
			.execute(
				statement.into(),
				db::params![group_id.to_string(), member_id.to_string()],
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		if deleted == 0 {
			return Ok(None);
		}
		batch
			.items
			.push(tangram_index::batch::Item::DeleteGroupMember(
				tangram_index::group::member::delete::Arg {
					group: group_id.clone().try_into()?,
					member: member.clone(),
				},
			));
		let subject = match member {
			tg::group::Member::Group(id) => tg::authorization::Subject::Group(id.clone()),
			tg::group::Member::User(id) => tg::authorization::Subject::User(id.clone()),
		};
		let arg = tg::grant::delete::Arg {
			permissions: tg::Either::Left(
				tg::authorization::Permission::Group(
					tg::authorization::permission::group::Permission::Write,
				)
				.into(),
			),
			resource: tg::Referent::with_node(tg::Selector::Id(group_id)),
			subject: subject.into(),
		};
		self.delete_grant_with_transaction(transaction, arg, batch)
			.await?;
		Ok(Some(()))
	}

	pub(crate) async fn remove_group_member_request(
		&self,
		request: http::Request<BoxBody>,
		group: &str,
		member: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let arg = request
			.query_params()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.unwrap_or_default();
		let group = group.replace(':', "/").parse()?;
		let member = member.replace(':', "/").parse()?;
		let Some(()) = self.remove_group_member(&group, &member, arg).await? else {
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
