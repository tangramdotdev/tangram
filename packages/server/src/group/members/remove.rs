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
		match self
			.authorize(group.clone().into(), tg::grant::Permission::Admin)
			.await?
		{
			None => return Ok(None),
			Some(false) => return Err(tg::error!("unauthorized")),
			Some(true) => (),
		}
		let session = self.clone();
		let (output, batch) = self
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
				.map_err(|error| tg::error!(!error, "failed to index the group member"))?;
		}
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
		client
			.remove_group_member(group, member, arg)
			.await
			.map_err(
				|error| tg::error!(!error, remote = %remote.name, "failed to remove the group member"),
			)
	}

	async fn remove_group_member_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		group: &tg::group::Selector,
		member: &tg::group::Member,
		batch: &mut tangram_index::batch::Arg,
	) -> tg::Result<Option<()>> {
		let Some(group) =
			Self::try_get_node_by_selector_with_transaction(transaction, group).await?
		else {
			return Ok(None);
		};
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
				db::params![group.id.to_string(), member_id.to_string()],
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		if deleted == 0 {
			return Ok(None);
		}
		batch
			.delete_group_members
			.push(tangram_index::group::member::delete::Arg {
				group: group.id.clone().try_into()?,
				member: member.clone(),
			});
		let principal = match member {
			tg::group::Member::Group(id) => tg::grant::Principal::Group(id.clone()),
			tg::group::Member::User(id) => tg::grant::Principal::User(id.clone()),
		};
		let arg = tg::grant::delete::Arg {
			principal: principal.into(),
			permission: tg::grant::Permission::Read,
			resource: tg::grant::Resource::Id(group.id.clone()),
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
