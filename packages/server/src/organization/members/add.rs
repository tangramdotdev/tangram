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
	pub(crate) async fn add_organization_member(
		&self,
		organization: &tg::organization::Selector,
		arg: tg::organization::members::add::Arg,
	) -> tg::Result<()> {
		let location = self
			.server
			.location(arg.location.as_ref())
			.map_err(|error| tg::error!(!error, "failed to resolve the location"))?;
		match location {
			tg::Location::Local(_) => {
				self.add_organization_member_local(organization, &arg.member)
					.await
			},
			tg::Location::Remote(remote) => {
				self.add_organization_member_remote(organization, arg, remote)
					.await
			},
		}
	}

	async fn add_organization_member_local(
		&self,
		organization: &tg::organization::Selector,
		member: &tg::organization::Member,
	) -> tg::Result<()> {
		let session = self.clone();
		self.server
			.database
			.run(|transaction| {
				let organization = organization.clone();
				let member = member.clone();
				let session = session.clone();
				async move {
					session
						.add_organization_member_with_transaction(
							transaction,
							&organization,
							&member,
						)
						.await?;
					Ok::<_, crate::database::Error>(ControlFlow::Break(()))
				}
				.boxed()
			})
			.await
	}

	async fn add_organization_member_remote(
		&self,
		organization: &tg::organization::Selector,
		mut arg: tg::organization::members::add::Arg,
		remote: tg::location::Remote,
	) -> tg::Result<()> {
		let client = self.get_remote_session(&remote.name).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
		)?;
		arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
		client
			.add_organization_member(organization, arg)
			.await
			.map_err(
				|error| tg::error!(!error, remote = %remote.name, "failed to add the organization member"),
			)
	}

	async fn add_organization_member_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		organization: &tg::organization::Selector,
		member: &tg::organization::Member,
	) -> tg::Result<()> {
		let organization =
			Self::try_get_node_by_selector_with_transaction(transaction, organization)
				.await?
				.ok_or_else(|| tg::error!("failed to find the organization"))?;
		if organization.kind != tg::id::Kind::Organization {
			return Err(tg::error!("failed to find the organization"));
		}
		let member_id: tg::Id = member.clone().into();
		if Self::try_get_node_by_id_with_transaction(transaction, &member_id)
			.await?
			.is_none()
		{
			return Err(tg::error!("failed to find the member"));
		}
		let p = transaction.p();
		let statement = formatdoc!(
			"
				insert into organization_members (organization, member)
				values ({p}1, {p}2)
				on conflict (organization, member) do nothing;
			"
		);
		transaction
			.execute(
				statement.into(),
				db::params![organization.id.to_string(), member_id.to_string()],
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let principal = match member {
			tg::organization::Member::Group(id) => tg::grant::Principal::Group(id.clone()),
			tg::organization::Member::User(id) => tg::grant::Principal::User(id.clone()),
		};
		let arg = tg::grant::create::Arg {
			principal: principal.into(),
			permission: tg::grant::Permission::Read,
			resource: tg::grant::Resource::Id(organization.id),
		};
		self.create_grant_with_transaction(transaction, arg).await?;
		Ok(())
	}

	pub(crate) async fn add_organization_member_request(
		&self,
		request: http::Request<BoxBody>,
		organization: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let arg: tg::organization::members::add::Arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;
		let organization = organization.replace(':', "/").parse()?;
		self.add_organization_member(&organization, arg).await?;
		let response = http::Response::builder().empty().unwrap().boxed_body();
		Ok(response)
	}
}
