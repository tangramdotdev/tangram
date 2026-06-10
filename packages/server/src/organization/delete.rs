use {
	crate::Session,
	futures::FutureExt as _,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
	tangram_index::prelude::*,
};

impl Session {
	pub(crate) async fn try_delete_organization(
		&self,
		organization: &tg::organization::Selector,
		arg: tg::organization::delete::Arg,
	) -> tg::Result<Option<()>> {
		let location = self
			.server
			.location(arg.location.as_ref())
			.map_err(|error| tg::error!(!error, "failed to resolve the location"))?;
		match location {
			tg::Location::Local(_) => self.try_delete_organization_local(organization).await,
			tg::Location::Remote(remote) => {
				self.try_delete_organization_remote(organization, arg, remote)
					.await
			},
		}
	}

	async fn try_delete_organization_local(
		&self,
		organization: &tg::organization::Selector,
	) -> tg::Result<Option<()>> {
		let session = self.clone();
		let (output, batch) = self
			.server
			.database
			.run(|transaction| {
				let organization = organization.clone();
				let session = session.clone();
				async move {
					let mut batch = tangram_index::batch::Arg::default();
					let output = session
						.delete_organization_with_transaction(
							transaction,
							&organization,
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
				.map_err(|error| tg::error!(!error, "failed to index the organization"))?;
		}
		Ok(output)
	}

	async fn try_delete_organization_remote(
		&self,
		organization: &tg::organization::Selector,
		mut arg: tg::organization::delete::Arg,
		remote: tg::location::Remote,
	) -> tg::Result<Option<()>> {
		let client = self.get_remote_session(&remote.name).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
		)?;
		arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
		client
			.try_delete_organization(organization, arg)
			.await
			.map_err(
				|error| tg::error!(!error, remote = %remote.name, "failed to delete the organization"),
			)
	}

	async fn delete_organization_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		organization: &tg::organization::Selector,
		batch: &mut tangram_index::batch::Arg,
	) -> tg::Result<Option<()>> {
		let Some(node) =
			Self::try_get_node_by_selector_with_transaction(transaction, organization).await?
		else {
			return Ok(None);
		};
		if node.kind != tg::id::Kind::Organization {
			return Ok(None);
		}
		if Self::node_has_children_with_transaction(transaction, &node.id).await? {
			return Err(tg::error!("cannot delete an organization with children"));
		}
		let p = transaction.p();
		#[derive(db::row::Deserialize)]
		struct OrganizationMemberRow {
			#[tangram_database(as = "db::value::FromStr")]
			member: tg::organization::Member,
			#[tangram_database(as = "db::value::FromStr")]
			organization: tg::organization::Id,
		}
		let statement = format!(
			"
				select organization, member
				from organization_members
				where organization = {p}1;
			"
		);
		let organization_members = transaction
			.query_all_into::<OrganizationMemberRow>(
				statement.into(),
				db::params![node.id.to_string()],
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		for row in organization_members {
			batch.delete_organization_members.push(
				tangram_index::organization::member::delete::Arg {
					member: row.member,
					organization: row.organization,
				},
			);
		}
		self.delete_node_grants_with_transaction(transaction, &node.id, batch)
			.await?;
		batch.delete_organizations.push(node.id.clone().try_into()?);
		for statement in [
			format!("delete from organization_members where organization = {p}1;"),
			format!("delete from organizations where id = {p}1;"),
			format!("delete from nodes where id = {p}1;"),
		] {
			transaction
				.execute(statement.into(), db::params![node.id.to_string()])
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		}
		Ok(Some(()))
	}

	pub(crate) async fn try_delete_organization_request(
		&self,
		request: http::Request<BoxBody>,
		organization: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let arg = request
			.query_params()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.unwrap_or_default();
		let organization = organization.replace(':', "/").parse()?;
		let Some(()) = self.try_delete_organization(&organization, arg).await? else {
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
