use {
	crate::{Session, context::Authentication},
	futures::FutureExt as _,
	indoc::formatdoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{
		body::Boxed as BoxBody,
		request::Ext as _,
		response::{Ext as _, builder::Ext as _},
	},
};

impl Session {
	pub(crate) async fn delete_tag_grant(
		&self,
		arg: tg::tag::grants::delete::Arg,
	) -> tg::Result<Option<()>> {
		if self
			.context
			.authentication
			.as_ref()
			.is_some_and(Authentication::is_process)
		{
			return Err(tg::error!("unauthorized"));
		}
		let location = self
			.server
			.location(arg.location.as_ref())
			.map_err(|error| tg::error!(!error, "failed to resolve the location"))?;
		match location {
			tg::Location::Local(_) => self.delete_tag_grant_local(arg).await,
			tg::Location::Remote(remote) => self.delete_tag_grant_remote(arg, remote).await,
		}
	}

	async fn delete_tag_grant_local(
		&self,
		arg: tg::tag::grants::delete::Arg,
	) -> tg::Result<Option<()>> {
		let session = self.clone();
		self.server
			.database
			.run(|transaction| {
				let arg = arg.clone();
				let session = session.clone();
				async move {
					let output = session
						.delete_tag_grant_with_transaction(transaction, &arg)
						.await?;
					Ok::<_, crate::database::Error>(ControlFlow::Break(output))
				}
				.boxed()
			})
			.await
	}

	async fn delete_tag_grant_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		arg: &tg::tag::grants::delete::Arg,
	) -> tg::Result<Option<()>> {
		let Some(node) =
			Self::try_get_node_by_selector_with_transaction(transaction, &arg.tag).await?
		else {
			return Ok(None);
		};
		let p = transaction.p();
		let statement = formatdoc!(
			"
				delete from tag_grants
				where tag = {p}1 and principal = {p}2 and permission = {p}3;
			"
		);
		let n = transaction
			.execute(
				statement.into(),
				db::params![
					node.id.to_string(),
					arg.principal.to_string(),
					arg.permission.to_string()
				],
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		if n == 0 {
			return Ok(None);
		}
		Self::decrement_visibility_with_transaction(
			transaction,
			&node.id,
			&arg.principal.to_string(),
		)
		.await?;
		Ok(Some(()))
	}

	async fn delete_tag_grant_remote(
		&self,
		mut arg: tg::tag::grants::delete::Arg,
		remote: tg::location::Remote,
	) -> tg::Result<Option<()>> {
		let client = self.get_remote_session(&remote.name).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
		)?;
		arg.location = Some(tg::Location::Local(tg::location::Local::default()).into());
		client.delete_tag_grant(arg).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to delete the tag grant"),
		)
	}

	pub(crate) async fn delete_tag_grant_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let arg = request
			.query_params()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.ok_or_else(|| tg::error!("expected query params"))?;
		let Some(()) = self.delete_tag_grant(arg).await? else {
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
		};
		Ok(http::Response::builder().empty().unwrap().boxed_body())
	}
}
