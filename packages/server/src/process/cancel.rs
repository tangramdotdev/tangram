use {
	crate::{Context, Server},
	futures::{StreamExt as _, stream::FuturesUnordered},
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
};

impl Server {
	pub async fn try_cancel_process_with_context(
		&self,
		_context: &Context,
		id: &tg::process::Id,
		arg: tg::process::cancel::Arg,
	) -> tg::Result<Option<()>> {
		let locations = self
			.locations(arg.location.as_ref())
			.await
			.map_err(|source| tg::error!(!source, "failed to resolve the locations"))?;

		if let Some(local) = &locations.local {
			if local.current
				&& let Some(output) = self
					.try_cancel_process_local(id, arg.clone())
					.await
					.map_err(|source| tg::error!(!source, %id, "failed to cancel the process"))?
			{
				return Ok(Some(output));
			}

			if let Some(output) = self
				.try_cancel_process_regions(id, arg.clone(), &local.regions)
				.await
				.map_err(
					|source| tg::error!(!source, %id, "failed to cancel the process in another region"),
				)? {
				return Ok(Some(output));
			}
		}

		if let Some(output) = self
			.try_cancel_process_remotes(id, arg, &locations.remotes)
			.await
			.map_err(
				|source| tg::error!(!source, %id, "failed to cancel the process in a remote"),
			)? {
			return Ok(Some(output));
		}

		Ok(None)
	}

	async fn try_cancel_process_local(
		&self,
		id: &tg::process::Id,
		arg: tg::process::cancel::Arg,
	) -> tg::Result<Option<()>> {
		// Get a process store connection.
		let mut connection = self
			.process_store
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a process store connection"))?;

		// Begin a transaction.
		let transaction = connection
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

		let status = self
			.try_lock_process_for_token_mutation_with_transaction(&transaction, id)
			.await
			.map_err(|source| tg::error!(!source, "failed to lock the process"))?;
		let Some(status) = status else {
			transaction
				.rollback()
				.await
				.map_err(|source| tg::error!(!source, "failed to roll back the transaction"))?;
			return Ok(None);
		};

		// Delete the process token.
		let p = transaction.p();
		let statement = formatdoc!(
			r"
				delete from process_tokens
				where process = {p}1 and token = {p}2;
			"
		);
		let params = db::params![id.to_string(), arg.token];
		let deleted = transaction
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// If no token was removed, then validate whether the token was stale or invalid.
		if deleted == 0 && !status.is_finished() {
			transaction
				.rollback()
				.await
				.map_err(|source| tg::error!(!source, "failed to roll back the transaction"))?;
			return Err(tg::error!("the process token was not found"));
		}

		// Update the token count.
		self.update_process_token_count_with_transaction(&transaction, id)
			.await
			.map_err(|source| tg::error!(!source, "failed to update the token count"))?;

		// Commit the transaction.
		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		drop(connection);

		// Publish the watchdog message if a token was removed.
		if deleted > 0 {
			self.spawn_publish_watchdog_message_task();
		}

		Ok(Some(()))
	}

	async fn try_cancel_process_regions(
		&self,
		id: &tg::process::Id,
		arg: tg::process::cancel::Arg,
		regions: &[String],
	) -> tg::Result<Option<()>> {
		let mut futures = regions
			.iter()
			.map(|region| self.try_cancel_process_region(id, arg.clone(), region))
			.collect::<FuturesUnordered<_>>();
		let mut result = Ok(None);
		while let Some(next) = futures.next().await {
			match next {
				Ok(Some(output)) => {
					result = Ok(Some(output));
					break;
				},
				Ok(None) => (),
				Err(source) => {
					result = Err(source);
				},
			}
		}
		let Some(output) = result? else {
			return Ok(None);
		};
		Ok(Some(output))
	}

	async fn try_cancel_process_region(
		&self,
		id: &tg::process::Id,
		arg: tg::process::cancel::Arg,
		region: &str,
	) -> tg::Result<Option<()>> {
		let client = self.get_region_client(region.to_owned()).await.map_err(
			|source| tg::error!(!source, region = %region, %id, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let arg = tg::process::cancel::Arg {
			location: Some(location.into()),
			..arg
		};
		let Some(()) = client.try_cancel_process(id, arg).await.map_err(
			|source| tg::error!(!source, region = %region, "failed to cancel the process"),
		)?
		else {
			return Ok(None);
		};
		Ok(Some(()))
	}

	async fn try_cancel_process_remotes(
		&self,
		id: &tg::process::Id,
		arg: tg::process::cancel::Arg,
		remotes: &[crate::location::Remote],
	) -> tg::Result<Option<()>> {
		let mut futures = remotes
			.iter()
			.map(|remote| self.try_cancel_process_remote(id, arg.clone(), remote))
			.collect::<FuturesUnordered<_>>();
		let mut result = Ok(None);
		while let Some(next) = futures.next().await {
			match next {
				Ok(Some(output)) => {
					result = Ok(Some(output));
					break;
				},
				Ok(None) => (),
				Err(source) => {
					result = Err(source);
				},
			}
		}
		let Some(output) = result? else {
			return Ok(None);
		};
		Ok(Some(output))
	}

	async fn try_cancel_process_remote(
		&self,
		id: &tg::process::Id,
		arg: tg::process::cancel::Arg,
		remote: &crate::location::Remote,
	) -> tg::Result<Option<()>> {
		let client = self.get_remote_client(remote.name.clone()).await.map_err(
			|source| tg::error!(!source, remote = %remote.name, %id, "failed to get the remote client"),
		)?;
		let arg = tg::process::cancel::Arg {
			location: Some(tg::location::Arg(vec![
				tg::location::arg::Component::Local(tg::location::arg::LocalComponent {
					regions: remote.regions.clone(),
				}),
			])),
			..arg
		};
		let Some(()) = client.try_cancel_process(id, arg).await.map_err(
			|source| tg::error!(!source, remote = %remote.name, "failed to cancel the process"),
		)?
		else {
			return Ok(None);
		};
		Ok(Some(()))
	}

	pub(crate) async fn handle_cancel_process_request(
		&self,
		request: http::Request<BoxBody>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;

		// Parse the ID.
		let id = id
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the process id"))?;

		// Parse the arg.
		let arg = request
			.query_params()
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the query params"))?
			.ok_or_else(|| tg::error!("query parameters required"))?;

		let Some(()) = self
			.try_cancel_process_with_context(context, &id, arg)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to cancel the process"))?
		else {
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
		};

		// Create the response.
		match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR)) => (),
			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		}

		let response = http::Response::builder().empty().unwrap().boxed_body();

		Ok(response)
	}
}
