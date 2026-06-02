use {
	crate::Session,
	futures::{FutureExt as _, StreamExt as _, stream::FuturesUnordered},
	indoc::formatdoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
};

impl Session {
	pub async fn try_cancel_process(
		&self,
		id: &tg::process::Id,
		arg: tg::process::cancel::Arg,
	) -> tg::Result<Option<()>> {
		let locations = self
			.locations(arg.location.as_ref())
			.await
			.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;

		if let Some(local) = &locations.local {
			if local.current
				&& let Some(output) = self
					.try_cancel_process_local(id, arg.clone())
					.await
					.map_err(|error| tg::error!(!error, %id, "failed to cancel the process"))?
			{
				return Ok(Some(output));
			}

			if let Some(output) = self
				.try_cancel_process_regions(id, arg.clone(), &local.regions)
				.await
				.map_err(
					|error| tg::error!(!error, %id, "failed to cancel the process in another region"),
				)? {
				return Ok(Some(output));
			}
		}

		if let Some(output) = self
			.try_cancel_process_remotes(id, arg, &locations.remotes)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to cancel the process in a remote"))?
		{
			return Ok(Some(output));
		}

		Ok(None)
	}

	async fn try_cancel_process_local(
		&self,
		id: &tg::process::Id,
		arg: tg::process::cancel::Arg,
	) -> tg::Result<Option<()>> {
		let id = id.clone();
		let lease = arg.lease;
		let session = self.clone();
		let deleted = self
			.server
			.process_store
			.run(|transaction| {
				let id = id.clone();
				let lease = lease.clone();
				let session = session.clone();
				async move {
					session
						.try_cancel_process_with_transaction(transaction, &id, &lease)
						.await
				}
				.boxed()
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to cancel the process"))?;

		if deleted.is_some_and(|deleted| deleted > 0) {
			self.server.spawn_publish_watchdog_message_task();
		}

		Ok(deleted.map(drop))
	}

	async fn try_cancel_process_with_transaction(
		&self,
		transaction: &crate::database::Transaction<'_>,
		id: &tg::process::Id,
		lease: &str,
	) -> tg::Result<ControlFlow<Option<u64>, crate::database::Error>> {
		let status = self
			.server
			.try_lock_process_with_transaction(transaction, id)
			.await?;
		let status = match status {
			ControlFlow::Break(status) => status,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		let Some(status) = status else {
			return Ok(ControlFlow::Break(None));
		};

		let p = transaction.p();
		let statement = formatdoc!(
			r"
				delete from process_leases
				where process = {p}1 and lease = {p}2;
			"
		);
		let params = db::params![id.to_string(), lease.to_owned()];
		let result = transaction.execute(statement.into(), params).await;
		let deleted = crate::database::retry!(result, "failed to execute the statement");

		if deleted == 0 && !status.is_finished() {
			return Err(tg::error!("the process lease was not found"));
		}

		let result = self
			.server
			.update_process_lease_count_with_transaction(transaction, id)
			.await?;
		match result {
			ControlFlow::Break(()) => (),
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}

		Ok(ControlFlow::Break(Some(deleted)))
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
		let client = self.get_region_session(region).await.map_err(
			|error| tg::error!(!error, region = %region, %id, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let arg = tg::process::cancel::Arg {
			location: Some(location.into()),
			..arg
		};
		let Some(()) = client.try_cancel_process(id, arg).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to cancel the process"),
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
		let client = self.get_remote_session(&remote.name).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, %id, "failed to get the remote client"),
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
			|error| tg::error!(!error, remote = %remote.name, "failed to cancel the process"),
		)?
		else {
			return Ok(None);
		};
		Ok(Some(()))
	}

	pub(crate) async fn try_cancel_process_request(
		&self,
		request: http::Request<BoxBody>,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;

		// Parse the ID.
		let id = id
			.parse()
			.map_err(|error| tg::error!(!error, "failed to parse the process id"))?;

		// Parse the arg.
		let arg = request
			.query_params()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.ok_or_else(|| tg::error!("query parameters required"))?;

		let Some(()) = self
			.try_cancel_process(&id, arg)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to cancel the process"))?
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
