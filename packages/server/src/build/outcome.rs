use crate::{
	util::http::{full, not_found, Incoming, Outgoing},
	Server,
};
use futures::{future, TryFutureExt as _, TryStreamExt as _};
use indoc::formatdoc;
use std::pin::pin;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};

impl Server {
	pub async fn try_get_build_outcome(
		&self,
		id: &tg::build::Id,
		arg: tg::build::outcome::Arg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> tg::Result<Option<Option<tg::build::Outcome>>> {
		if let Some(outcome) = self
			.try_get_build_outcome_local(id, arg.clone(), stop.clone())
			.await?
		{
			Ok(Some(outcome))
		} else if let Some(outcome) = self
			.try_get_build_outcome_remote(id, arg.clone(), stop.clone())
			.await?
		{
			Ok(Some(outcome))
		} else {
			Ok(None)
		}
	}

	async fn try_get_build_outcome_local(
		&self,
		id: &tg::build::Id,
		arg: tg::build::outcome::Arg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> tg::Result<Option<Option<tg::build::Outcome>>> {
		// Verify the build is local.
		if !self.get_build_exists_local(id).await? {
			return Ok(None);
		}

		// Wait for the build to finish.
		let arg = tg::build::status::Arg {
			timeout: arg.timeout,
		};
		let status = self
			.try_get_build_status_local(id, arg, stop)
			.await?
			.ok_or_else(|| tg::error!("expected the build to exist"))?;
		let finished = status.try_filter_map(|status| {
			future::ready(Ok(if status == tg::build::Status::Finished {
				Some(())
			} else {
				None
			}))
		});
		let finished = pin!(finished)
			.try_next()
			.map_ok(|option| option.is_some())
			.await?;
		if !finished {
			return Ok(Some(None));
		}

		// Get the outcome.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		let p = connection.p();
		let statement = formatdoc!(
			"
				select outcome
				from builds
				where id = {p}1;
			"
		);
		let params = db::params![id];
		let outcome = connection
			.query_optional_value_into(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		Ok(Some(outcome))
	}

	async fn try_get_build_outcome_remote(
		&self,
		id: &tg::build::Id,
		arg: tg::build::outcome::Arg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> tg::Result<Option<Option<tg::build::Outcome>>> {
		let Some(remote) = self.remotes.first() else {
			return Ok(None);
		};
		let Some(outcome) = remote.try_get_build_outcome(id, arg, stop).await? else {
			return Ok(None);
		};
		Ok(Some(outcome))
	}
}

impl Server {
	pub(crate) async fn handle_get_build_outcome_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["builds", id, "outcome"] = path_components.as_slice() else {
			let path = request.uri().path();
			return Err(tg::error!(%path, "unexpected path"));
		};
		let id = id
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the ID"))?;

		// Get the search params.
		let arg = request
			.uri()
			.query()
			.map(serde_urlencoded::from_str)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to deserialize the search params"))?
			.unwrap_or_default();

		let stop = request.extensions().get().cloned().unwrap();
		let Some(outcome) = handle.try_get_build_outcome(&id, arg, Some(stop)).await? else {
			return Ok(not_found());
		};

		// Create the body.
		let outcome = if let Some(outcome) = outcome {
			Some(outcome.data(handle).await?)
		} else {
			None
		};
		let body = serde_json::to_vec(&outcome)
			.map_err(|source| tg::error!(!source, "failed to serialize the body"))?;
		let body = full(body);

		// Create the response.
		let response = http::Response::builder()
			.status(http::StatusCode::OK)
			.body(body)
			.unwrap();

		Ok(response)
	}
}
