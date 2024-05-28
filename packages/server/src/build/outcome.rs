use crate::Server;
use futures::{future, Future, FutureExt as _, TryFutureExt as _, TryStreamExt as _};
use indoc::formatdoc;
use std::pin::pin;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};
use tg::Handle as _;

impl Server {
	pub async fn try_get_build_outcome_future(
		&self,
		id: &tg::build::Id,
		arg: tg::build::outcome::Arg,
	) -> tg::Result<
		Option<impl Future<Output = tg::Result<Option<tg::build::Outcome>>> + Send + 'static>,
	> {
		if let Some(outcome) = self.try_get_build_outcome_local(id, arg.clone()).await? {
			Ok(Some(outcome.left_future()))
		} else if let Some(outcome) = self.try_get_build_outcome_remote(id, arg.clone()).await? {
			Ok(Some(outcome.right_future()))
		} else {
			Ok(None)
		}
	}

	async fn try_get_build_outcome_local(
		&self,
		id: &tg::build::Id,
		arg: tg::build::outcome::Arg,
	) -> tg::Result<
		Option<impl Future<Output = tg::Result<Option<tg::build::Outcome>>> + Send + 'static>,
	> {
		// Verify the build is local.
		if !self.get_build_exists_local(id).await? {
			return Ok(None);
		}
		let server = self.clone();
		let id = id.clone();
		let future = async move { server.try_get_build_outcome_local_inner(&id, arg).await };
		Ok(Some(future))
	}

	async fn try_get_build_outcome_local_inner(
		&self,
		id: &tg::build::Id,
		arg: tg::build::outcome::Arg,
	) -> tg::Result<Option<tg::build::Outcome>> {
		// Wait for the build to finish.
		let arg = tg::build::status::Arg {
			timeout: arg.timeout,
		};
		let status = self
			.try_get_build_status_local(id, arg)
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
			return Ok(None);
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
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			.ok_or_else(|| tg::error!("expected the outcome to be set"))?;

		Ok(Some(outcome))
	}

	async fn try_get_build_outcome_remote(
		&self,
		id: &tg::build::Id,
		arg: tg::build::outcome::Arg,
	) -> tg::Result<
		Option<impl Future<Output = tg::Result<Option<tg::build::Outcome>>> + Send + 'static>,
	> {
		let futures = self.remotes.iter().map(|remote| {
			{
				let remote = remote.clone();
				let id = id.clone();
				let arg = arg.clone();
				async move {
					remote
						.get_build_outcome(&id, arg)
						.await
						.map(futures::FutureExt::boxed)
				}
			}
			.boxed()
		});
		let Ok((future, _)) = future::select_ok(futures).await else {
			return Ok(None);
		};
		Ok(Some(future))
	}
}

impl Server {
	pub(crate) async fn handle_get_build_outcome_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
		id: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;

		// Get the query.
		let arg = request.query_params().transpose()?.unwrap_or_default();

		let Some(future) = handle.try_get_build_outcome(&id, arg).await? else {
			return Ok(http::Response::builder().not_found().empty().unwrap());
		};

		// Create the body.
		let future = future.and_then({
			let handle = handle.clone();
			move |option| async move {
				if let Some(outcome) = option {
					outcome.data(&handle, None).await.map(Some)
				} else {
					Ok(None)
				}
			}
		});

		// Create the response.
		let response = http::Response::builder().future_json(future).unwrap();

		Ok(response)
	}
}
