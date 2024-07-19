use crate::Server;
use futures::{future, Future, FutureExt as _, TryFutureExt as _, TryStreamExt as _};
use indoc::formatdoc;
use itertools::Itertools as _;
use std::pin::pin;
use tangram_client::{self as tg, handle::Ext as _};
use tangram_database::{self as db, prelude::*};
use tangram_futures::task::Stop;
use tangram_http::{outgoing::response::Ext as _, Incoming, Outgoing};

impl Server {
	pub async fn try_get_build_outcome_future(
		&self,
		id: &tg::build::Id,
	) -> tg::Result<
		Option<impl Future<Output = tg::Result<Option<tg::build::Outcome>>> + Send + 'static>,
	> {
		if let Some(outcome) = self.try_get_build_outcome_local(id).await? {
			Ok(Some(outcome.left_future()))
		} else if let Some(outcome) = self.try_get_build_outcome_remote(id).await? {
			Ok(Some(outcome.right_future()))
		} else {
			Ok(None)
		}
	}

	async fn try_get_build_outcome_local(
		&self,
		id: &tg::build::Id,
	) -> tg::Result<
		Option<impl Future<Output = tg::Result<Option<tg::build::Outcome>>> + Send + 'static>,
	> {
		// Verify the build is local.
		if !self.get_build_exists_local(id).await? {
			return Ok(None);
		}
		let server = self.clone();
		let id = id.clone();
		let future = async move { server.try_get_build_outcome_local_inner(&id).await };
		Ok(Some(future))
	}

	async fn try_get_build_outcome_local_inner(
		&self,
		id: &tg::build::Id,
	) -> tg::Result<Option<tg::build::Outcome>> {
		// Wait for the build to finish.
		let status = self
			.try_get_build_status_local_stream(id)
			.await?
			.ok_or_else(|| tg::error!("expected the build to exist"))?;
		let finished = status.try_filter_map(|status| {
			future::ready(Ok(
				if matches!(
					status,
					tg::build::status::Event::Status(tg::build::Status::Finished)
				) {
					Some(())
				} else {
					None
				},
			))
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
			.connection(db::Priority::Low)
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
	) -> tg::Result<
		Option<impl Future<Output = tg::Result<Option<tg::build::Outcome>>> + Send + 'static>,
	> {
		let futures = self
			.remotes
			.iter()
			.map(|remote| {
				{
					let remote = remote.clone();
					let id = id.clone();
					async move {
						remote
							.get_build_outcome(&id)
							.await
							.map(futures::FutureExt::boxed)
					}
				}
				.boxed()
			})
			.collect_vec();
		if futures.is_empty() {
			return Ok(None);
		}
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
		// Parse the ID.
		let id = id.parse()?;

		// Get the future.
		let Some(future) = handle.try_get_build_outcome(&id).await? else {
			return Ok(http::Response::builder().not_found().empty().unwrap());
		};

		// Stop the future when the server stops.
		let stop = request.extensions().get::<Stop>().cloned().unwrap();
		let future = future.boxed();
		let stop = async move { stop.stopped().await }.boxed();
		let future = future::select(future, stop).map(|result| match result {
			future::Either::Left((result, _)) => result,
			future::Either::Right(((), _)) => Ok(None),
		});

		// Create the body.
		let future = future.and_then({
			let handle = handle.clone();
			move |option| async move {
				if let Some(outcome) = option {
					outcome.data(&handle).await.map(Some)
				} else {
					Ok(None)
				}
			}
		});

		// Create the response.
		let response = http::Response::builder()
			.future_optional_json(future)
			.unwrap();

		Ok(response)
	}
}
