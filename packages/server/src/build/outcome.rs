use crate::Server;
use futures::{
	future, stream, Future, FutureExt as _, StreamExt as _, TryFutureExt as _, TryStreamExt as _,
};
use indoc::formatdoc;
use itertools::Itertools as _;
use std::pin::pin;
use tangram_client::{self as tg, handle::Ext as _};
use tangram_database::{self as db, prelude::*};
use tangram_futures::task::Stop;
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};

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
			.query_optional_value_into::<db::value::Json<_>>(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			.ok_or_else(|| tg::error!("expected the outcome to be set"))?
			.0;

		Ok(Some(outcome))
	}

	async fn try_get_build_outcome_remote(
		&self,
		id: &tg::build::Id,
	) -> tg::Result<
		Option<impl Future<Output = tg::Result<Option<tg::build::Outcome>>> + Send + 'static>,
	> {
		let futures = self
			.get_remote_clients()
			.await?
			.into_values()
			.map(|client| {
				{
					let client = client.clone();
					let id = id.clone();
					async move {
						client
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

		// Get the accept header.
		let accept: Option<mime::Mime> = request.parse_header(http::header::ACCEPT).transpose()?;

		// Get the stream.
		let Some(future) = handle.try_get_build_outcome(&id).await? else {
			return Ok(http::Response::builder().not_found().empty().unwrap());
		};
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
		let stream = stream::once(future).filter_map(|option| future::ready(option.transpose()));

		// Stop the stream when the server stops.
		let stop = request.extensions().get::<Stop>().cloned().unwrap();
		let stop = async move { stop.wait().await };
		let stream = stream.take_until(stop);

		// Create the body.
		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			Some((mime::TEXT, mime::EVENT_STREAM)) => {
				let content_type = mime::TEXT_EVENT_STREAM;
				let stream = stream.map(|result| match result {
					Ok(event) => event.try_into(),
					Err(error) => error.try_into(),
				});
				(Some(content_type), Outgoing::sse(stream))
			},

			_ => {
				return Err(tg::error!(?accept, "invalid accept header"));
			},
		};

		// Create the response.
		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		let response = response.body(body).unwrap();

		Ok(response)
	}
}
