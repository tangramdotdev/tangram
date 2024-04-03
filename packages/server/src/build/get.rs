use crate::{
	util::http::{full, not_found, Incoming, Outgoing},
	Http, Server,
};
use futures::{stream, StreamExt, TryStreamExt};
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_error::{error, Error, Result};
use tg::Handle;

impl Server {
	pub async fn try_get_build(
		&self,
		id: &tg::build::Id,
		_arg: tg::build::GetArg,
	) -> Result<Option<tg::build::GetOutput>> {
		if let Some(output) = self.try_get_build_local(id).await? {
			Ok(Some(output))
		} else if let Some(output) = self.try_get_build_remote(id).await? {
			Ok(Some(output))
		} else {
			Ok(None)
		}
	}

	pub(crate) async fn try_get_build_local(
		&self,
		id: &tg::build::Id,
	) -> Result<Option<tg::build::GetOutput>> {
		// Get a database connection.
		let connection = self
			.inner
			.database
			.connection()
			.await
			.map_err(|source| error!(!source, "failed to get a database connection"))?;

		// Get the build.
		let p = connection.p();
		let statement = formatdoc!(
			"
				select
					id,
					count,
					host,
					log,
					outcome,
					retry,
					status,
					target,
					weight,
					created_at,
					queued_at,
					started_at,
					finished_at
				from builds
				where id = {p}1;
			"
		);
		let params = db::params![id];
		let output = connection
			.query_optional_into(statement, params)
			.await
			.map_err(|source| error!(!source, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

		Ok(output)
	}

	async fn try_get_build_remote(
		&self,
		id: &tg::build::Id,
	) -> Result<Option<tg::build::GetOutput>> {
		// Get the remote.
		let Some(remote) = self.inner.remotes.first() else {
			return Ok(None);
		};

		// Get the build from the remote server.
		let arg = tg::build::GetArg::default();
		let Some(output) = remote.try_get_build(id, arg).await? else {
			return Ok(None);
		};

		// Insert the build if it is finished.
		if output.status == tg::build::Status::Finished {
			let arg = tg::build::children::GetArg {
				timeout: Some(std::time::Duration::ZERO),
				..Default::default()
			};
			let children = self
				.get_build_children(id, arg, None)
				.await?
				.map_ok(|chunk| stream::iter(chunk.items).map(Ok::<_, Error>))
				.try_flatten()
				.try_collect()
				.await?;
			let arg = tg::build::PutArg {
				id: output.id.clone(),
				children,
				count: output.count,
				host: output.host.clone(),
				log: output.log.clone(),
				outcome: output.outcome.clone(),
				retry: output.retry,
				status: output.status,
				target: output.target.clone(),
				weight: output.weight,
				created_at: output.created_at,
				queued_at: output.queued_at,
				started_at: output.started_at,
				finished_at: output.finished_at,
			};
			self.insert_build(id, &arg).await?;
		}

		Ok(Some(output))
	}
}

impl Http {
	pub async fn handle_get_build_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<hyper::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["builds", id] = path_components.as_slice() else {
			let path = request.uri().path();
			return Err(error!(%path, "unexpected path"));
		};
		let id = id
			.parse()
			.map_err(|source| error!(!source, "failed to parse the ID"))?;

		// Get the search params.
		let arg = request
			.uri()
			.query()
			.map(serde_urlencoded::from_str)
			.transpose()
			.map_err(|source| error!(!source, "failed to deserialize the search params"))?
			.unwrap_or_default();

		// Get the build.
		let Some(output) = self.inner.tg.try_get_build(&id, arg).await? else {
			return Ok(not_found());
		};

		// Create the body.
		let body = serde_json::to_vec(&output)
			.map_err(|source| error!(!source, "failed to serialize the body"))?;
		let body = full(body);

		// Create the response.
		let response = http::Response::builder()
			.status(http::StatusCode::OK)
			.body(body)
			.unwrap();

		Ok(response)
	}
}
