use crate::Server;
use futures::{stream, StreamExt as _, TryStreamExt as _};
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_http::{outgoing::response::Ext as _, Incoming, Outgoing};

impl Server {
	pub async fn try_get_build(
		&self,
		id: &tg::build::Id,
	) -> tg::Result<Option<tg::build::get::Output>> {
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
	) -> tg::Result<Option<tg::build::get::Output>> {
		// Get a database connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Get the build.
		let p = connection.p();
		let statement = formatdoc!(
			"
				select
					id,
					complete,
					count,
					host,
					log,
					outcome,
					retry,
					status,
					target,
					weight,
					created_at,
					dequeued_at,
					started_at,
					finished_at,
					heartbeat_at,
					touched_at
				from builds
				where id = {p}1;
			"
		);
		let params = db::params![id];
		let output = connection
			.query_optional_into(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

		Ok(output)
	}

	async fn try_get_build_remote(
		&self,
		id: &tg::build::Id,
	) -> tg::Result<Option<tg::build::get::Output>> {
		// Get the remote.
		let Some(remote) = self.remotes.first() else {
			return Ok(None);
		};

		// Get the build from the remote server.
		let Some(output) = remote.try_get_build(id).await? else {
			return Ok(None);
		};

		// Put the build if it is finished.
		if output.status == tg::build::Status::Finished {
			let arg = tg::build::children::Arg {
				timeout: Some(std::time::Duration::ZERO),
				..Default::default()
			};
			let children = self
				.try_get_build_children_stream(id, arg)
				.await?
				.ok_or_else(|| tg::error!("expected the build to exist"))?
				.map_ok(|chunk| stream::iter(chunk.items).map(Ok::<_, tg::Error>))
				.try_flatten()
				.try_collect()
				.await?;
			let arg = tg::build::put::Arg {
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
				dequeued_at: output.dequeued_at,
				started_at: output.started_at,
				finished_at: output.finished_at,
			};
			self.put_build(id, arg).await?;
		}

		Ok(Some(output))
	}
}

impl Server {
	pub(crate) async fn handle_get_build_request<H>(
		handle: &H,
		_request: http::Request<Incoming>,
		id: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		let Some(output) = handle.try_get_build(&id).await? else {
			return Ok(http::Response::builder().not_found().empty().unwrap());
		};
		let response = http::Response::builder().json(output).unwrap();
		Ok(response)
	}
}
