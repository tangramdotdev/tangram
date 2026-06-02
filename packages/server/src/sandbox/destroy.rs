use {
	crate::{Session, context::Authentication, database::Transaction},
	futures::{FutureExt as _, StreamExt as _, stream::FuturesUnordered},
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody,
		request::Ext as _,
		response::{Ext as _, builder::Ext as _},
	},
};

#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;
#[cfg(feature = "turso")]
mod turso;

#[derive(Clone, Copy, Debug)]
pub(crate) enum Condition {
	HeartbeatExpired { max_heartbeat_at: i64 },
}

struct InnerArg {
	condition: Option<Condition>,
	now: i64,
}

struct InnerOutput {
	destroyed: bool,
	unfinished_processes: Vec<tg::process::Id>,
}

impl Session {
	pub(crate) async fn try_destroy_sandbox(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::destroy::Arg,
	) -> tg::Result<Option<bool>> {
		if self
			.context
			.authentication
			.as_ref()
			.is_some_and(Authentication::is_process)
		{
			return Err(tg::error!("unauthorized"));
		}

		let locations = self
			.locations(arg.location.as_ref())
			.await
			.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;

		if let Some(local) = &locations.local {
			if local.current
				&& let Some(output) = self
					.try_destroy_sandbox_local(id, arg.error.clone(), None)
					.await
					.map_err(|error| tg::error!(!error, %id, "failed to destroy the sandbox"))?
			{
				return Ok(Some(output));
			}

			if let Some(output) = self
				.try_destroy_sandbox_regions(id, &arg, &local.regions)
				.await
				.map_err(
					|error| tg::error!(!error, %id, "failed to destroy the sandbox in another region"),
				)? {
				return Ok(Some(output));
			}
		}

		if let Some(output) = self
			.try_destroy_sandbox_remotes(id, &arg, &locations.remotes)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to destroy the sandbox in a remote"))?
		{
			return Ok(Some(output));
		}

		Ok(None)
	}

	pub(crate) async fn try_destroy_sandbox_local(
		&self,
		id: &tg::sandbox::Id,
		error: Option<tg::Either<tg::error::Data, tg::error::Id>>,
		condition: Option<Condition>,
	) -> tg::Result<Option<bool>> {
		// Verify the sandbox is local.
		if !self
			.server
			.get_sandbox_exists_local(id)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the sandbox"))?
		{
			return Ok(None);
		}

		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let process_error = {
			let error = error.unwrap_or_else(|| {
				tg::Either::Left(tg::error::Data {
					code: Some(tg::error::Code::Cancellation),
					message: Some("the process was canceled".into()),
					..Default::default()
				})
			});
			let error_code = match error.as_ref() {
				tg::Either::Left(data) => data.code,
				tg::Either::Right(_) => None,
			};
			let error = self.store_process_error(error).await;
			(error, error_code)
		};
		let session = self.clone();
		let output = self
			.server
			.process_store
			.run(|transaction| {
				let id = id.clone();
				let process_error = process_error.clone();
				let session = session.clone();
				async move {
					session
						.try_destroy_sandbox_with_transaction(
							transaction,
							&id,
							condition,
							now,
							process_error,
						)
						.await
				}
				.boxed()
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to destroy the sandbox"))?;

		let (output, finished_processes) = output;
		let Some(output) = output else {
			return Ok(None);
		};
		if !output {
			return Ok(Some(false));
		}

		// Spawn a task to publish the status message.
		self.server.spawn_publish_sandbox_status_task(id);

		// Spawn a task to publish the finalize message.
		self.server.spawn_publish_sandbox_finalize_message_task();

		for process in &finished_processes {
			self.spawn_process_finish_tasks(process);
		}

		Ok(Some(true))
	}

	async fn try_destroy_sandbox_with_transaction(
		&self,
		transaction: &Transaction<'_>,
		id: &tg::sandbox::Id,
		condition: Option<Condition>,
		now: i64,
		process_error: (
			tg::Either<tg::error::Data, tg::error::Id>,
			Option<tg::error::Code>,
		),
	) -> tg::Result<ControlFlow<(Option<bool>, Vec<tg::process::Id>), crate::database::Error>> {
		let arg = InnerArg { condition, now };

		let InnerOutput {
			destroyed,
			unfinished_processes,
		} = match self.try_destroy_sandbox_inner(transaction, id, arg).await? {
			ControlFlow::Break(output) => output,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		if !destroyed {
			return Ok(ControlFlow::Break((Some(false), Vec::new())));
		}

		let mut finished_processes = Vec::new();
		if !unfinished_processes.is_empty() {
			for process in unfinished_processes {
				let arg = crate::process::finish::InnerArg {
					checksum: None,
					condition: None,
					error: Some(process_error.0.clone()),
					error_code: process_error.1,
					exit: 1,
					now,
					output: None,
				};
				let finished = self
					.try_finish_process_inner(transaction, &process, arg)
					.await
					.map_err(|error| tg::error!(!error, "failed to finish the process"))?;
				let finished = match finished {
					ControlFlow::Break(finished) => finished,
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				};
				if finished {
					finished_processes.push(process);
				}
			}
		}

		match self
			.server
			.delete_sandbox_tokens_with_transaction(transaction, id)
			.await
			.map_err(|error| tg::error!(!error, "failed to delete the sandbox tokens"))?
		{
			ControlFlow::Break(()) => {},
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}
		match self
			.server
			.delete_sandbox_process_tokens_with_transaction(transaction, id)
			.await
			.map_err(|error| tg::error!(!error, "failed to delete the process tokens"))?
		{
			ControlFlow::Break(()) => {},
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}

		Ok(ControlFlow::Break((Some(true), finished_processes)))
	}

	async fn try_destroy_sandbox_inner(
		&self,
		transaction: &Transaction<'_>,
		id: &tg::sandbox::Id,
		arg: InnerArg,
	) -> tg::Result<ControlFlow<InnerOutput, crate::database::Error>> {
		match transaction {
			#[cfg(feature = "postgres")]
			Transaction::Postgres(transaction) => {
				match self
					.try_destroy_sandbox_inner_postgres(transaction, id, arg)
					.await?
				{
					ControlFlow::Break(output) => Ok(ControlFlow::Break(output)),
					ControlFlow::Continue(error) => Ok(ControlFlow::Continue(
						crate::database::Error::Postgres(error),
					)),
				}
			},
			#[cfg(feature = "sqlite")]
			Transaction::Sqlite(transaction) => {
				match self
					.try_destroy_sandbox_inner_sqlite(transaction, id, arg)
					.await?
				{
					ControlFlow::Break(output) => Ok(ControlFlow::Break(output)),
					ControlFlow::Continue(error) => {
						Ok(ControlFlow::Continue(crate::database::Error::Sqlite(error)))
					},
				}
			},
			#[cfg(feature = "turso")]
			Transaction::Turso(transaction) => {
				match self
					.try_destroy_sandbox_inner_turso(transaction, id, arg)
					.await?
				{
					ControlFlow::Break(output) => Ok(ControlFlow::Break(output)),
					ControlFlow::Continue(error) => {
						Ok(ControlFlow::Continue(crate::database::Error::Turso(error)))
					},
				}
			},
		}
	}

	async fn try_destroy_sandbox_regions(
		&self,
		id: &tg::sandbox::Id,
		arg: &tg::sandbox::destroy::Arg,
		regions: &[String],
	) -> tg::Result<Option<bool>> {
		let mut futures = regions
			.iter()
			.map(|region| self.try_destroy_sandbox_region(id, arg, region))
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

	async fn try_destroy_sandbox_region(
		&self,
		id: &tg::sandbox::Id,
		arg: &tg::sandbox::destroy::Arg,
		region: &str,
	) -> tg::Result<Option<bool>> {
		let client = self.get_region_session(region).await.map_err(
			|error| tg::error!(!error, region = %region, %id, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let arg = tg::sandbox::destroy::Arg {
			location: Some(location.into()),
			..arg.clone()
		};
		let Some(destroyed) = client.try_destroy_sandbox(id, arg).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to destroy the sandbox"),
		)?
		else {
			return Ok(None);
		};
		Ok(Some(destroyed))
	}

	async fn try_destroy_sandbox_remotes(
		&self,
		id: &tg::sandbox::Id,
		arg: &tg::sandbox::destroy::Arg,
		remotes: &[crate::location::Remote],
	) -> tg::Result<Option<bool>> {
		let mut futures = remotes
			.iter()
			.map(|remote| self.try_destroy_sandbox_remote(id, arg, remote))
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

	async fn try_destroy_sandbox_remote(
		&self,
		id: &tg::sandbox::Id,
		arg: &tg::sandbox::destroy::Arg,
		remote: &crate::location::Remote,
	) -> tg::Result<Option<bool>> {
		let client = self.get_remote_session(&remote.name).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, %id, "failed to get the remote client"),
		)?;
		let arg = tg::sandbox::destroy::Arg {
			location: Some(tg::location::Arg(vec![
				tg::location::arg::Component::Local(tg::location::arg::LocalComponent {
					regions: remote.regions.clone(),
				}),
			])),
			..arg.clone()
		};
		let Some(destroyed) = client.try_destroy_sandbox(id, arg).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to destroy the sandbox"),
		)?
		else {
			return Ok(None);
		};
		Ok(Some(destroyed))
	}

	pub(crate) async fn try_destroy_sandbox_request(
		&self,
		request: http::Request<BoxBody>,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;
		let id = id
			.parse::<tg::sandbox::Id>()
			.map_err(|error| tg::error!(!error, "failed to parse the sandbox id"))?;
		let arg = request
			.json_or_default()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;

		let Some(destroyed) = self
			.try_destroy_sandbox(&id, arg)
			.await
			.map_err(|error| tg::error!(!error, %id, "failed to destroy the sandbox"))?
		else {
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
		};
		if !destroyed {
			return Ok(http::Response::builder()
				.status(http::StatusCode::CONFLICT)
				.empty()
				.unwrap()
				.boxed_body());
		}

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
