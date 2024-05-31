use crate::{BuildPermit, Server};
use bytes::Bytes;
use either::Either;
use futures::{future, FutureExt as _};
use itertools::Itertools as _;
use tangram_client as tg;
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};
use tangram_messenger::Messenger as _;

impl Server {
	pub async fn build_target(
		&self,
		id: &tg::target::Id,
		arg: tg::target::build::Arg,
	) -> tg::Result<tg::target::build::Output> {
		// Get a local build if one exists that satisfies the retry constraint.
		let build = 'a: {
			// Find a build.
			let list_arg = tg::build::list::Arg {
				limit: Some(1),
				order: Some(tg::build::list::Order::CreatedAtDesc),
				status: None,
				target: Some(id.clone()),
			};
			let Some(output) = self.list_builds(list_arg).await?.items.first().cloned() else {
				break 'a None;
			};
			let build = tg::Build::with_id(output.id);

			// Verify that the build's outcome satisfies the retry constraint.
			let outcome_arg = tg::build::outcome::Arg {
				timeout: Some(std::time::Duration::ZERO),
			};
			let outcome = build.get_outcome(self, outcome_arg).await?;
			if let Some(outcome) = outcome {
				if outcome.retry() <= arg.retry {
					break 'a None;
				}
			}

			// Touch the build.
			tokio::spawn({
				let server = self.clone();
				let build = build.clone();
				async move {
					let arg = tg::build::touch::Arg { remote: None };
					server.touch_build(build.id(), arg).await.ok();
				}
			});

			Some(build)
		};

		// Get a remote build if one exists that satisfies the retry constraint.
		let build = 'a: {
			if let Some(build) = build {
				break 'a Some(build);
			}

			// Find a build.
			let futures = self
				.remotes
				.iter()
				.map(|remote| {
					let remote = remote.clone();
					async move {
						// Find a build.
						let list_arg = tg::build::list::Arg {
							limit: Some(1),
							order: Some(tg::build::list::Order::CreatedAtDesc),
							status: None,
							target: Some(id.clone()),
						};
						let Some(output) =
							remote.list_builds(list_arg).await?.items.first().cloned()
						else {
							return Ok::<_, tg::Error>(None);
						};
						let build = tg::Build::with_id(output.id);

						// Verify that the build's outcome satisfies the retry constraint.
						let outcome_arg = tg::build::outcome::Arg {
							timeout: Some(std::time::Duration::ZERO),
						};
						let outcome = build.get_outcome(&remote, outcome_arg).await?;
						if let Some(outcome) = outcome {
							if outcome.retry() <= arg.retry {
								return Ok(None);
							}
						}

						Ok(Some((build, remote.clone())))
					}
					.boxed()
				})
				.collect_vec();

			// Wait for the first build.
			if futures.is_empty() {
				break 'a None;
			}
			let Ok((Some((build, remote)), _)) = future::select_ok(futures).await else {
				break 'a None;
			};

			// Touch the build.
			tokio::spawn({
				let remote = remote.clone();
				let build = build.clone();
				async move {
					let arg = tg::build::touch::Arg { remote: None };
					remote.touch_build(build.id(), arg).await.ok();
				}
			});

			Some(build)
		};

		// If a local or remote build was found that satisfies the retry constraint, then add it as a child of the parent and return it.
		if let Some(build) = build {
			// Add the build as a child of the parent.
			if let Some(parent) = arg.parent.as_ref() {
				self.add_build_child(parent, build.id()).await?;
			}

			let output = tg::target::build::Output {
				build: build.id().clone(),
			};

			return Ok(output);
		}

		// Otherwise, if the remote arg was set, then build the target remotely.
		if let Some(remote) = arg.remote.as_ref() {
			let remote = self
				.remotes
				.get(remote)
				.ok_or_else(|| tg::error!("the remote does not exist"))?
				.clone();
			let output = remote.build_target(id, arg).await?;
			return Ok(output);
		}

		// Finally, create a new build.
		let build_id = tg::build::Id::new();

		// Get the host.
		let target = tg::Target::with_id(id.clone());
		let host = target.host(self).await?;

		// Put the build.
		let put_arg = tg::build::put::Arg {
			id: build_id.clone(),
			children: Vec::new(),
			host: host.clone(),
			log: None,
			outcome: None,
			retry: arg.retry,
			status: tg::build::Status::Created,
			target: id.clone(),
			created_at: time::OffsetDateTime::now_utc(),
			dequeued_at: None,
			started_at: None,
			finished_at: None,
		};
		self.put_build(&build_id, put_arg).await?;

		// Create the build.
		let build = tg::Build::with_id(build_id.clone());

		// Create the build's log if necessary.
		if self.options.advanced.write_build_logs_to_database {
			let path = self.logs_path().join(id.to_string());
			tokio::fs::File::create(&path).await.map_err(
				|source| tg::error!(!source, %path = path.display(), "failed to create the log file"),
			)?;
		}

		// Add the build to the parent.
		if let Some(parent) = arg.parent.as_ref() {
			self.add_build_child(parent, build.id()).await?;
		}

		// Publish the message.
		tokio::spawn({
			let server = self.clone();
			async move {
				server
					.messenger
					.publish("builds.created".to_owned(), Bytes::new())
					.await
					.inspect_err(|error| tracing::error!(%error, "failed to publish"))
					.ok();
			}
		});

		// Spawn a task to spawn the build when the parent's permit is available.
		let server = self.clone();
		let parent = arg.parent.clone();
		let build = build.clone();
		tokio::spawn(async move {
			// Acquire the parent's permit.
			let Some(permit) = parent.as_ref().and_then(|parent| {
				server
					.build_permits
					.get(parent)
					.map(|permit| permit.clone())
			}) else {
				return;
			};
			let permit = permit
				.lock_owned()
				.map(|guard| BuildPermit(Either::Right(guard)))
				.await;

			// Start the build.
			server
				.spawn_build(build, permit, None)
				.await
				.inspect_err(|error| tracing::error!(?error, "failed to spawn the build"))
				.ok();
		});

		let output = tg::target::build::Output { build: build_id };

		Ok(output)
	}
}

impl Server {
	pub(crate) async fn handle_build_target_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
		id: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		let arg = request.json().await?;
		let output = handle.build_target(&id, arg).await?;
		let response = http::Response::builder().json(output).unwrap();
		Ok(response)
	}
}
